use anyhow::{anyhow, Context, Result};
use data_encoding::{BASE64URL_NOPAD, HEXLOWER};
use ed25519_dalek::{Signer, SigningKey};
use iroh::endpoint::{RecvStream, SendStream};
use iroh::{Endpoint, NodeAddr, SecretKey};
use rand::rngs::OsRng;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, oneshot, Mutex, RwLock};
use uuid::Uuid;

pub const CHAT_ALPN: &[u8] = b"simple-chat/p2p/1";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PersistedConfig {
    pub secret_key_hex: String,
    pub nickname: String,
    pub username: Option<String>,
    pub server_url: String,
    pub registered_on_server: bool,
    #[serde(default)]
    pub messages: Vec<ChatMessagePayload>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PeerInfo {
    pub node_id: String,
    pub nickname: String,
    pub username: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ChatMessagePayload {
    pub id: String,
    pub sender_id: String,
    pub sender_name: String,
    pub content: String,
    pub timestamp: u64,
    pub is_self: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DirectoryUser {
    pub username: String,
    pub display_name: String,
    pub node_id: String,
    pub online: bool,
    pub last_seen: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StateSnapshot {
    pub node_id: String,
    pub ticket: String,
    pub nickname: String,
    pub username: Option<String>,
    pub server_url: String,
    pub registered_on_server: bool,
    pub connected_peer: Option<PeerInfo>,
    pub pending_request: Option<PeerInfo>,
    pub messages: Vec<ChatMessagePayload>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum P2PEvent {
    ConnectionRequested { peer: PeerInfo },
    PeerConnected { peer: PeerInfo },
    PeerDisconnected { reason: Option<String> },
    ChatMessage { message: ChatMessagePayload },
    StateUpdated { snapshot: StateSnapshot },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WireMessage {
    Handshake {
        node_id: String,
        nickname: String,
        username: Option<String>,
    },
    HandshakeReject {
        reason: String,
    },
    Chat {
        id: String,
        sender_id: String,
        sender_name: String,
        content: String,
        timestamp: u64,
    },
    Disconnect,
}

pub fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Encode `NodeAddr` into a compact URL-safe Base64 ticket string.
pub fn encode_ticket(addr: &NodeAddr) -> Result<String> {
    let json = serde_json::to_vec(addr).context("Failed to serialize NodeAddr")?;
    Ok(BASE64URL_NOPAD.encode(&json))
}

/// Decode a compact URL-safe Base64 ticket string back into `NodeAddr`.
pub fn decode_ticket(ticket: &str) -> Result<NodeAddr> {
    let raw = BASE64URL_NOPAD
        .decode(ticket.trim().as_bytes())
        .context("Invalid ticket encoding (expected Base64URL)")?;
    let addr: NodeAddr = serde_json::from_slice(&raw).context("Invalid NodeAddr JSON in ticket")?;
    Ok(addr)
}

/// Sign registration payload `${username}:${node_id}:${timestamp}` with Ed25519 signing key.
pub fn sign_registration_payload(
    signing_key: &SigningKey,
    username: &str,
    node_id_hex: &str,
    timestamp: u64,
) -> String {
    let payload = format!(
        "{}:{}:{}",
        username.trim().to_lowercase(),
        node_id_hex,
        timestamp
    );
    let sig = signing_key.sign(payload.as_bytes());
    HEXLOWER.encode(&sig.to_bytes())
}

/// Write a length-prefixed JSON `WireMessage` to an Iroh QUIC `SendStream`.
pub async fn write_wire_message(send: &mut SendStream, msg: &WireMessage) -> Result<()> {
    let bytes = serde_json::to_vec(msg)?;
    let len = (bytes.len() as u32).to_be_bytes();
    send.write_all(&len).await?;
    send.write_all(&bytes).await?;
    Ok(())
}

/// Read a length-prefixed JSON `WireMessage` from an Iroh QUIC `RecvStream`.
pub async fn read_wire_message(recv: &mut RecvStream) -> Result<Option<WireMessage>> {
    let mut len_buf = [0u8; 4];
    match recv.read_exact(&mut len_buf).await {
        Ok(()) => {}
        Err(iroh::endpoint::ReadExactError::FinishedEarly(_)) => return Ok(None),
        Err(e) => return Err(anyhow!("Stream read error: {e}")),
    }
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > 1024 * 1024 {
        return Err(anyhow!("Frame exceeds 1MB limit"));
    }
    let mut data = vec![0u8; len];
    recv.read_exact(&mut data).await?;
    let msg: WireMessage = serde_json::from_slice(&data)?;
    Ok(Some(msg))
}

pub type EventCallback = Arc<dyn Fn(P2PEvent) + Send + Sync + 'static>;

struct PendingApproval {
    peer: PeerInfo,
    responder: oneshot::Sender<bool>,
}

pub struct ChatNode {
    pub endpoint: Endpoint,
    pub signing_key: SigningKey,
    pub secret_key_hex: String,
    pub node_id_hex: String,
    pub config_path: Option<PathBuf>,
    pub nickname: RwLock<String>,
    pub username: RwLock<Option<String>>,
    pub server_url: RwLock<String>,
    pub registered_on_server: RwLock<bool>,
    pub connected_peer: RwLock<Option<PeerInfo>>,
    pub messages: RwLock<Vec<ChatMessagePayload>>,
    pending_approval: Mutex<Option<PendingApproval>>,
    active_tx: Mutex<Option<mpsc::Sender<WireMessage>>>,
    event_cb: EventCallback,
    http_client: reqwest::Client,
}

impl ChatNode {
    pub async fn new(event_cb: EventCallback) -> Result<Arc<Self>> {
        Self::new_with_config(event_cb, None).await
    }

    pub async fn new_with_config(
        event_cb: EventCallback,
        config_path: Option<PathBuf>,
    ) -> Result<Arc<Self>> {
        let default_server =
            std::env::var("DIRECTORY_SERVER_URL").unwrap_or_else(|_| "http://127.0.0.1:3100".into());

        // Load existing config if present
        let loaded_cfg: Option<PersistedConfig> = config_path
            .as_ref()
            .and_then(|p| std::fs::read_to_string(p).ok())
            .and_then(|raw| serde_json::from_str(&raw).ok());

        let mut secret_bytes = [0u8; 32];
        let mut initial_nickname = "User".to_string();
        let mut initial_username = None;
        let mut initial_server_url = default_server;
        let mut initial_registered = false;
        let mut initial_messages = Vec::new();

        if let Some(cfg) = loaded_cfg {
            if let Ok(decoded) = HEXLOWER.decode(cfg.secret_key_hex.trim().as_bytes()) {
                if decoded.len() == 32 {
                    secret_bytes.copy_from_slice(&decoded);
                } else {
                    OsRng.fill_bytes(&mut secret_bytes);
                }
            } else {
                OsRng.fill_bytes(&mut secret_bytes);
            }
            if !cfg.nickname.trim().is_empty() {
                initial_nickname = cfg.nickname;
            }
            initial_username = cfg.username;
            if !cfg.server_url.trim().is_empty() {
                initial_server_url = cfg.server_url;
            }
            initial_registered = cfg.registered_on_server && initial_username.is_some();
            initial_messages = cfg.messages;
        } else {
            OsRng.fill_bytes(&mut secret_bytes);
        }

        let secret_key_hex = HEXLOWER.encode(&secret_bytes);
        let signing_key = SigningKey::from_bytes(&secret_bytes);
        let iroh_secret = SecretKey::from_bytes(&secret_bytes);
        let node_id_hex = HEXLOWER.encode(iroh_secret.public().as_bytes());

        let endpoint = Endpoint::builder()
            .secret_key(iroh_secret)
            .alpns(vec![CHAT_ALPN.to_vec()])
            .bind()
            .await
            .context("Failed to bind Iroh QUIC endpoint")?;

        let node = Arc::new(Self {
            endpoint,
            signing_key,
            secret_key_hex,
            node_id_hex,
            config_path,
            nickname: RwLock::new(initial_nickname),
            username: RwLock::new(initial_username),
            server_url: RwLock::new(initial_server_url),
            registered_on_server: RwLock::new(initial_registered),
            connected_peer: RwLock::new(None),
            messages: RwLock::new(initial_messages),
            pending_approval: Mutex::new(None),
            active_tx: Mutex::new(None),
            event_cb,
            http_client: reqwest::Client::new(),
        });

        // Persist initial config (so newly generated Ed25519 keypair is saved immediately)
        node.save_config().await;

        // If already registered previously, automatically re-register on startup
        if initial_registered {
            let startup_node = Arc::clone(&node);
            tokio::spawn(async move {
                // Wait briefly for Iroh endpoint relay/local addresses to settle
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                if startup_node.push_registration_to_server().await.is_ok() {
                    if let Ok(snap) = startup_node.snapshot().await {
                        (startup_node.event_cb)(P2PEvent::StateUpdated { snapshot: snap });
                    }
                }
            });
        }

        // Spawn accept loop for incoming P2P connections
        let accept_node = Arc::clone(&node);
        tokio::spawn(async move {
            accept_node.accept_loop().await;
        });

        // Spawn periodic directory server heartbeat loop
        let hb_node = Arc::clone(&node);
        tokio::spawn(async move {
            hb_node.heartbeat_loop().await;
        });

        Ok(node)
    }

    pub async fn save_config(&self) {
        let Some(ref path) = self.config_path else {
            return;
        };
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let all_msgs = self.messages.read().await;
        let saved_msgs = if all_msgs.len() > 1000 {
            all_msgs[all_msgs.len() - 1000..].to_vec()
        } else {
            all_msgs.clone()
        };
        let cfg = PersistedConfig {
            secret_key_hex: self.secret_key_hex.clone(),
            nickname: self.nickname.read().await.clone(),
            username: self.username.read().await.clone(),
            server_url: self.server_url.read().await.clone(),
            registered_on_server: *self.registered_on_server.read().await,
            messages: saved_msgs,
        };
        if let Ok(json) = serde_json::to_string_pretty(&cfg) {
            let _ = std::fs::write(path, json);
        }
    }

    pub async fn current_ticket(&self) -> Result<String> {
        let addr = self.endpoint.node_addr().await?;
        encode_ticket(&addr)
    }

    pub async fn snapshot(&self) -> Result<StateSnapshot> {
        let ticket = self.current_ticket().await.unwrap_or_default();
        let pending_request = self
            .pending_approval
            .lock()
            .await
            .as_ref()
            .map(|p| p.peer.clone());
        Ok(StateSnapshot {
            node_id: self.node_id_hex.clone(),
            ticket,
            nickname: self.nickname.read().await.clone(),
            username: self.username.read().await.clone(),
            server_url: self.server_url.read().await.clone(),
            registered_on_server: *self.registered_on_server.read().await,
            connected_peer: self.connected_peer.read().await.clone(),
            pending_request,
            messages: self.messages.read().await.clone(),
        })
    }

    async fn accept_loop(self: Arc<Self>) {
        while let Some(incoming) = self.endpoint.accept().await {
            let node = Arc::clone(&self);
            tokio::spawn(async move {
                if let Ok(conn) = incoming.await {
                    if let Ok((send, recv)) = conn.accept_bi().await {
                        let remote_id_hex =
                            HEXLOWER.encode(conn.remote_node_id().unwrap().as_bytes());
                        let _ = node.handle_bi_stream(send, recv, remote_id_hex, false).await;
                    }
                }
            });
        }
    }

    async fn heartbeat_loop(self: Arc<Self>) {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(15));
        loop {
            interval.tick().await;
            let is_reg = *self.registered_on_server.read().await;
            if is_reg {
                let _ = self.push_registration_to_server().await;
            }
        }
    }

    pub async fn register_on_server(
        self: &Arc<Self>,
        server_url: String,
        username: String,
        nickname: String,
    ) -> Result<StateSnapshot> {
        let clean_user = username.trim().to_lowercase();
        let clean_nick = if nickname.trim().is_empty() {
            clean_user.clone()
        } else {
            nickname.trim().to_string()
        };
        let clean_url = server_url.trim().trim_end_matches('/').to_string();

        *self.server_url.write().await = clean_url;
        *self.username.write().await = Some(clean_user);
        *self.nickname.write().await = clean_nick;

        self.push_registration_to_server().await?;
        *self.registered_on_server.write().await = true;
        self.save_config().await;

        let snap = self.snapshot().await?;
        (self.event_cb)(P2PEvent::StateUpdated {
            snapshot: snap.clone(),
        });
        Ok(snap)
    }

    async fn push_registration_to_server(&self) -> Result<()> {
        let username = self
            .username
            .read()
            .await
            .clone()
            .ok_or_else(|| anyhow!("Username not set"))?;
        let display_name = self.nickname.read().await.clone();
        let server_url = self.server_url.read().await.clone();
        let ticket = self.current_ticket().await?;
        let timestamp = now_millis();
        let signature_hex =
            sign_registration_payload(&self.signing_key, &username, &self.node_id_hex, timestamp);

        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct RegReq {
            username: String,
            display_name: String,
            node_id: String,
            public_key_hex: String,
            ticket: String,
            timestamp: u64,
            signature_hex: String,
        }

        let res = self
            .http_client
            .post(format!("{}/api/register", server_url))
            .json(&RegReq {
                username,
                display_name,
                node_id: self.node_id_hex.clone(),
                public_key_hex: self.node_id_hex.clone(),
                ticket,
                timestamp,
                signature_hex,
            })
            .send()
            .await
            .context("管理サーバへの接続に失敗しました")?;

        if !res.status().is_success() {
            let err_text = res.text().await.unwrap_or_default();
            return Err(anyhow!("管理サーバ登録エラー: {}", err_text));
        }
        Ok(())
    }

    pub async fn fetch_online_users(&self) -> Result<Vec<DirectoryUser>> {
        let server_url = self.server_url.read().await.clone();
        #[derive(Deserialize)]
        struct UsersResp {
            users: Vec<DirectoryUser>,
        }
        let resp: UsersResp = self
            .http_client
            .get(format!("{}/api/users", server_url))
            .send()
            .await
            .context("管理サーバへの接続に失敗しました")?
            .json()
            .await
            .context("ユーザ一覧の解析に失敗しました")?;
        Ok(resp.users)
    }

    pub async fn connect_by_username(self: &Arc<Self>, target_username: &str) -> Result<PeerInfo> {
        let server_url = self.server_url.read().await.clone();
        let clean_target = target_username.trim().trim_start_matches('@').to_lowercase();

        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct LookupResp {
            node_id: String,
            ticket: String,
        }

        let res = self
            .http_client
            .get(format!("{}/api/users/{}", server_url, clean_target))
            .send()
            .await
            .context("管理サーバへの問い合わせに失敗しました")?;

        if !res.status().is_success() {
            return Err(anyhow!("ユーザ @{} が見つかりません", clean_target));
        }

        let lookup: LookupResp = res.json().await?;
        let peer = self
            .connect_by_ticket_verified(&lookup.ticket, Some(&lookup.node_id))
            .await?;
        Ok(peer)
    }

    pub async fn connect_by_ticket_verified(
        self: &Arc<Self>,
        ticket_str: &str,
        expected_node_id_hex: Option<&str>,
    ) -> Result<PeerInfo> {
        let node_addr = decode_ticket(ticket_str)?;
        let ticket_node_hex = HEXLOWER.encode(node_addr.node_id.as_bytes());

        if let Some(expected) = expected_node_id_hex {
            if ticket_node_hex != expected.to_lowercase() {
                return Err(anyhow!(
                    "公開鍵不一致エラー: チケットのNodeIDがサーバ登録の公開鍵と一致しません"
                ));
            }
        }

        let conn = self
            .endpoint
            .connect(node_addr, CHAT_ALPN)
            .await
            .context("相手ノードへのQUIC P2P接続に失敗しました")?;

        let remote_id_hex = HEXLOWER.encode(conn.remote_node_id()?.as_bytes());
        if let Some(expected) = expected_node_id_hex {
            if remote_id_hex != expected.to_lowercase() {
                return Err(anyhow!(
                    "TLS公開鍵検証エラー: 接続先の公開鍵が期待値と一致しません"
                ));
            }
        }

        let (send, recv) = conn
            .open_bi()
            .await
            .context("双方向QUICストリームのオープンに失敗しました")?;

        let peer = self
            .handle_bi_stream(send, recv, remote_id_hex, true)
            .await?;
        Ok(peer)
    }

    async fn handle_bi_stream(
        self: &Arc<Self>,
        mut send: SendStream,
        mut recv: RecvStream,
        remote_id_hex: String,
        is_initiator: bool,
    ) -> Result<PeerInfo> {
        let peer_info = if is_initiator {
            let my_handshake = WireMessage::Handshake {
                node_id: self.node_id_hex.clone(),
                nickname: self.nickname.read().await.clone(),
                username: self.username.read().await.clone(),
            };
            write_wire_message(&mut send, &my_handshake).await?;
            match read_wire_message(&mut recv).await? {
                Some(WireMessage::Handshake {
                    nickname, username, ..
                }) => PeerInfo {
                    node_id: remote_id_hex.clone(),
                    nickname,
                    username,
                },
                Some(WireMessage::HandshakeReject { reason }) => {
                    return Err(anyhow!("相手に接続リクエストを拒否されました ({})", reason));
                }
                _ => return Err(anyhow!("相手からの応答が不正または切断されました")),
            }
        } else {
            let info = match read_wire_message(&mut recv).await? {
                Some(WireMessage::Handshake {
                    nickname, username, ..
                }) => PeerInfo {
                    node_id: remote_id_hex.clone(),
                    nickname,
                    username,
                },
                _ => return Err(anyhow!("着信接続のHandshakeが不正です")),
            };

            let (tx_decision, rx_decision) = oneshot::channel::<bool>();
            {
                let mut pending = self.pending_approval.lock().await;
                *pending = Some(PendingApproval {
                    peer: info.clone(),
                    responder: tx_decision,
                });
            }

            (self.event_cb)(P2PEvent::ConnectionRequested {
                peer: info.clone(),
            });
            if let Ok(snap) = self.snapshot().await {
                (self.event_cb)(P2PEvent::StateUpdated { snapshot: snap });
            }

            // Wait for user approval/rejection (or 60s timeout)
            let accepted = tokio::select! {
                res = rx_decision => res.unwrap_or(false),
                _ = tokio::time::sleep(std::time::Duration::from_secs(60)) => false,
            };

            // Clear pending slot if it still belongs to this peer
            {
                let mut pending = self.pending_approval.lock().await;
                if pending.as_ref().map(|p| &p.peer.node_id) == Some(&info.node_id) {
                    *pending = None;
                }
            }

            if !accepted {
                let reject_msg = WireMessage::HandshakeReject {
                    reason: "ユーザにより拒否されました".to_string(),
                };
                let _ = write_wire_message(&mut send, &reject_msg).await;
                let _ = send.finish();
                if let Ok(snap) = self.snapshot().await {
                    (self.event_cb)(P2PEvent::StateUpdated { snapshot: snap });
                }
                return Err(anyhow!("着信接続リクエストを拒否しました"));
            }

            let my_handshake = WireMessage::Handshake {
                node_id: self.node_id_hex.clone(),
                nickname: self.nickname.read().await.clone(),
                username: self.username.read().await.clone(),
            };
            write_wire_message(&mut send, &my_handshake).await?;
            info
        };

        // Disconnect any previous active connection
        if let Some(prev_tx) = self.active_tx.lock().await.take() {
            let _ = prev_tx.send(WireMessage::Disconnect).await;
        }

        let (tx, mut rx) = mpsc::channel::<WireMessage>(32);
        *self.active_tx.lock().await = Some(tx);
        *self.connected_peer.write().await = Some(peer_info.clone());

        (self.event_cb)(P2PEvent::PeerConnected {
            peer: peer_info.clone(),
        });
        if let Ok(snap) = self.snapshot().await {
            (self.event_cb)(P2PEvent::StateUpdated { snapshot: snap });
        }

        // Writer task
        tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                let is_disconnect = matches!(msg, WireMessage::Disconnect);
                if write_wire_message(&mut send, &msg).await.is_err() || is_disconnect {
                    let _ = send.finish();
                    break;
                }
            }
        });

        // Reader task
        let reader_node = Arc::clone(self);
        let peer_clone = peer_info.clone();
        tokio::spawn(async move {
            while let Ok(Some(wire_msg)) = read_wire_message(&mut recv).await {
                match wire_msg {
                    WireMessage::Chat {
                        id,
                        sender_id,
                        sender_name,
                        content,
                        timestamp,
                    } => {
                        let payload = ChatMessagePayload {
                            id,
                            sender_id,
                            sender_name,
                            content,
                            timestamp,
                            is_self: false,
                        };
                        reader_node.messages.write().await.push(payload.clone());
                        reader_node.save_config().await;
                        (reader_node.event_cb)(P2PEvent::ChatMessage { message: payload });
                    }
                    WireMessage::Disconnect => {
                        break;
                    }
                    _ => {}
                }
            }

            // Clean up when stream closes
            let mut active_peer = reader_node.connected_peer.write().await;
            if active_peer.as_ref().map(|p| &p.node_id) == Some(&peer_clone.node_id) {
                *active_peer = None;
                *reader_node.active_tx.lock().await = None;
                (reader_node.event_cb)(P2PEvent::PeerDisconnected { reason: None });
            }
        });

        Ok(peer_info)
    }

    pub async fn send_chat_message(&self, content: String) -> Result<ChatMessagePayload> {
        let tx = self
            .active_tx
            .lock()
            .await
            .clone()
            .ok_or_else(|| anyhow!("現在接続中のピアがいません"))?;

        let id = Uuid::new_v4().to_string();
        let sender_name = self.nickname.read().await.clone();
        let timestamp = now_millis();

        let wire = WireMessage::Chat {
            id: id.clone(),
            sender_id: self.node_id_hex.clone(),
            sender_name: sender_name.clone(),
            content: content.clone(),
            timestamp,
        };

        tx.send(wire)
            .await
            .map_err(|_| anyhow!("ピアへの送信チャネルが閉じています"))?;

        let payload = ChatMessagePayload {
            id,
            sender_id: self.node_id_hex.clone(),
            sender_name,
            content,
            timestamp,
            is_self: true,
        };

        self.messages.write().await.push(payload.clone());
        self.save_config().await;
        (self.event_cb)(P2PEvent::ChatMessage {
            message: payload.clone(),
        });

        Ok(payload)
    }

    pub async fn clear_messages(&self) -> Result<StateSnapshot> {
        self.messages.write().await.clear();
        self.save_config().await;
        let snap = self.snapshot().await?;
        (self.event_cb)(P2PEvent::StateUpdated {
            snapshot: snap.clone(),
        });
        Ok(snap)
    }

    pub async fn respond_connection_request(&self, accept: bool) -> Result<StateSnapshot> {
        let pending = self.pending_approval.lock().await.take();
        if let Some(p) = pending {
            let _ = p.responder.send(accept);
        }
        // Allow accept task to update state briefly
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        let snap = self.snapshot().await?;
        (self.event_cb)(P2PEvent::StateUpdated {
            snapshot: snap.clone(),
        });
        Ok(snap)
    }

    pub async fn disconnect(&self) -> Result<()> {
        if let Some(tx) = self.active_tx.lock().await.take() {
            let _ = tx.send(WireMessage::Disconnect).await;
        }
        *self.connected_peer.write().await = None;
        (self.event_cb)(P2PEvent::PeerDisconnected {
            reason: Some("User disconnected".into()),
        });
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::broadcast;

    #[test]
    fn test_ed25519_signature_generation() {
        let secret = [7u8; 32];
        let signing_key = SigningKey::from_bytes(&secret);
        let pub_hex = HEXLOWER.encode(signing_key.verifying_key().as_bytes());
        let sig_hex = sign_registration_payload(&signing_key, "Alice", &pub_hex, 1700000000);
        assert_eq!(sig_hex.len(), 128);
    }

    #[tokio::test]
    async fn test_config_persistence_across_restarts() -> Result<()> {
        let temp_file =
            std::env::temp_dir().join(format!("simple-chat-test-{}.json", Uuid::new_v4()));

        let node1 = ChatNode::new_with_config(Arc::new(|_| {}), Some(temp_file.clone())).await?;
        let first_node_id = node1.node_id_hex.clone();
        *node1.nickname.write().await = "AliceSaved".to_string();
        *node1.username.write().await = Some("alice_saved".to_string());
        node1.messages.write().await.push(ChatMessagePayload {
            id: "msg-1".to_string(),
            sender_id: first_node_id.clone(),
            sender_name: "AliceSaved".to_string(),
            content: "Persisted hello!".to_string(),
            timestamp: 1700000123,
            is_self: true,
        });
        node1.save_config().await;
        drop(node1);

        let node2 = ChatNode::new_with_config(Arc::new(|_| {}), Some(temp_file.clone())).await?;
        assert_eq!(node2.node_id_hex, first_node_id);
        assert_eq!(node2.nickname.read().await.as_str(), "AliceSaved");
        assert_eq!(
            node2.username.read().await.as_deref(),
            Some("alice_saved")
        );
        let loaded_msgs = node2.messages.read().await;
        assert_eq!(loaded_msgs.len(), 1);
        assert_eq!(loaded_msgs[0].content, "Persisted hello!");

        let _ = std::fs::remove_file(temp_file);
        Ok(())
    }

    #[tokio::test]
    async fn test_p2p_two_nodes_handshake_and_chat() -> Result<()> {
        let (tx_a, mut rx_a) = broadcast::channel::<P2PEvent>(16);
        let (tx_b, mut rx_b) = broadcast::channel::<P2PEvent>(16);

        let node_a = ChatNode::new(Arc::new(move |ev| {
            let _ = tx_a.send(ev);
        }))
        .await?;
        *node_a.nickname.write().await = "Alice".to_string();

        let node_b = ChatNode::new(Arc::new(move |ev| {
            let _ = tx_b.send(ev);
        }))
        .await?;
        *node_b.nickname.write().await = "Bob".to_string();

        let ticket_a = node_a.current_ticket().await?;
        let decoded_a = decode_ticket(&ticket_a)?;
        assert_eq!(
            HEXLOWER.encode(decoded_a.node_id.as_bytes()),
            node_a.node_id_hex
        );

        // 1. Test REJECT flow first: Bob tries to connect, Alice rejects
        let node_b_reject = Arc::clone(&node_b);
        let ticket_a_reject = ticket_a.clone();
        let expected_a_id = node_a.node_id_hex.clone();
        let connect_reject_handle = tokio::spawn(async move {
            node_b_reject
                .connect_by_ticket_verified(&ticket_a_reject, Some(&expected_a_id))
                .await
        });

        // Wait for Alice to receive ConnectionRequested from Bob, then reject
        for _ in 0..10 {
            if let Ok(P2PEvent::ConnectionRequested { peer }) =
                tokio::time::timeout(std::time::Duration::from_secs(2), rx_a.recv()).await?
            {
                assert_eq!(peer.nickname, "Bob");
                node_a.respond_connection_request(false).await?;
                break;
            }
        }
        let reject_res = connect_reject_handle.await?;
        assert!(reject_res.is_err(), "Rejected connection should return Err");

        // 2. Test ACCEPT flow: Bob connects again, Alice approves
        let node_b_accept = Arc::clone(&node_b);
        let ticket_a_accept = ticket_a.clone();
        let expected_a_id2 = node_a.node_id_hex.clone();
        let connect_accept_handle = tokio::spawn(async move {
            node_b_accept
                .connect_by_ticket_verified(&ticket_a_accept, Some(&expected_a_id2))
                .await
        });

        // Wait for Alice to receive ConnectionRequested and approve it
        for _ in 0..10 {
            if let Ok(P2PEvent::ConnectionRequested { peer }) =
                tokio::time::timeout(std::time::Duration::from_secs(2), rx_a.recv()).await?
            {
                assert_eq!(peer.nickname, "Bob");
                node_a.respond_connection_request(true).await?;
                break;
            }
        }

        let peer_a_info = connect_accept_handle.await??;
        assert_eq!(peer_a_info.nickname, "Alice");
        assert_eq!(peer_a_info.node_id, node_a.node_id_hex);

        // Wait for Node A to register peer-connected event
        let mut a_connected = false;
        for _ in 0..10 {
            if let Ok(P2PEvent::PeerConnected { peer }) =
                tokio::time::timeout(std::time::Duration::from_secs(2), rx_a.recv()).await?
            {
                assert_eq!(peer.nickname, "Bob");
                a_connected = true;
                break;
            }
        }
        assert!(a_connected, "Node A should receive PeerConnected from Bob");

        // Bob sends a chat message to Alice
        node_b
            .send_chat_message("Hello Alice from P2P!".to_string())
            .await?;

        // Alice should receive the chat message event and store it in history
        let mut received_text = None;
        for _ in 0..10 {
            if let Ok(P2PEvent::ChatMessage { message }) =
                tokio::time::timeout(std::time::Duration::from_secs(2), rx_a.recv()).await?
            {
                if !message.is_self {
                    received_text = Some(message.content);
                    break;
                }
            }
        }
        assert_eq!(received_text.as_deref(), Some("Hello Alice from P2P!"));

        // Verify history is persisted in Node A's snapshot
        let snap_a = node_a.snapshot().await?;
        assert_eq!(snap_a.messages.len(), 1);
        assert_eq!(snap_a.messages[0].content, "Hello Alice from P2P!");

        // Verify Bob's self-sent message is also in Bob's history
        let _ = rx_b.recv().await;
        let snap_b = node_b.snapshot().await?;
        assert_eq!(snap_b.messages.len(), 1);

        node_b.disconnect().await?;
        Ok(())
    }
}
