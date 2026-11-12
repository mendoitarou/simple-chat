use anyhow::{anyhow, Context, Result};
use data_encoding::{BASE64URL_NOPAD, HEXLOWER};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use iroh::endpoint::{RecvStream, SendStream};
use iroh::{Endpoint, NodeAddr, SecretKey};
use rand::rngs::OsRng;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, oneshot, Mutex, RwLock};
use uuid::Uuid;

pub const CHAT_ALPN: &[u8] = b"simple-chat/p2p/1";
pub const LAN_MATCH_PORT_START: u16 = 41920;
pub const LAN_MATCH_PORT_END: u16 = 41939;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LanMatchBeacon {
    pub node_id: String,
    pub nickname: String,
    pub username: Option<String>,
    pub ticket: String,
    pub match_tag: String,
    pub timestamp: u64,
    pub signature_hex: String,
}

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
pub struct PendingRequestInfo {
    pub peer: PeerInfo,
    pub introduced_by: Option<String>,
    pub room_name: Option<String>,
    #[serde(default)]
    pub matched_tag: Option<String>,
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
    #[serde(default)]
    pub channel_kind: String,
    #[serde(default)]
    pub channel_id: String,
    #[serde(default)]
    pub channel_name: String,
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
pub struct GroupRoomMember {
    pub node_id: String,
    pub nickname: String,
    pub username: Option<String>,
    pub ticket: String,
    pub last_seen: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupRoom {
    pub room_id: String,
    pub name: String,
    pub host_node_id: String,
    pub host_name: String,
    #[serde(default)]
    pub has_passphrase: bool,
    pub members: Vec<GroupRoomMember>,
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
    pub matching_enabled: bool,
    pub matching_tag: String,
    pub room_id: Option<String>,
    pub room_name: Option<String>,
    pub room_has_passphrase: bool,
    pub active_channel_id: Option<String>,
    pub connected_peer: Option<PeerInfo>,
    pub connected_peers: Vec<PeerInfo>,
    pub pending_request: Option<PeerInfo>,
    pub pending_requests: Vec<PendingRequestInfo>,
    pub messages: Vec<ChatMessagePayload>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum P2PEvent {
    ConnectionRequested {
        peer: PeerInfo,
        introduced_by: Option<String>,
        room_name: Option<String>,
        matched_tag: Option<String>,
    },
    PeerConnected {
        peer: PeerInfo,
    },
    PeerDisconnected {
        peer: Option<PeerInfo>,
        reason: Option<String>,
    },
    ChatMessage {
        message: ChatMessagePayload,
    },
    StateUpdated {
        snapshot: StateSnapshot,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WireMessage {
    Handshake {
        node_id: String,
        nickname: String,
        username: Option<String>,
        #[serde(default)]
        ticket: String,
        #[serde(default)]
        room_id: Option<String>,
        #[serde(default)]
        room_name: Option<String>,
        #[serde(default)]
        room_passphrase_hash: Option<String>,
        #[serde(default)]
        introduced_by_node_id: Option<String>,
        #[serde(default)]
        matched_tag: Option<String>,
        #[serde(default)]
        match_session_id: Option<String>,
    },
    HandshakeReject {
        reason: String,
    },
    PreAuthorizePeer {
        node_id: String,
    },
    PeerIntroduction {
        peer: PeerInfo,
        ticket: String,
        introduced_by: String,
        introduced_by_node_id: String,
        room_id: Option<String>,
        room_name: Option<String>,
        #[serde(default)]
        room_passphrase_hash: Option<String>,
    },
    RoomUpdate {
        room_id: Option<String>,
        room_name: Option<String>,
        #[serde(default)]
        room_passphrase_hash: Option<String>,
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

/// Sign arbitrary room payload `${prefix}:${room_id}:${node_id}:${timestamp}` with Ed25519 signing key.
pub fn sign_room_payload(
    signing_key: &SigningKey,
    prefix: &str,
    room_id: &str,
    node_id_hex: &str,
    timestamp: u64,
) -> String {
    let payload = format!("{}:{}:{}:{}", prefix, room_id, node_id_hex, timestamp);
    let sig = signing_key.sign(payload.as_bytes());
    HEXLOWER.encode(&sig.to_bytes())
}

/// Deterministically hash a room passphrase scoped to `room_id` using RFC 8032 Ed25519 (SHA-512).
pub fn hash_room_passphrase(room_id: &str, passphrase: &str) -> Option<String> {
    let clean = passphrase.trim();
    if clean.is_empty() {
        return None;
    }
    const ROOM_PASS_DOMAIN_KEY: [u8; 32] = *b"SimpleChatRoomPassphraseDomainV1";
    let key = SigningKey::from_bytes(&ROOM_PASS_DOMAIN_KEY);
    let msg = format!("room-passphrase:{}:{}", room_id.trim(), clean);
    let sig = key.sign(msg.as_bytes());
    Some(HEXLOWER.encode(&sig.to_bytes())[..64].to_string())
}

/// Sign LAN matching beacon payload.
pub fn sign_lan_beacon(
    signing_key: &SigningKey,
    node_id_hex: &str,
    match_tag: &str,
    ticket: &str,
    timestamp: u64,
) -> String {
    let payload = format!(
        "lanmatch:{}:{}:{}:{}",
        node_id_hex,
        match_tag.trim().to_lowercase(),
        ticket,
        timestamp
    );
    let sig = signing_key.sign(payload.as_bytes());
    HEXLOWER.encode(&sig.to_bytes())
}

/// Verify Ed25519 signature and ticket node_id of a `LanMatchBeacon`.
pub fn verify_lan_beacon(beacon: &LanMatchBeacon) -> bool {
    let now = now_millis();
    if now.abs_diff(beacon.timestamp) > 60_000 {
        return false;
    }
    let Ok(pub_bytes) = HEXLOWER.decode(beacon.node_id.trim().as_bytes()) else {
        return false;
    };
    let Ok(pub_arr): Result<[u8; 32], _> = pub_bytes.try_into() else {
        return false;
    };
    let Ok(verifying_key) = VerifyingKey::from_bytes(&pub_arr) else {
        return false;
    };
    let Ok(sig_bytes) = HEXLOWER.decode(beacon.signature_hex.trim().as_bytes()) else {
        return false;
    };
    let Ok(sig_arr): Result<[u8; 64], _> = sig_bytes.try_into() else {
        return false;
    };
    let sig = Signature::from_bytes(&sig_arr);
    let payload = format!(
        "lanmatch:{}:{}:{}:{}",
        beacon.node_id,
        beacon.match_tag.trim().to_lowercase(),
        beacon.ticket,
        beacon.timestamp
    );
    if verifying_key.verify(payload.as_bytes(), &sig).is_err() {
        return false;
    }
    if let Ok(addr) = decode_ticket(&beacon.ticket) {
        HEXLOWER.encode(addr.node_id.as_bytes()) == beacon.node_id
    } else {
        false
    }
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

#[derive(Clone)]
struct PeerSession {
    info: PeerInfo,
    tx: mpsc::Sender<WireMessage>,
    matched_tag: Option<String>,
    match_session_id: Option<String>,
}

fn migrate_legacy_messages(messages: &mut [ChatMessagePayload]) {
    let first_peer: Option<(String, String)> = messages
        .iter()
        .find(|m| !m.is_self && !m.sender_id.is_empty())
        .map(|m| (m.sender_id.clone(), m.sender_name.clone()));

    let mut last_seen_peer: Option<(String, String)> = first_peer;

    for m in messages.iter_mut() {
        if !m.is_self && !m.sender_id.is_empty() {
            last_seen_peer = Some((m.sender_id.clone(), m.sender_name.clone()));
        }
        if m.channel_id.trim().is_empty() {
            m.channel_kind = "direct".to_string();
            if !m.is_self && !m.sender_id.is_empty() {
                m.channel_id = format!("direct:{}", m.sender_id);
                m.channel_name = m.sender_name.clone();
            } else if let Some((ref pid, ref pname)) = last_seen_peer {
                m.channel_id = format!("direct:{}", pid);
                m.channel_name = pname.clone();
            } else {
                m.channel_id = "direct:legacy".to_string();
                m.channel_name = "過去のチャット".to_string();
            }
        }
    }
}

enum PendingAction {
    IncomingStream {
        responder: oneshot::Sender<bool>,
    },
    IntroducedDial {
        ticket: String,
        introduced_by_node_id: String,
        room_id: Option<String>,
        room_name: Option<String>,
    },
    LanMatched {
        ticket: String,
    },
}

struct PendingApprovalEntry {
    info: PendingRequestInfo,
    action: PendingAction,
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
    pub matching_enabled: RwLock<bool>,
    pub matching_tag: RwLock<String>,
    pub room_id: RwLock<Option<String>>,
    pub room_name: RwLock<Option<String>>,
    pub room_passphrase_hash: RwLock<Option<String>>,
    peers: RwLock<HashMap<String, PeerSession>>,
    pre_authorized_peers: Mutex<HashSet<String>>,
    pending_match_tags: Mutex<HashMap<String, String>>,
    dismissed_match_peers: Mutex<HashSet<String>>,
    pending_approvals: Mutex<Vec<PendingApprovalEntry>>,
    lan_socket: Option<Arc<tokio::net::UdpSocket>>,
    pub messages: RwLock<Vec<ChatMessagePayload>>,
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
            migrate_legacy_messages(&mut initial_messages);
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

        // Bind a UDP socket in LAN_MATCH_PORT_START..=LAN_MATCH_PORT_END for local network encounter matching
        let mut bound_udp = None;
        for port in LAN_MATCH_PORT_START..=LAN_MATCH_PORT_END {
            if let Ok(sock) = tokio::net::UdpSocket::bind(("0.0.0.0", port)).await {
                let _ = sock.set_broadcast(true);
                bound_udp = Some(Arc::new(sock));
                break;
            }
        }
        if bound_udp.is_none() {
            if let Ok(sock) = tokio::net::UdpSocket::bind(("0.0.0.0", 0)).await {
                let _ = sock.set_broadcast(true);
                bound_udp = Some(Arc::new(sock));
            }
        }

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
            matching_enabled: RwLock::new(false),
            matching_tag: RwLock::new(String::new()),
            room_id: RwLock::new(None),
            room_name: RwLock::new(None),
            room_passphrase_hash: RwLock::new(None),
            peers: RwLock::new(HashMap::new()),
            pre_authorized_peers: Mutex::new(HashSet::new()),
            pending_match_tags: Mutex::new(HashMap::new()),
            dismissed_match_peers: Mutex::new(HashSet::new()),
            pending_approvals: Mutex::new(Vec::new()),
            lan_socket: bound_udp.clone(),
            messages: RwLock::new(initial_messages),
            event_cb,
            http_client: reqwest::Client::new(),
        });

        // Persist initial config (so newly generated Ed25519 keypair is saved immediately)
        node.save_config().await;

        // If already registered previously, automatically re-register on startup
        if initial_registered {
            let startup_node = Arc::clone(&node);
            tokio::spawn(async move {
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

        // Spawn LAN encounter matching loop
        if let Some(sock) = bound_udp {
            let lan_node = Arc::clone(&node);
            tokio::spawn(async move {
                lan_node.lan_matching_loop(sock).await;
            });
        }

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

    pub async fn current_channel_meta(&self) -> (String, String, String) {
        let room_id = self.room_id.read().await.clone();
        let room_name = self.room_name.read().await.clone();
        let peers_vec: Vec<PeerSession> = self.peers.read().await.values().cloned().collect();

        if room_id.is_some() || peers_vec.len() > 1 {
            let cid = if let Some(ref rid) = room_id {
                format!("group:{}", rid)
            } else {
                let mut short_ids: Vec<String> = peers_vec
                    .iter()
                    .map(|p| p.info.node_id[..8.min(p.info.node_id.len())].to_string())
                    .collect();
                short_ids.push(self.node_id_hex[..8.min(self.node_id_hex.len())].to_string());
                short_ids.sort();
                format!("group:adhoc:{}", short_ids.join("-"))
            };
            let cname = if let Some(ref rname) = room_name {
                rname.clone()
            } else {
                let mut names: Vec<String> =
                    peers_vec.iter().map(|p| p.info.nickname.clone()).collect();
                names.sort();
                format!("グループ ({})", names.join(", "))
            };
            return ("group".to_string(), cid, cname);
        }

        if let Some(p) = peers_vec.first() {
            if let Some(ref tag) = p.matched_tag {
                let sid = p
                    .match_session_id
                    .clone()
                    .unwrap_or_else(|| p.info.node_id.clone());
                let cname = if tag.trim().is_empty() {
                    format!("{} (誰でも)", p.info.nickname)
                } else {
                    format!("{} (#{})", p.info.nickname, tag.trim())
                };
                return ("match".to_string(), format!("match:{}", sid), cname);
            } else {
                return (
                    "direct".to_string(),
                    format!("direct:{}", p.info.node_id),
                    p.info.nickname.clone(),
                );
            }
        }

        ("".to_string(), "".to_string(), "".to_string())
    }

    pub async fn channel_meta_for_peer(&self, peer_node_id: &str) -> (String, String, String) {
        let (kind, cid, cname) = self.current_channel_meta().await;
        if !cid.is_empty() {
            return (kind, cid, cname);
        }
        if let Some(p) = self.peers.read().await.get(peer_node_id) {
            if let Some(ref tag) = p.matched_tag {
                let sid = p
                    .match_session_id
                    .clone()
                    .unwrap_or_else(|| p.info.node_id.clone());
                let cname = if tag.trim().is_empty() {
                    format!("{} (誰でも)", p.info.nickname)
                } else {
                    format!("{} (#{})", p.info.nickname, tag.trim())
                };
                return ("match".to_string(), format!("match:{}", sid), cname);
            }
            return (
                "direct".to_string(),
                format!("direct:{}", p.info.node_id),
                p.info.nickname.clone(),
            );
        }
        (
            "direct".to_string(),
            format!("direct:{}", peer_node_id),
            peer_node_id[..8.min(peer_node_id.len())].to_string(),
        )
    }

    pub async fn snapshot(&self) -> Result<StateSnapshot> {
        let ticket = self.current_ticket().await.unwrap_or_default();
        let connected_peers: Vec<PeerInfo> = self
            .peers
            .read()
            .await
            .values()
            .map(|s| s.info.clone())
            .collect();
        let connected_peer = connected_peers.first().cloned();

        let pending_requests: Vec<PendingRequestInfo> = self
            .pending_approvals
            .lock()
            .await
            .iter()
            .map(|p| p.info.clone())
            .collect();
        let pending_request = pending_requests.first().map(|r| r.peer.clone());

        let (_, ch_id, _) = self.current_channel_meta().await;
        let active_channel_id = if ch_id.is_empty() { None } else { Some(ch_id) };

        Ok(StateSnapshot {
            node_id: self.node_id_hex.clone(),
            ticket,
            nickname: self.nickname.read().await.clone(),
            username: self.username.read().await.clone(),
            server_url: self.server_url.read().await.clone(),
            registered_on_server: *self.registered_on_server.read().await,
            matching_enabled: *self.matching_enabled.read().await,
            matching_tag: self.matching_tag.read().await.clone(),
            room_id: self.room_id.read().await.clone(),
            room_name: self.room_name.read().await.clone(),
            room_has_passphrase: self.room_passphrase_hash.read().await.is_some(),
            active_channel_id,
            connected_peer,
            connected_peers,
            pending_request,
            pending_requests,
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
                        let _ = node
                            .handle_bi_stream(send, recv, remote_id_hex, false, None)
                            .await;
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
            if self.room_id.read().await.is_some() {
                let _ = self.push_room_to_server().await;
            }
        }
    }

    async fn lan_matching_loop(self: Arc<Self>, sock: Arc<tokio::net::UdpSocket>) {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(2500));
        let mut buf = vec![0u8; 8192];
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if *self.matching_enabled.read().await {
                        let _ = self.broadcast_lan_beacon(&sock).await;
                    }
                }
                recv_res = sock.recv_from(&mut buf) => {
                    if let Ok((len, _addr)) = recv_res {
                        if *self.matching_enabled.read().await {
                            if let Ok(beacon) = serde_json::from_slice::<LanMatchBeacon>(&buf[..len]) {
                                self.process_lan_beacon(beacon).await;
                            }
                        }
                    }
                }
            }
        }
    }

    async fn broadcast_lan_beacon(&self, sock: &tokio::net::UdpSocket) -> Result<()> {
        let ticket = self.current_ticket().await?;
        let matching_tag = self.matching_tag.read().await.clone();
        let nickname = self.nickname.read().await.clone();
        let username = self.username.read().await.clone();
        let timestamp = now_millis();
        let signature_hex = sign_lan_beacon(
            &self.signing_key,
            &self.node_id_hex,
            &matching_tag,
            &ticket,
            timestamp,
        );
        let beacon = LanMatchBeacon {
            node_id: self.node_id_hex.clone(),
            nickname,
            username,
            ticket,
            match_tag: matching_tag,
            timestamp,
            signature_hex,
        };
        let payload = serde_json::to_vec(&beacon)?;
        for port in LAN_MATCH_PORT_START..=LAN_MATCH_PORT_END {
            let _ = sock
                .send_to(&payload, format!("255.255.255.255:{}", port))
                .await;
            let _ = sock
                .send_to(&payload, format!("127.0.0.1:{}", port))
                .await;
        }
        Ok(())
    }

    async fn process_lan_beacon(self: &Arc<Self>, beacon: LanMatchBeacon) {
        if beacon.node_id == self.node_id_hex {
            return;
        }
        if !verify_lan_beacon(&beacon) {
            return;
        }
        let my_tag = self.matching_tag.read().await.trim().to_lowercase();
        let peer_tag = beacon.match_tag.trim().to_lowercase();
        if my_tag != peer_tag {
            return;
        }
        if self.peers.read().await.contains_key(&beacon.node_id) {
            return;
        }
        if self
            .dismissed_match_peers
            .lock()
            .await
            .contains(&beacon.node_id)
        {
            return;
        }

        let mut pending = self.pending_approvals.lock().await;
        if pending.iter().any(|p| p.info.peer.node_id == beacon.node_id) {
            return;
        }

        let peer_info = PeerInfo {
            node_id: beacon.node_id.clone(),
            nickname: beacon.nickname,
            username: beacon.username,
        };
        let tag_display = beacon.match_tag.trim().to_string();

        pending.push(PendingApprovalEntry {
            info: PendingRequestInfo {
                peer: peer_info.clone(),
                introduced_by: None,
                room_name: None,
                matched_tag: Some(tag_display.clone()),
            },
            action: PendingAction::LanMatched {
                ticket: beacon.ticket,
            },
        });
        drop(pending);

        (self.event_cb)(P2PEvent::ConnectionRequested {
            peer: peer_info,
            introduced_by: None,
            room_name: None,
            matched_tag: Some(tag_display),
        });
        if let Ok(snap) = self.snapshot().await {
            (self.event_cb)(P2PEvent::StateUpdated { snapshot: snap });
        }
    }

    pub async fn set_lan_matching(
        self: &Arc<Self>,
        enabled: bool,
        matching_tag: String,
    ) -> Result<StateSnapshot> {
        let clean_tag = matching_tag.trim().to_string();
        *self.matching_enabled.write().await = enabled;
        *self.matching_tag.write().await = clean_tag;
        self.dismissed_match_peers.lock().await.clear();

        if !enabled {
            let mut pending = self.pending_approvals.lock().await;
            pending.retain(|p| !matches!(p.action, PendingAction::LanMatched { .. }));
        } else if let Some(ref sock) = self.lan_socket {
            let _ = self.broadcast_lan_beacon(sock).await;
        }

        let snap = self.snapshot().await?;
        (self.event_cb)(P2PEvent::StateUpdated {
            snapshot: snap.clone(),
        });
        Ok(snap)
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

    pub async fn create_group_room(
        self: &Arc<Self>,
        room_name: String,
        passphrase: Option<String>,
    ) -> Result<StateSnapshot> {
        let clean_name = room_name.trim().to_string();
        if clean_name.is_empty() {
            return Err(anyhow!("ルーム名を入力してください"));
        }
        let has_pass = passphrase
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .is_some();
        let prefix = if has_pass { "room-locked-" } else { "room-" };

        let existing_opt = self.room_id.read().await.clone();
        let r_id = match existing_opt {
            Some(ref old_id)
                if old_id.starts_with("room-locked-") == has_pass =>
            {
                old_id.clone()
            }
            Some(old_id) => {
                self.leave_room_on_server(&old_id).await;
                format!("{}{}", prefix, &Uuid::new_v4().to_string()[..8])
            }
            None => format!("{}{}", prefix, &Uuid::new_v4().to_string()[..8]),
        };

        let pass_hash = passphrase
            .as_deref()
            .and_then(|p| hash_room_passphrase(&r_id, p));

        *self.room_id.write().await = Some(r_id.clone());
        *self.room_name.write().await = Some(clean_name.clone());
        *self.room_passphrase_hash.write().await = pass_hash.clone();

        // Best-effort sync with directory server if reachable
        let _ = self.push_room_to_server().await;

        // Broadcast room update to any already-connected peers
        let sessions: Vec<PeerSession> = self.peers.read().await.values().cloned().collect();
        for s in sessions {
            let _ = s
                .tx
                .send(WireMessage::RoomUpdate {
                    room_id: Some(r_id.clone()),
                    room_name: Some(clean_name.clone()),
                    room_passphrase_hash: pass_hash.clone(),
                })
                .await;
        }

        let snap = self.snapshot().await?;
        (self.event_cb)(P2PEvent::StateUpdated {
            snapshot: snap.clone(),
        });
        Ok(snap)
    }

    async fn push_room_to_server(&self) -> Result<GroupRoom> {
        let room_id = self
            .room_id
            .read()
            .await
            .clone()
            .ok_or_else(|| anyhow!("Room ID not set"))?;
        let name = self
            .room_name
            .read()
            .await
            .clone()
            .unwrap_or_else(|| room_id.clone());
        let passphrase_hash = self.room_passphrase_hash.read().await.clone();
        let server_url = self.server_url.read().await.clone();
        let nickname = self.nickname.read().await.clone();
        let username = self.username.read().await.clone();
        let ticket = self.current_ticket().await?;
        let timestamp = now_millis();
        let signature_hex = sign_room_payload(
            &self.signing_key,
            "room",
            &room_id,
            &self.node_id_hex,
            timestamp,
        );

        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct RoomReq {
            room_id: String,
            name: String,
            passphrase_hash: Option<String>,
            node_id: String,
            public_key_hex: String,
            nickname: String,
            username: Option<String>,
            ticket: String,
            timestamp: u64,
            signature_hex: String,
        }

        #[derive(Deserialize)]
        struct RoomResp {
            room: GroupRoom,
        }

        let res = self
            .http_client
            .post(format!("{}/api/rooms", server_url))
            .json(&RoomReq {
                room_id,
                name,
                passphrase_hash,
                node_id: self.node_id_hex.clone(),
                public_key_hex: self.node_id_hex.clone(),
                nickname,
                username,
                ticket,
                timestamp,
                signature_hex,
            })
            .send()
            .await
            .context("管理サーバへのルーム登録に失敗しました")?;

        if !res.status().is_success() {
            let err_text = res.text().await.unwrap_or_default();
            return Err(anyhow!("ルーム登録エラー: {}", err_text));
        }
        let data: RoomResp = res.json().await?;
        Ok(data.room)
    }

    async fn leave_room_on_server(&self, room_id: &str) {
        let server_url = self.server_url.read().await.clone();
        let timestamp = now_millis();
        let signature_hex = sign_room_payload(
            &self.signing_key,
            "leave",
            room_id,
            &self.node_id_hex,
            timestamp,
        );

        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct LeaveReq {
            node_id: String,
            public_key_hex: String,
            timestamp: u64,
            signature_hex: String,
        }

        let _ = self
            .http_client
            .post(format!("{}/api/rooms/{}/leave", server_url, room_id))
            .json(&LeaveReq {
                node_id: self.node_id_hex.clone(),
                public_key_hex: self.node_id_hex.clone(),
                timestamp,
                signature_hex,
            })
            .send()
            .await;
    }

    pub async fn fetch_group_rooms(&self) -> Result<Vec<GroupRoom>> {
        let server_url = self.server_url.read().await.clone();
        #[derive(Deserialize)]
        struct RoomsResp {
            rooms: Vec<GroupRoom>,
        }
        let mut resp: RoomsResp = self
            .http_client
            .get(format!("{}/api/rooms", server_url))
            .send()
            .await
            .context("管理サーバへの接続に失敗しました")?
            .json()
            .await
            .context("ルーム一覧の解析に失敗しました")?;
        for r in &mut resp.rooms {
            if r.room_id.starts_with("room-locked-") {
                r.has_passphrase = true;
            }
        }
        Ok(resp.rooms)
    }

    pub async fn join_group_room(
        self: &Arc<Self>,
        room_id: &str,
        passphrase: Option<String>,
    ) -> Result<StateSnapshot> {
        let rooms = self.fetch_group_rooms().await?;
        let room = rooms
            .into_iter()
            .find(|r| r.room_id == room_id)
            .ok_or_else(|| anyhow!("指定されたグループルームが見つかりません"))?;

        if room.has_passphrase
            && passphrase
                .as_deref()
                .map(str::trim)
                .unwrap_or("")
                .is_empty()
        {
            return Err(anyhow!("このルームに参加するには合言葉の入力が必要です"));
        }

        let target_member = room
            .members
            .iter()
            .find(|m| m.node_id != self.node_id_hex)
            .ok_or_else(|| anyhow!("ルーム内に接続可能な他メンバーがいません"))?;

        let pass_hash = passphrase
            .as_deref()
            .and_then(|p| hash_room_passphrase(&room.room_id, p));

        *self.room_id.write().await = Some(room.room_id.clone());
        *self.room_name.write().await = Some(room.name.clone());
        *self.room_passphrase_hash.write().await = pass_hash;

        if let Err(err) = self
            .connect_by_ticket_verified(&target_member.ticket, Some(&target_member.node_id))
            .await
        {
            if self.peers.read().await.is_empty() {
                *self.room_id.write().await = None;
                *self.room_name.write().await = None;
                *self.room_passphrase_hash.write().await = None;
            }
            return Err(err);
        }

        let _ = self.push_room_to_server().await;
        self.snapshot().await
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
        self.connect_by_ticket_internal(ticket_str, expected_node_id_hex, None)
            .await
    }

    async fn connect_by_ticket_internal(
        self: &Arc<Self>,
        ticket_str: &str,
        expected_node_id_hex: Option<&str>,
        introduced_by_node_id: Option<String>,
    ) -> Result<PeerInfo> {
        let node_addr = decode_ticket(ticket_str)?;
        let ticket_node_hex = HEXLOWER.encode(node_addr.node_id.as_bytes());

        if ticket_node_hex == self.node_id_hex {
            return Err(anyhow!("自分自身のチケットには接続できません"));
        }

        if let Some(expected) = expected_node_id_hex {
            if ticket_node_hex != expected.to_lowercase() {
                return Err(anyhow!(
                    "公開鍵不一致エラー: チケットのNodeIDがサーバ登録の公開鍵と一致しません"
                ));
            }
        }

        if let Some(existing) = self.peers.read().await.get(&ticket_node_hex) {
            return Ok(existing.info.clone());
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
            .handle_bi_stream(send, recv, remote_id_hex, true, introduced_by_node_id)
            .await?;
        Ok(peer)
    }

    async fn handle_bi_stream(
        self: &Arc<Self>,
        mut send: SendStream,
        mut recv: RecvStream,
        remote_id_hex: String,
        is_initiator: bool,
        introduced_by_node_id: Option<String>,
    ) -> Result<PeerInfo> {
        let my_ticket = self.current_ticket().await.unwrap_or_default();
        let is_introduced_link = introduced_by_node_id.is_some();

        let (
            peer_info,
            peer_ticket,
            remote_room_id,
            remote_room_name,
            remote_room_pass_hash,
            final_matched_tag,
            final_match_sid,
        ) = if is_initiator {
            let init_match_tag = self
                .pending_match_tags
                .lock()
                .await
                .remove(&remote_id_hex.to_lowercase());
            let init_match_sid = init_match_tag
                .as_ref()
                .map(|_| Uuid::new_v4().to_string()[..8].to_string());

            let my_handshake = WireMessage::Handshake {
                node_id: self.node_id_hex.clone(),
                nickname: self.nickname.read().await.clone(),
                username: self.username.read().await.clone(),
                ticket: my_ticket,
                room_id: self.room_id.read().await.clone(),
                room_name: self.room_name.read().await.clone(),
                room_passphrase_hash: self.room_passphrase_hash.read().await.clone(),
                introduced_by_node_id,
                matched_tag: init_match_tag.clone(),
                match_session_id: init_match_sid.clone(),
            };
            write_wire_message(&mut send, &my_handshake).await?;
            match read_wire_message(&mut recv).await? {
                Some(WireMessage::Handshake {
                    nickname,
                    username,
                    ticket,
                    room_id,
                    room_name,
                    room_passphrase_hash,
                    matched_tag: resp_matched_tag,
                    match_session_id: resp_match_sid,
                    ..
                }) => (
                    PeerInfo {
                        node_id: remote_id_hex.clone(),
                        nickname,
                        username,
                    },
                    ticket,
                    room_id,
                    room_name,
                    room_passphrase_hash,
                    init_match_tag.or(resp_matched_tag),
                    init_match_sid.or(resp_match_sid),
                ),
                Some(WireMessage::HandshakeReject { reason }) => {
                    return Err(anyhow!("相手に接続リクエストを拒否されました ({})", reason));
                }
                _ => return Err(anyhow!("相手からの応答が不正または切断されました")),
            }
        } else {
            let (
                info,
                remote_ticket,
                r_id,
                r_name,
                r_pass_hash,
                intro_node_id,
                remote_matched_tag,
                remote_match_sid,
            ) = match read_wire_message(&mut recv).await? {
                Some(WireMessage::Handshake {
                    nickname,
                    username,
                    ticket,
                    room_id,
                    room_name,
                    room_passphrase_hash,
                    introduced_by_node_id,
                    matched_tag,
                    match_session_id,
                    ..
                }) => (
                    PeerInfo {
                        node_id: remote_id_hex.clone(),
                        nickname,
                        username,
                    },
                    ticket,
                    room_id,
                    room_name,
                    room_passphrase_hash,
                    introduced_by_node_id,
                    matched_tag,
                    match_session_id,
                ),
                _ => return Err(anyhow!("着信接続のHandshakeが不正です")),
            };

            let local_pending_tag = self
                .pending_match_tags
                .lock()
                .await
                .remove(&remote_id_hex.to_lowercase());
            let existing_approval_tag = self
                .pending_approvals
                .lock()
                .await
                .iter()
                .find(|p| p.info.peer.node_id == info.node_id)
                .and_then(|p| p.info.matched_tag.clone());
            let combined_matched_tag = remote_matched_tag
                .or(local_pending_tag)
                .or(existing_approval_tag);

            // Check if this peer was pre-authorized by an existing group member or authenticated via room passphrase
            let my_room_pass_hash = self.room_passphrase_hash.read().await.clone();
            let mut is_pre_auth = {
                let mut pre = self.pre_authorized_peers.lock().await;
                let in_set = pre.remove(&remote_id_hex);
                let introducer_connected = if let Some(ref intro_id) = intro_node_id {
                    self.peers.read().await.contains_key(intro_id)
                } else {
                    false
                };
                in_set || introducer_connected
            };

            // Verify room passphrase if this node is hosting/in a passphrase-protected room
            if !is_pre_auth {
                if let Some(ref required_hash) = my_room_pass_hash {
                    if r_pass_hash.as_ref() == Some(required_hash) {
                        // Peer proved knowledge of the secret room passphrase!
                        is_pre_auth = true;
                    } else if r_id.is_some() && r_id == *self.room_id.read().await {
                        // Peer attempted to join our room with a wrong/missing passphrase
                        let reject_msg = WireMessage::HandshakeReject {
                            reason: "ルームの合言葉が一致しません".to_string(),
                        };
                        let _ = write_wire_message(&mut send, &reject_msg).await;
                        let _ = send.finish();
                        return Err(anyhow!("ルームの合言葉が一致しないため着信を拒否しました"));
                    }
                }
            }

            if !is_pre_auth {
                let (tx_decision, rx_decision) = oneshot::channel::<bool>();
                let was_already_pending = {
                    let mut pending = self.pending_approvals.lock().await;
                    let had_existing = pending.iter().any(|p| p.info.peer.node_id == info.node_id);
                    pending.retain(|p| p.info.peer.node_id != info.node_id);
                    pending.push(PendingApprovalEntry {
                        info: PendingRequestInfo {
                            peer: info.clone(),
                            introduced_by: None,
                            room_name: r_name.clone(),
                            matched_tag: combined_matched_tag.clone(),
                        },
                        action: PendingAction::IncomingStream {
                            responder: tx_decision,
                        },
                    });
                    if !had_existing {
                        drop(pending);
                        (self.event_cb)(P2PEvent::ConnectionRequested {
                            peer: info.clone(),
                            introduced_by: None,
                            room_name: r_name.clone(),
                            matched_tag: combined_matched_tag.clone(),
                        });
                    }
                    had_existing
                };

                if !was_already_pending {
                    if let Ok(snap) = self.snapshot().await {
                        (self.event_cb)(P2PEvent::StateUpdated { snapshot: snap });
                    }
                }

                let accepted = tokio::select! {
                    res = rx_decision => res.unwrap_or(false),
                    _ = tokio::time::sleep(std::time::Duration::from_secs(60)) => false,
                };

                {
                    let mut pending = self.pending_approvals.lock().await;
                    pending.retain(|p| p.info.peer.node_id != info.node_id);
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
            } else {
                let mut pending = self.pending_approvals.lock().await;
                pending.retain(|p| p.info.peer.node_id != info.node_id);
            }

            let combined_match_sid = remote_match_sid.or_else(|| {
                combined_matched_tag
                    .as_ref()
                    .map(|_| Uuid::new_v4().to_string()[..8].to_string())
            });

            let my_handshake = WireMessage::Handshake {
                node_id: self.node_id_hex.clone(),
                nickname: self.nickname.read().await.clone(),
                username: self.username.read().await.clone(),
                ticket: my_ticket,
                room_id: self.room_id.read().await.clone(),
                room_name: self.room_name.read().await.clone(),
                room_passphrase_hash: self.room_passphrase_hash.read().await.clone(),
                introduced_by_node_id: None,
                matched_tag: combined_matched_tag.clone(),
                match_session_id: combined_match_sid.clone(),
            };
            write_wire_message(&mut send, &my_handshake).await?;
            (
                info,
                remote_ticket,
                r_id,
                r_name,
                r_pass_hash,
                combined_matched_tag,
                combined_match_sid,
            )
        };

        // Clear any pending LAN match approval for this peer now that we're connected
        {
            let mut pending = self.pending_approvals.lock().await;
            pending.retain(|p| p.info.peer.node_id != peer_info.node_id);
        }

        // Adopt room metadata if we didn't have one yet
        if self.room_id.read().await.is_none() && remote_room_id.is_some() {
            *self.room_id.write().await = remote_room_id;
            *self.room_name.write().await = remote_room_name;
            if self.room_passphrase_hash.read().await.is_none() && remote_room_pass_hash.is_some() {
                *self.room_passphrase_hash.write().await = remote_room_pass_hash;
            }
        }

        // Snapshot existing peers BEFORE inserting the new peer so we can introduce them for full-mesh
        let existing_peers: Vec<PeerSession> = self
            .peers
            .read()
            .await
            .values()
            .filter(|s| s.info.node_id != peer_info.node_id)
            .cloned()
            .collect();

        let (tx, mut rx) = mpsc::channel::<WireMessage>(64);
        let new_session = PeerSession {
            info: peer_info.clone(),
            tx: tx.clone(),
            matched_tag: final_matched_tag,
            match_session_id: final_match_sid,
        };

        // Replace any old stream for the same peer node_id
        if let Some(old) = self
            .peers
            .write()
            .await
            .insert(peer_info.node_id.clone(), new_session)
        {
            let _ = old.tx.send(WireMessage::Disconnect).await;
        }

        // If weAlready had other peers in our session (and this wasn't already an introduced secondary link),
        // introduce the new peer to all our existing peers (and pre-authorize existing peers for the new peer).
        if !existing_peers.is_empty() && !is_introduced_link && !peer_ticket.is_empty() {
            let my_nick = self.nickname.read().await.clone();
            let cur_room_id = self.room_id.read().await.clone();
            let cur_room_name = self.room_name.read().await.clone();
            let cur_room_pass_hash = self.room_passphrase_hash.read().await.clone();

            for existing in existing_peers {
                // 1. Pre-authorize `existing` on the new peer so when `existing` approves and dials `new_peer`,
                // `new_peer` knows `existing` is part of the same group.
                let _ = tx
                    .send(WireMessage::PreAuthorizePeer {
                        node_id: existing.info.node_id.clone(),
                    })
                    .await;

                // 2. Send `PeerIntroduction` to `existing` so `existing` gets an approval prompt to connect to `new_peer`.
                let _ = existing
                    .tx
                    .send(WireMessage::PeerIntroduction {
                        peer: peer_info.clone(),
                        ticket: peer_ticket.clone(),
                        introduced_by: my_nick.clone(),
                        introduced_by_node_id: self.node_id_hex.clone(),
                        room_id: cur_room_id.clone(),
                        room_name: cur_room_name.clone(),
                        room_passphrase_hash: cur_room_pass_hash.clone(),
                    })
                    .await;
            }
        }

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
        let reader_tx = tx.clone();
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
                        let is_dup = reader_node
                            .messages
                            .read()
                            .await
                            .iter()
                            .any(|m| m.id == id);
                        if !is_dup {
                            let (channel_kind, channel_id, channel_name) =
                                reader_node.channel_meta_for_peer(&peer_clone.node_id).await;
                            let payload = ChatMessagePayload {
                                id,
                                sender_id,
                                sender_name,
                                content,
                                timestamp,
                                is_self: false,
                                channel_kind,
                                channel_id,
                                channel_name,
                            };
                            reader_node.messages.write().await.push(payload.clone());
                            reader_node.save_config().await;
                            (reader_node.event_cb)(P2PEvent::ChatMessage { message: payload });
                        }
                    }
                    WireMessage::PreAuthorizePeer { node_id } => {
                        reader_node
                            .pre_authorized_peers
                            .lock()
                            .await
                            .insert(node_id.to_lowercase());
                    }
                    WireMessage::PeerIntroduction {
                        peer,
                        ticket,
                        introduced_by,
                        introduced_by_node_id,
                        room_id,
                        room_name,
                        room_passphrase_hash,
                    } => {
                        if reader_node.room_passphrase_hash.read().await.is_none()
                            && room_passphrase_hash.is_some()
                        {
                            *reader_node.room_passphrase_hash.write().await = room_passphrase_hash;
                        }
                        let already_connected =
                            reader_node.peers.read().await.contains_key(&peer.node_id);
                        if !already_connected && peer.node_id != reader_node.node_id_hex {
                            let mut pending = reader_node.pending_approvals.lock().await;
                            let already_pending =
                                pending.iter().any(|p| p.info.peer.node_id == peer.node_id);
                            if !already_pending {
                                pending.push(PendingApprovalEntry {
                                    info: PendingRequestInfo {
                                        peer: peer.clone(),
                                        introduced_by: Some(introduced_by.clone()),
                                        room_name: room_name.clone(),
                                        matched_tag: None,
                                    },
                                    action: PendingAction::IntroducedDial {
                                        ticket,
                                        introduced_by_node_id,
                                        room_id,
                                        room_name: room_name.clone(),
                                    },
                                });
                                drop(pending);
                                (reader_node.event_cb)(P2PEvent::ConnectionRequested {
                                    peer,
                                    introduced_by: Some(introduced_by),
                                    room_name,
                                    matched_tag: None,
                                });
                                if let Ok(snap) = reader_node.snapshot().await {
                                    (reader_node.event_cb)(P2PEvent::StateUpdated {
                                        snapshot: snap,
                                    });
                                }
                            }
                        }
                    }
                    WireMessage::RoomUpdate {
                        room_id,
                        room_name,
                        room_passphrase_hash,
                    } => {
                        *reader_node.room_id.write().await = room_id;
                        *reader_node.room_name.write().await = room_name;
                        *reader_node.room_passphrase_hash.write().await = room_passphrase_hash;
                        if let Ok(snap) = reader_node.snapshot().await {
                            (reader_node.event_cb)(P2PEvent::StateUpdated { snapshot: snap });
                        }
                    }
                    WireMessage::Disconnect => {
                        break;
                    }
                    _ => {}
                }
            }

            // Clean up when stream closes (only if this stream is still the active session for this peer)
            let removed = {
                let mut peers_guard = reader_node.peers.write().await;
                let is_same = peers_guard
                    .get(&peer_clone.node_id)
                    .map(|s| s.tx.same_channel(&reader_tx))
                    .unwrap_or(false);
                if is_same {
                    peers_guard.remove(&peer_clone.node_id)
                } else {
                    None
                }
            };
            if removed.is_some() {
                (reader_node.event_cb)(P2PEvent::PeerDisconnected {
                    peer: Some(peer_clone),
                    reason: None,
                });
                if let Ok(snap) = reader_node.snapshot().await {
                    (reader_node.event_cb)(P2PEvent::StateUpdated { snapshot: snap });
                }
            }
        });

        Ok(peer_info)
    }

    pub async fn send_chat_message(&self, content: String) -> Result<ChatMessagePayload> {
        let sessions: Vec<PeerSession> = self.peers.read().await.values().cloned().collect();
        if sessions.is_empty() {
            return Err(anyhow!("現在接続中のピアがいません"));
        }

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

        let mut sent_count = 0;
        for s in sessions {
            if s.tx.send(wire.clone()).await.is_ok() {
                sent_count += 1;
            }
        }
        if sent_count == 0 {
            return Err(anyhow!("ピアへの送信チャネルが閉じています"));
        }

        let (channel_kind, channel_id, channel_name) = self.current_channel_meta().await;
        let payload = ChatMessagePayload {
            id,
            sender_id: self.node_id_hex.clone(),
            sender_name,
            content,
            timestamp,
            is_self: true,
            channel_kind,
            channel_id,
            channel_name,
        };

        self.messages.write().await.push(payload.clone());
        self.save_config().await;
        (self.event_cb)(P2PEvent::ChatMessage {
            message: payload.clone(),
        });

        Ok(payload)
    }

    pub async fn clear_messages(&self, channel_id: Option<String>) -> Result<StateSnapshot> {
        {
            let mut msgs = self.messages.write().await;
            match channel_id.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
                Some(cid) => msgs.retain(|m| m.channel_id != cid),
                None => msgs.clear(),
            }
        }
        self.save_config().await;
        let snap = self.snapshot().await?;
        (self.event_cb)(P2PEvent::StateUpdated {
            snapshot: snap.clone(),
        });
        Ok(snap)
    }

    pub async fn respond_connection_request(
        self: &Arc<Self>,
        node_id: Option<String>,
        accept: bool,
    ) -> Result<StateSnapshot> {
        let entry_opt = {
            let mut pending = self.pending_approvals.lock().await;
            if pending.is_empty() {
                None
            } else if let Some(ref target_id) = node_id {
                if let Some(idx) = pending
                    .iter()
                    .position(|p| &p.info.peer.node_id == target_id)
                {
                    Some(pending.remove(idx))
                } else {
                    None
                }
            } else {
                Some(pending.remove(0))
            }
        };

        if let Some(entry) = entry_opt {
            if accept {
                if let Some(ref tag) = entry.info.matched_tag {
                    self.pending_match_tags
                        .lock()
                        .await
                        .insert(entry.info.peer.node_id.to_lowercase(), tag.clone());
                }
            }
            match entry.action {
                PendingAction::IncomingStream { responder } => {
                    let _ = responder.send(accept);
                    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
                }
                PendingAction::IntroducedDial {
                    ticket,
                    introduced_by_node_id,
                    room_id,
                    room_name,
                } => {
                    if accept {
                        if self.room_id.read().await.is_none() && room_id.is_some() {
                            *self.room_id.write().await = room_id;
                            *self.room_name.write().await = room_name;
                        }
                        let target_node_id = entry.info.peer.node_id.clone();
                        let _ = self
                            .connect_by_ticket_internal(
                                &ticket,
                                Some(&target_node_id),
                                Some(introduced_by_node_id),
                            )
                            .await?;
                    }
                }
                PendingAction::LanMatched { ticket } => {
                    let target_node_id = entry.info.peer.node_id.clone();
                    if accept {
                        self.pre_authorized_peers
                            .lock()
                            .await
                            .insert(target_node_id.to_lowercase());
                        let dial_node = Arc::clone(self);
                        tokio::spawn(async move {
                            let _ = dial_node
                                .connect_by_ticket_internal(&ticket, Some(&target_node_id), None)
                                .await;
                        });
                    } else {
                        self.dismissed_match_peers
                            .lock()
                            .await
                            .insert(target_node_id);
                    }
                }
            }
        }

        let snap = self.snapshot().await?;
        (self.event_cb)(P2PEvent::StateUpdated {
            snapshot: snap.clone(),
        });
        Ok(snap)
    }

    pub async fn disconnect(&self) -> Result<()> {
        self.disconnect_peer(None).await
    }

    pub async fn disconnect_peer(&self, target_node_id: Option<String>) -> Result<()> {
        if let Some(node_id) = target_node_id {
            if let Some(session) = self.peers.write().await.remove(&node_id) {
                let _ = session.tx.send(WireMessage::Disconnect).await;
                (self.event_cb)(P2PEvent::PeerDisconnected {
                    peer: Some(session.info),
                    reason: Some("Peer disconnected".into()),
                });
            }
        } else {
            let drained: Vec<PeerSession> =
                self.peers.write().await.drain().map(|(_, v)| v).collect();
            for s in drained {
                let _ = s.tx.send(WireMessage::Disconnect).await;
            }
            if let Some(r_id) = self.room_id.write().await.take() {
                self.leave_room_on_server(&r_id).await;
            }
            *self.room_name.write().await = None;
            *self.room_passphrase_hash.write().await = None;
            (self.event_cb)(P2PEvent::PeerDisconnected {
                peer: None,
                reason: Some("User disconnected".into()),
            });
        }
        if let Ok(snap) = self.snapshot().await {
            (self.event_cb)(P2PEvent::StateUpdated { snapshot: snap });
        }
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
            channel_kind: "direct".to_string(),
            channel_id: "direct:bob".to_string(),
            channel_name: "Bob".to_string(),
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
        let (tx_a, mut rx_a) = broadcast::channel::<P2PEvent>(32);
        let (tx_b, mut rx_b) = broadcast::channel::<P2PEvent>(32);

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

        // 1. Test REJECT flow first: Bob tries to connect, Alice rejects
        let node_b_reject = Arc::clone(&node_b);
        let ticket_a_reject = ticket_a.clone();
        let expected_a_id = node_a.node_id_hex.clone();
        let connect_reject_handle = tokio::spawn(async move {
            node_b_reject
                .connect_by_ticket_verified(&ticket_a_reject, Some(&expected_a_id))
                .await
        });

        for _ in 0..10 {
            if let Ok(P2PEvent::ConnectionRequested { peer, .. }) =
                tokio::time::timeout(std::time::Duration::from_secs(2), rx_a.recv()).await?
            {
                assert_eq!(peer.nickname, "Bob");
                node_a
                    .respond_connection_request(Some(peer.node_id), false)
                    .await?;
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

        for _ in 0..10 {
            if let Ok(P2PEvent::ConnectionRequested { peer, .. }) =
                tokio::time::timeout(std::time::Duration::from_secs(2), rx_a.recv()).await?
            {
                assert_eq!(peer.nickname, "Bob");
                node_a
                    .respond_connection_request(Some(peer.node_id), true)
                    .await?;
                break;
            }
        }

        let peer_a_info = connect_accept_handle.await??;
        assert_eq!(peer_a_info.nickname, "Alice");

        node_b
            .send_chat_message("Hello Alice from P2P!".to_string())
            .await?;

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

        let _ = rx_b.recv().await;
        node_b.disconnect().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_p2p_three_nodes_group_mesh_and_chat() -> Result<()> {
        let (tx_a, mut rx_a) = broadcast::channel::<P2PEvent>(64);
        let (tx_b, mut rx_b) = broadcast::channel::<P2PEvent>(64);
        let (tx_c, mut rx_c) = broadcast::channel::<P2PEvent>(64);

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

        let node_c = ChatNode::new(Arc::new(move |ev| {
            let _ = tx_c.send(ev);
        }))
        .await?;
        *node_c.nickname.write().await = "Charlie".to_string();

        let ticket_a = node_a.current_ticket().await?;

        // Step 1: Bob connects to Alice (1-on-1)
        let b_clone = Arc::clone(&node_b);
        let t_a = ticket_a.clone();
        let b_join = tokio::spawn(async move { b_clone.connect_by_ticket_verified(&t_a, None).await });

        for _ in 0..15 {
            if let Ok(P2PEvent::ConnectionRequested { peer, .. }) =
                tokio::time::timeout(std::time::Duration::from_secs(2), rx_a.recv()).await?
            {
                node_a
                    .respond_connection_request(Some(peer.node_id), true)
                    .await?;
                break;
            }
        }
        b_join.await??;

        // Step 2: Charlie connects to Alice -> Alice approves -> Alice introduces Charlie to Bob
        let c_clone = Arc::clone(&node_c);
        let t_a2 = ticket_a.clone();
        let c_join = tokio::spawn(async move { c_clone.connect_by_ticket_verified(&t_a2, None).await });

        for _ in 0..15 {
            if let Ok(P2PEvent::ConnectionRequested { peer, .. }) =
                tokio::time::timeout(std::time::Duration::from_secs(2), rx_a.recv()).await?
            {
                if peer.nickname == "Charlie" {
                    node_a
                        .respond_connection_request(Some(peer.node_id), true)
                        .await?;
                    break;
                }
            }
        }
        c_join.await??;

        // Step 3: Bob should receive a ConnectionRequested (introduced by Alice) for Charlie!
        let mut bob_introduced_charlie = false;
        for _ in 0..20 {
            if let Ok(P2PEvent::ConnectionRequested {
                peer,
                introduced_by,
                ..
            }) = tokio::time::timeout(std::time::Duration::from_secs(3), rx_b.recv()).await?
            {
                if peer.nickname == "Charlie" {
                    assert_eq!(introduced_by.as_deref(), Some("Alice"));
                    node_b
                        .respond_connection_request(Some(peer.node_id), true)
                        .await?;
                    bob_introduced_charlie = true;
                    break;
                }
            }
        }
        assert!(
            bob_introduced_charlie,
            "Bob must receive peer introduction for Charlie from Alice"
        );

        // Wait briefly for Bob <-> Charlie stream to settle
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Verify all 3 nodes have 2 connected peers each (Full Mesh!)
        assert_eq!(node_a.snapshot().await?.connected_peers.len(), 2);
        assert_eq!(node_b.snapshot().await?.connected_peers.len(), 2);
        assert_eq!(node_c.snapshot().await?.connected_peers.len(), 2);

        // Step 4: Charlie sends a group message -> both Alice and Bob receive it
        node_c
            .send_chat_message("Hello Group from Charlie!".to_string())
            .await?;

        let mut alice_got = false;
        for _ in 0..20 {
            if let Ok(P2PEvent::ChatMessage { message }) =
                tokio::time::timeout(std::time::Duration::from_secs(2), rx_a.recv()).await?
            {
                if message.content == "Hello Group from Charlie!" {
                    alice_got = true;
                    break;
                }
            }
        }
        assert!(alice_got, "Alice should receive Charlie's group message");

        let mut bob_got = false;
        for _ in 0..20 {
            if let Ok(P2PEvent::ChatMessage { message }) =
                tokio::time::timeout(std::time::Duration::from_secs(2), rx_b.recv()).await?
            {
                if message.content == "Hello Group from Charlie!" {
                    bob_got = true;
                    break;
                }
            }
        }
        assert!(bob_got, "Bob should receive Charlie's group message");

        // Step 5: Alice leaves the group -> Bob and Charlie remain connected and can still chat!
        node_a.disconnect().await?;
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        assert_eq!(node_b.snapshot().await?.connected_peers.len(), 1);
        assert_eq!(node_c.snapshot().await?.connected_peers.len(), 1);

        node_b
            .send_chat_message("Alice left, still here Charlie!".to_string())
            .await?;

        let mut charlie_got_after_alice_left = false;
        for _ in 0..20 {
            if let Ok(P2PEvent::ChatMessage { message }) =
                tokio::time::timeout(std::time::Duration::from_secs(2), rx_c.recv()).await?
            {
                if message.content == "Alice left, still here Charlie!" {
                    charlie_got_after_alice_left = true;
                    break;
                }
            }
        }
        assert!(
            charlie_got_after_alice_left,
            "Charlie should still receive Bob's message after Alice disconnects"
        );

        node_b.disconnect().await?;
        node_c.disconnect().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_lan_matching_discovery_and_approval() -> Result<()> {
        let (tx_a, mut rx_a) = broadcast::channel::<P2PEvent>(64);
        let (tx_b, mut rx_b) = broadcast::channel::<P2PEvent>(64);

        let node_a = ChatNode::new(Arc::new(move |ev| {
            let _ = tx_a.send(ev);
        }))
        .await?;
        *node_a.nickname.write().await = "AliceMatch".to_string();

        let node_b = ChatNode::new(Arc::new(move |ev| {
            let _ = tx_b.send(ev);
        }))
        .await?;
        *node_b.nickname.write().await = "BobMatch".to_string();

        // Turn LAN matching ON for both with matching tag "Rust"
        node_a.set_lan_matching(true, "Rust".to_string()).await?;
        node_b.set_lan_matching(true, "rust".to_string()).await?;

        // Wait for Alice to receive a LAN match ConnectionRequested event for Bob
        let mut alice_matched_bob = false;
        for _ in 0..20 {
            if let Ok(P2PEvent::ConnectionRequested {
                peer, matched_tag, ..
            }) = tokio::time::timeout(std::time::Duration::from_secs(3), rx_a.recv()).await?
            {
                if peer.nickname == "BobMatch" {
                    assert!(matched_tag.is_some());
                    alice_matched_bob = true;
                    break;
                }
            }
        }
        assert!(
            alice_matched_bob,
            "Alice should automatically discover Bob via LAN matching beacon"
        );

        // Alice approves the match (which dials Bob)
        node_a
            .respond_connection_request(Some(node_b.node_id_hex.clone()), true)
            .await?;

        // Wait for Bob to have the pending request (from beacon or incoming stream), then approve
        for _ in 0..25 {
            if !node_b.snapshot().await?.pending_requests.is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        node_b
            .respond_connection_request(Some(node_a.node_id_hex.clone()), true)
            .await?;

        // Wait for connection to establish
        let mut connected = false;
        for _ in 0..30 {
            if node_a.snapshot().await?.connected_peers.len() == 1
                && node_b.snapshot().await?.connected_peers.len() == 1
            {
                connected = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        assert!(
            connected,
            "Alice and Bob should establish P2P session after approving LAN match"
        );

        let _ = rx_b.recv().await;
        node_a.disconnect().await?;
        node_b.disconnect().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_room_passphrase_verification() -> Result<()> {
        let node_host = ChatNode::new(Arc::new(|_| {})).await?;
        *node_host.nickname.write().await = "HostAlice".to_string();

        let snap = node_host
            .create_group_room("秘密基地ルーム".to_string(), Some("opensesame".to_string()))
            .await?;
        assert!(snap.room_has_passphrase);
        let room_id = snap.room_id.unwrap();
        let host_ticket = node_host.current_ticket().await?;

        // 1. Wrong passphrase should be rejected automatically during handshake
        let node_intruder = ChatNode::new(Arc::new(|_| {})).await?;
        *node_intruder.nickname.write().await = "Intruder".to_string();
        *node_intruder.room_id.write().await = Some(room_id.clone());
        *node_intruder.room_name.write().await = Some("秘密基地ルーム".to_string());
        *node_intruder.room_passphrase_hash.write().await =
            hash_room_passphrase(&room_id, "wrongpass");

        let wrong_res = node_intruder
            .connect_by_ticket_verified(&host_ticket, Some(&node_host.node_id_hex))
            .await;
        assert!(
            wrong_res.is_err(),
            "Wrong room passphrase must be rejected by host"
        );

        // 2. Correct passphrase should be accepted automatically
        let node_friend = ChatNode::new(Arc::new(|_| {})).await?;
        *node_friend.nickname.write().await = "FriendBob".to_string();
        *node_friend.room_id.write().await = Some(room_id.clone());
        *node_friend.room_name.write().await = Some("秘密基地ルーム".to_string());
        *node_friend.room_passphrase_hash.write().await =
            hash_room_passphrase(&room_id, "opensesame");

        let ok_peer = node_friend
            .connect_by_ticket_verified(&host_ticket, Some(&node_host.node_id_hex))
            .await?;
        assert_eq!(ok_peer.nickname, "HostAlice");

        node_host.disconnect().await?;
        node_friend.disconnect().await?;
        Ok(())
    }
}
