import React, { useEffect, useRef, useState } from "react";
import { invoke } from "@tauri-apps/api/core";
import { listen } from "@tauri-apps/api/event";
import {
  ChatMessagePayload,
  DirectoryUser,
  P2PEvent,
  PeerInfo,
  StateSnapshot,
} from "../types";

export const MainView: React.FC = () => {
  const [nodeId, setNodeId] = useState<string>("");
  const [ticket, setTicket] = useState<string>("");
  const [nickname, setNickname] = useState<string>("User");
  const [username, setUsername] = useState<string>("");
  const [serverUrl, setServerUrl] = useState<string>("http://127.0.0.1:3100");
  const [registeredOnServer, setRegisteredOnServer] = useState<boolean>(false);
  const [connectedPeer, setConnectedPeer] = useState<PeerInfo | null>(null);
  const [pendingRequest, setPendingRequest] = useState<PeerInfo | null>(null);
  const [messages, setMessages] = useState<ChatMessagePayload[]>([]);

  const [activeTab, setActiveTab] = useState<"server" | "manual">("server");
  const [onlineUsers, setOnlineUsers] = useState<DirectoryUser[]>([]);
  const [remoteTicketInput, setRemoteTicketInput] = useState<string>("");
  const [chatInput, setChatInput] = useState<string>("");
  const [statusMsg, setStatusMsg] = useState<string>("");
  const [errorMsg, setErrorMsg] = useState<string>("");
  const [isBusy, setIsBusy] = useState<boolean>(false);

  const messagesEndRef = useRef<HTMLDivElement | null>(null);

  const applySnapshot = (snap: StateSnapshot) => {
    setNodeId(snap.nodeId);
    setTicket(snap.ticket);
    setNickname(snap.nickname);
    if (snap.username) setUsername(snap.username);
    setServerUrl(snap.serverUrl);
    setRegisteredOnServer(snap.registeredOnServer);
    setConnectedPeer(snap.connectedPeer);
    setPendingRequest(snap.pendingRequest);
    setMessages(snap.messages);
  };

  const refreshOnlineUsers = async () => {
    try {
      const list = await invoke<DirectoryUser[]>("fetch_online_users");
      setOnlineUsers(list);
    } catch {
      // Directory server might not be running yet; ignore silently during polling
    }
  };

  useEffect(() => {
    document.documentElement.classList.remove("overlay-mode");
    document.body.classList.remove("overlay-mode");

    invoke<StateSnapshot>("get_state_snapshot")
      .then(applySnapshot)
      .catch((err) => setErrorMsg(String(err)));

    refreshOnlineUsers();
    const timer = window.setInterval(refreshOnlineUsers, 5000);

    const unlistenPromise = listen<P2PEvent>("p2p-event", (event) => {
      const payload = event.payload;
      if (payload.type === "connection-requested") {
        setPendingRequest(payload.peer);
        setStatusMsg(`📩 ${payload.peer.nickname} から接続リクエストが届いています`);
        setErrorMsg("");
      } else if (payload.type === "peer-connected") {
        setPendingRequest(null);
        setConnectedPeer(payload.peer);
        setStatusMsg(`✅ ${payload.peer.nickname} とP2P暗号化接続を確立しました`);
        setErrorMsg("");
      } else if (payload.type === "peer-disconnected") {
        setConnectedPeer(null);
        setStatusMsg("🔌 通信が切断されました");
      } else if (payload.type === "chat-message") {
        setMessages((prev) => {
          if (prev.some((m) => m.id === payload.message.id)) return prev;
          return [...prev, payload.message];
        });
      } else if (payload.type === "state-updated") {
        applySnapshot(payload.snapshot);
      }
    });

    return () => {
      window.clearInterval(timer);
      unlistenPromise.then((unlisten) => unlisten());
    };
  }, []);

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

  const handleRegisterServer = async (e: React.FormEvent) => {
    e.preventDefault();
    setErrorMsg("");
    setStatusMsg("");
    if (!username.trim()) {
      setErrorMsg("ユーザID (英数字 2〜24文字) を入力してください");
      return;
    }
    setIsBusy(true);
    try {
      const snap = await invoke<StateSnapshot>("register_user", {
        serverUrl: serverUrl.trim(),
        username: username.trim(),
        nickname: nickname.trim() || username.trim(),
      });
      applySnapshot(snap);
      setStatusMsg(`🔒 @${snap.username} として公開鍵と署名をサーバに登録しました`);
      await refreshOnlineUsers();
    } catch (err) {
      setErrorMsg(String(err));
    } finally {
      setIsBusy(false);
    }
  };

  const handleConnectByUsername = async (targetUser: string) => {
    setErrorMsg("");
    setStatusMsg(`⏳ @${targetUser} に接続リクエストを送信中 (相手の承認待ち)...`);
    setIsBusy(true);
    try {
      const peer = await invoke<PeerInfo>("connect_user", {
        username: targetUser,
      });
      setConnectedPeer(peer);
      setStatusMsg(`✅ @${targetUser} (${peer.nickname}) と接続しました`);
    } catch (err) {
      setErrorMsg(String(err));
      setStatusMsg("");
    } finally {
      setIsBusy(false);
    }
  };

  const handleConnectByTicket = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!remoteTicketInput.trim()) return;
    setErrorMsg("");
    setStatusMsg("⏳ チケットから接続リクエストを送信中 (相手の承認待ち)...");
    setIsBusy(true);
    try {
      await invoke("set_nickname", { nickname: nickname.trim() || "User" });
      const peer = await invoke<PeerInfo>("connect_peer", {
        ticket: remoteTicketInput.trim(),
      });
      setConnectedPeer(peer);
      setRemoteTicketInput("");
      setStatusMsg(`✅ ${peer.nickname} と接続しました`);
    } catch (err) {
      setErrorMsg(String(err));
      setStatusMsg("");
    } finally {
      setIsBusy(false);
    }
  };

  const handleRespondRequest = async (accept: boolean) => {
    setErrorMsg("");
    try {
      setPendingRequest(null);
      const snap = await invoke<StateSnapshot>("respond_connection_request", {
        accept,
      });
      applySnapshot(snap);
      if (!accept) {
        setStatusMsg("🛡️ 接続リクエストを拒否しました");
      }
    } catch (err) {
      setErrorMsg(String(err));
    }
  };

  const handleDisconnect = async () => {
    try {
      await invoke("disconnect_peer");
      setConnectedPeer(null);
      setStatusMsg("切断しました");
    } catch (err) {
      setErrorMsg(String(err));
    }
  };

  const handleSendChat = async (e: React.FormEvent) => {
    e.preventDefault();
    const content = chatInput.trim();
    if (!content || !connectedPeer) return;
    try {
      setChatInput("");
      await invoke("send_message", { content });
    } catch (err) {
      setErrorMsg(String(err));
    }
  };

  const handleCopyTicket = async () => {
    try {
      await navigator.clipboard.writeText(ticket);
      setStatusMsg("📋 接続チケットをクリップボードにコピーしました");
    } catch {
      setErrorMsg("クリップボードへのコピーに失敗しました");
    }
  };

  const handleExportHistory = async () => {
    if (messages.length === 0) return;
    setErrorMsg("");
    try {
      const lines = messages.map((m) => {
        const dt = new Date(m.timestamp).toLocaleString();
        const who = m.isSelf ? `${nickname} (自分)` : m.senderName;
        return `[${dt}] ${who}: ${m.content}`;
      });
      const savedPath = await invoke<string>("export_chat_history", {
        formattedText: lines.join("\n") + "\n",
      });
      setStatusMsg(`💾 チャット履歴を保存しました: ${savedPath}`);
    } catch (err) {
      setErrorMsg(String(err));
    }
  };

  const handleClearHistory = async () => {
    if (messages.length === 0) return;
    setErrorMsg("");
    try {
      const snap = await invoke<StateSnapshot>("clear_chat_history");
      applySnapshot(snap);
      setStatusMsg("🗑️ チャット履歴を消去しました");
    } catch (err) {
      setErrorMsg(String(err));
    }
  };

  const formatTime = (ts: number) => {
    const d = new Date(ts);
    return d.toLocaleTimeString([], {
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
    });
  };

  return (
    <div className="main-window-layout">
      {/* Left Sidebar: P2P Connection & Server Directory */}
      <aside className="sidebar-panel">
        <div className="brand-header">
          <h2>SimpleChat P2P</h2>
          <span className="crypto-badge" title={`Ed25519 NodeID: ${nodeId}`}>
            🔐 QUIC + Ed25519 E2EE
          </span>
        </div>

        {/* Active Peer Card */}
        <div className={`connection-card ${connectedPeer ? "active" : ""}`}>
          <div className="connection-info">
            <span className={`dot ${connectedPeer ? "online" : "offline"}`} />
            <div>
              <div className="conn-title">
                {connectedPeer
                  ? `接続中: ${connectedPeer.nickname}`
                  : "未接続 (待機中)"}
              </div>
              {connectedPeer && (
                <div className="conn-sub">
                  NodeID: {connectedPeer.nodeId.slice(0, 14)}...
                </div>
              )}
            </div>
          </div>
          {connectedPeer && (
            <button
              type="button"
              className="btn-disconnect"
              onClick={handleDisconnect}
            >
              切断
            </button>
          )}
        </div>

        {/* Incoming Connection Approval Card */}
        {pendingRequest && (
          <div className="incoming-request-card">
            <div className="incoming-request-header">
              📩 着信P2P接続リクエスト
            </div>
            <div className="incoming-request-peer">
              <strong>{pendingRequest.nickname}</strong>
              {pendingRequest.username ? ` (@${pendingRequest.username})` : ""}
              <div className="incoming-request-node">
                NodeID: {pendingRequest.nodeId.slice(0, 16)}...
              </div>
            </div>
            <div className="incoming-request-actions">
              <button
                type="button"
                className="btn-approve"
                onClick={() => handleRespondRequest(true)}
              >
                承認して接続
              </button>
              <button
                type="button"
                className="btn-reject"
                onClick={() => handleRespondRequest(false)}
              >
                拒否
              </button>
            </div>
          </div>
        )}

        {/* Mode Tabs */}
        <div className="tab-switcher">
          <button
            type="button"
            className={activeTab === "server" ? "active" : ""}
            onClick={() => setActiveTab("server")}
          >
            🌐 管理サーバ連携
          </button>
          <button
            type="button"
            className={activeTab === "manual" ? "active" : ""}
            onClick={() => setActiveTab("manual")}
          >
            🎫 手動チケット
          </button>
        </div>

        {activeTab === "server" ? (
          <div className="tab-content">
            <form className="config-form" onSubmit={handleRegisterServer}>
              <label>
                管理サーバ URL
                <input
                  type="text"
                  value={serverUrl}
                  onChange={(e) => setServerUrl(e.target.value)}
                  placeholder="http://127.0.0.1:3100"
                />
              </label>
              <div className="row-2col">
                <label>
                  ユーザID (@username)
                  <input
                    type="text"
                    value={username}
                    onChange={(e) => setUsername(e.target.value)}
                    placeholder="alice"
                  />
                </label>
                <label>
                  表示名 (ニックネーム)
                  <input
                    type="text"
                    value={nickname}
                    onChange={(e) => setNickname(e.target.value)}
                    placeholder="Alice"
                  />
                </label>
              </div>
              <button type="submit" className="btn-primary" disabled={isBusy}>
                {registeredOnServer
                  ? "🔄 登録情報を更新 (Ed25519署名送信)"
                  : "📝 サーバへ公開鍵・ユーザ登録"}
              </button>
            </form>

            <div className="directory-section">
              <div className="section-header">
                <span>オンラインユーザ一覧</span>
                <button
                  type="button"
                  className="btn-link"
                  onClick={refreshOnlineUsers}
                >
                  更新 ↻
                </button>
              </div>
              <div className="user-list">
                {onlineUsers.filter((u) => u.nodeId !== nodeId).length === 0 ? (
                  <div className="empty-hint">
                    他の登録ユーザがまだいません。別ウィンドウや相手のPCでユーザ登録するとここに表示されます。
                  </div>
                ) : (
                  onlineUsers
                    .filter((u) => u.nodeId !== nodeId)
                    .map((u) => (
                      <div key={u.username} className="user-item">
                        <div className="user-meta">
                          <span
                            className={`dot ${u.online ? "online" : "offline"}`}
                          />
                          <div>
                            <div className="user-name">
                              {u.displayName}{" "}
                              <span className="user-handle">@{u.username}</span>
                            </div>
                            <div className="user-key">
                              🔑 {u.nodeId.slice(0, 12)}...
                            </div>
                          </div>
                        </div>
                        <button
                          type="button"
                          className="btn-connect-sm"
                          disabled={
                            isBusy || connectedPeer?.nodeId === u.nodeId
                          }
                          onClick={() => handleConnectByUsername(u.username)}
                        >
                          {connectedPeer?.nodeId === u.nodeId
                            ? "接続済"
                            : "P2P接続"}
                        </button>
                      </div>
                    ))
                )}
              </div>
            </div>
          </div>
        ) : (
          <div className="tab-content">
            <div className="config-form">
              <label>
                表示名 (ニックネーム)
                <input
                  type="text"
                  value={nickname}
                  onChange={(e) => setNickname(e.target.value)}
                  onBlur={() =>
                    invoke("set_nickname", {
                      nickname: nickname.trim() || "User",
                    })
                  }
                />
              </label>

              <label>
                あなたの接続チケット (相手に共有)
                <div className="ticket-box">
                  <input type="text" readOnly value={ticket} />
                  <button
                    type="button"
                    className="btn-secondary"
                    onClick={handleCopyTicket}
                  >
                    コピー
                  </button>
                </div>
              </label>

              <form onSubmit={handleConnectByTicket}>
                <label>
                  相手の接続チケットを貼り付けて接続
                  <textarea
                    rows={3}
                    value={remoteTicketInput}
                    onChange={(e) => setRemoteTicketInput(e.target.value)}
                    placeholder="相手から受け取ったチケット文字列を入力..."
                  />
                </label>
                <button
                  type="submit"
                  className="btn-primary"
                  disabled={isBusy || !remoteTicketInput.trim()}
                >
                  🔗 チケットでP2P接続
                </button>
              </form>
            </div>
          </div>
        )}

        {statusMsg && <div className="alert-info">{statusMsg}</div>}
        {errorMsg && <div className="alert-error">{errorMsg}</div>}
      </aside>

      {/* Right Panel: Persistent Chat History */}
      <main className="chat-history-panel">
        <header className="chat-header">
          <div className="chat-header-info">
            <div className="chat-title-row">
              <h3>チャット履歴</h3>
              <span className="history-count-badge">{messages.length} 件</span>
              <span className="self-node-tag" title={`My NodeID: ${nodeId}`}>
                ID: {nodeId ? `${nodeId.slice(0, 8)}...` : "..."}
              </span>
            </div>
            <p className="chat-subtitle">
              メッセージは自動保存され、次回起動時にも引き継がれます
            </p>
          </div>
          <div className="chat-header-actions">
            <button
              type="button"
              className="btn-header-action"
              disabled={messages.length === 0}
              onClick={handleExportHistory}
              title="テキストファイルとしてダウンロードフォルダに保存"
            >
              💾 ログ出力
            </button>
            <button
              type="button"
              className="btn-header-action danger-outline"
              disabled={messages.length === 0}
              onClick={handleClearHistory}
              title="保存されているチャット履歴を消去"
            >
              🗑️ 履歴消去
            </button>
          </div>
        </header>

        <div className="messages-timeline">
          {messages.length === 0 ? (
            <div className="empty-chat">
              <p>まだチャット履歴がありません。</p>
              <p className="sub">
                左パネルから相手と接続し、メッセージを送受信するとここに記録されます。
              </p>
            </div>
          ) : (
            messages.map((m) => (
              <div
                key={m.id}
                className={`history-msg ${m.isSelf ? "self" : "peer"}`}
              >
                <div className="msg-meta">
                  <span className="msg-author">
                    {m.isSelf ? `${nickname} (あなた)` : m.senderName}
                  </span>
                  <span className="msg-time">{formatTime(m.timestamp)}</span>
                </div>
                <div className="msg-bubble">{m.content}</div>
              </div>
            ))
          )}
          <div ref={messagesEndRef} />
        </div>

        <form className="chat-composer" onSubmit={handleSendChat}>
          <input
            type="text"
            value={chatInput}
            onChange={(e) => setChatInput(e.target.value)}
            placeholder={
              connectedPeer
                ? `${connectedPeer.nickname} にメッセージを送信 (Enterで送信)...`
                : "左パネルで相手とP2P接続すると送信できます"
            }
            disabled={!connectedPeer}
          />
          <button
            type="submit"
            className="btn-send"
            disabled={!connectedPeer || !chatInput.trim()}
          >
            送信
          </button>
        </form>
      </main>
    </div>
  );
};
