import React, { useEffect, useRef, useState } from "react";
import { invoke } from "@tauri-apps/api/core";
import { listen } from "@tauri-apps/api/event";
import {
  ChatMessagePayload,
  DirectoryUser,
  GroupRoom,
  P2PEvent,
  PeerInfo,
  PendingRequestInfo,
  StateSnapshot,
} from "../types";

export const MainView: React.FC = () => {
  const [nodeId, setNodeId] = useState<string>("");
  const [ticket, setTicket] = useState<string>("");
  const [nickname, setNickname] = useState<string>("User");
  const [username, setUsername] = useState<string>("");
  const [serverUrl, setServerUrl] = useState<string>("http://127.0.0.1:3100");
  const [registeredOnServer, setRegisteredOnServer] = useState<boolean>(false);
  const [roomId, setRoomId] = useState<string | null>(null);
  const [roomName, setRoomName] = useState<string | null>(null);
  const [connectedPeers, setConnectedPeers] = useState<PeerInfo[]>([]);
  const [pendingRequests, setPendingRequests] = useState<PendingRequestInfo[]>(
    []
  );
  const [messages, setMessages] = useState<ChatMessagePayload[]>([]);

  const [activeTab, setActiveTab] = useState<"server" | "rooms" | "manual">(
    "server"
  );
  const [onlineUsers, setOnlineUsers] = useState<DirectoryUser[]>([]);
  const [groupRooms, setGroupRooms] = useState<GroupRoom[]>([]);
  const [newRoomNameInput, setNewRoomNameInput] = useState<string>("");
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
    setRoomId(snap.roomId || null);
    setRoomName(snap.roomName || null);
    setConnectedPeers(snap.connectedPeers || []);
    setPendingRequests(snap.pendingRequests || []);
    setMessages(snap.messages || []);
  };

  const refreshDirectory = async () => {
    try {
      const [users, rooms] = await Promise.all([
        invoke<DirectoryUser[]>("fetch_online_users"),
        invoke<GroupRoom[]>("fetch_group_rooms"),
      ]);
      setOnlineUsers(users);
      setGroupRooms(rooms);
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

    refreshDirectory();
    const timer = window.setInterval(refreshDirectory, 5000);

    const unlistenPromise = listen<P2PEvent>("p2p-event", (event) => {
      const payload = event.payload;
      if (payload.type === "connection-requested") {
        setPendingRequests((prev) => {
          if (prev.some((r) => r.peer.nodeId === payload.peer.nodeId)) {
            return prev;
          }
          return [
            ...prev,
            {
              peer: payload.peer,
              introducedBy: payload.introducedBy,
              roomName: payload.roomName,
            },
          ];
        });
        setStatusMsg(
          payload.introducedBy
            ? `🔗 ${payload.introducedBy} さんからグループメンバー ${payload.peer.nickname} の紹介が届いています`
            : `📩 ${payload.peer.nickname} から接続リクエストが届いています`
        );
        setErrorMsg("");
      } else if (payload.type === "peer-connected") {
        setPendingRequests((prev) =>
          prev.filter((r) => r.peer.nodeId !== payload.peer.nodeId)
        );
        setConnectedPeers((prev) => {
          if (prev.some((p) => p.nodeId === payload.peer.nodeId)) return prev;
          return [...prev, payload.peer];
        });
        setStatusMsg(
          `✅ ${payload.peer.nickname} とP2P暗号化接続を確立しました`
        );
        setErrorMsg("");
      } else if (payload.type === "peer-disconnected") {
        if (payload.peer) {
          setConnectedPeers((prev) =>
            prev.filter((p) => p.nodeId !== payload.peer?.nodeId)
          );
          setStatusMsg(`🔌 ${payload.peer.nickname} との通信が切断されました`);
        } else {
          setConnectedPeers([]);
          setRoomId(null);
          setRoomName(null);
          setStatusMsg("🔌 すべての通信を切断しました");
        }
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

  const isConnected = connectedPeers.length > 0;

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
      setStatusMsg(
        `🔒 @${snap.username} として公開鍵と署名をサーバに登録しました`
      );
      await refreshDirectory();
    } catch (err) {
      setErrorMsg(String(err));
    } finally {
      setIsBusy(false);
    }
  };

  const handleConnectByUsername = async (targetUser: string) => {
    setErrorMsg("");
    setStatusMsg(
      `⏳ @${targetUser} に接続リクエストを送信中 (相手の承認待ち)...`
    );
    setIsBusy(true);
    try {
      const peer = await invoke<PeerInfo>("connect_user", {
        username: targetUser,
      });
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
      setRemoteTicketInput("");
      setStatusMsg(`✅ ${peer.nickname} と接続しました`);
    } catch (err) {
      setErrorMsg(String(err));
      setStatusMsg("");
    } finally {
      setIsBusy(false);
    }
  };

  const handleCreateRoom = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!newRoomNameInput.trim()) return;
    setErrorMsg("");
    setIsBusy(true);
    try {
      const snap = await invoke<StateSnapshot>("create_group_room", {
        roomName: newRoomNameInput.trim(),
      });
      applySnapshot(snap);
      setNewRoomNameInput("");
      setStatusMsg(`🏠 グループルーム「${snap.roomName}」を作成・公開しました`);
      await refreshDirectory();
    } catch (err) {
      setErrorMsg(String(err));
    } finally {
      setIsBusy(false);
    }
  };

  const handleJoinRoom = async (targetRoom: GroupRoom) => {
    setErrorMsg("");
    setStatusMsg(
      `⏳ ルーム「${targetRoom.name}」へ参加リクエストを送信中 (承認待ち)...`
    );
    setIsBusy(true);
    try {
      const snap = await invoke<StateSnapshot>("join_group_room", {
        roomId: targetRoom.roomId,
      });
      applySnapshot(snap);
      setStatusMsg(`✅ ルーム「${targetRoom.name}」に参加しました`);
      await refreshDirectory();
    } catch (err) {
      setErrorMsg(String(err));
      setStatusMsg("");
    } finally {
      setIsBusy(false);
    }
  };

  const handleRespondRequest = async (targetNodeId: string, accept: boolean) => {
    setErrorMsg("");
    try {
      setPendingRequests((prev) =>
        prev.filter((r) => r.peer.nodeId !== targetNodeId)
      );
      const snap = await invoke<StateSnapshot>("respond_connection_request", {
        nodeId: targetNodeId,
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

  const handleDisconnect = async (targetNodeId?: string) => {
    try {
      await invoke("disconnect_peer", { nodeId: targetNodeId || null });
      if (!targetNodeId) {
        setConnectedPeers([]);
        setRoomId(null);
        setRoomName(null);
        setStatusMsg("切断しました");
      }
      await refreshDirectory();
    } catch (err) {
      setErrorMsg(String(err));
    }
  };

  const handleSendChat = async (e: React.FormEvent) => {
    e.preventDefault();
    const content = chatInput.trim();
    if (!content || !isConnected) return;
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
      {/* Left Sidebar: P2P Connection, Group Rooms & Server Directory */}
      <aside className="sidebar-panel">
        <div className="brand-header">
          <h2>SimpleChat P2P</h2>
          <span className="crypto-badge" title={`Ed25519 NodeID: ${nodeId}`}>
            🔐 Full-Mesh E2EE
          </span>
        </div>

        {/* Active Session / Group Members Card */}
        <div
          className={`connection-card-group ${
            isConnected || roomName ? "active" : ""
          }`}
        >
          <div className="connection-top-row">
            <div className="connection-info">
              <span className={`dot ${isConnected ? "online" : "offline"}`} />
              <div>
                <div className="conn-title">
                  {roomName
                    ? `🏠 ${roomName} (${connectedPeers.length + 1}人)`
                    : connectedPeers.length > 1
                      ? `👥 グループ会話 (${connectedPeers.length + 1}人)`
                      : connectedPeers.length === 1
                        ? `接続中: ${connectedPeers[0].nickname}`
                        : "未接続 (待機中)"}
                </div>
                {roomId && (
                  <div className="conn-sub">RoomID: {roomId}</div>
                )}
              </div>
            </div>
            {(isConnected || roomId) && (
              <button
                type="button"
                className="btn-disconnect"
                onClick={() => handleDisconnect()}
                title="すべてのピアから切断して退出"
              >
                {connectedPeers.length > 1 || roomId ? "退出" : "切断"}
              </button>
            )}
          </div>

          {connectedPeers.length > 0 && (
            <div className="peer-chips-list">
              {connectedPeers.map((p) => (
                <div key={p.nodeId} className="peer-chip" title={p.nodeId}>
                  <span className="peer-chip-name">
                    🟢 {p.nickname}
                    {p.username ? ` (@${p.username})` : ""}
                  </span>
                  <button
                    type="button"
                    className="peer-chip-remove"
                    onClick={() => handleDisconnect(p.nodeId)}
                    title={`${p.nickname} との接続を切断`}
                  >
                    ✕
                  </button>
                </div>
              ))}
            </div>
          )}
        </div>

        {/* Incoming Connection / Group Introduction Approval Cards */}
        {pendingRequests.map((req) => (
          <div key={req.peer.nodeId} className="incoming-request-card">
            <div className="incoming-request-header">
              {req.introducedBy
                ? `🔗 ${req.introducedBy} さんからのグループメンバー紹介`
                : "📩 着信P2P接続リクエスト"}
            </div>
            <div className="incoming-request-peer">
              <strong>{req.peer.nickname}</strong>
              {req.peer.username ? ` (@${req.peer.username})` : ""}
              {req.roomName ? ` [🏠 ${req.roomName}]` : ""}
              <div className="incoming-request-node">
                NodeID: {req.peer.nodeId.slice(0, 16)}...
              </div>
            </div>
            <div className="incoming-request-actions">
              <button
                type="button"
                className="btn-approve"
                onClick={() => handleRespondRequest(req.peer.nodeId, true)}
              >
                承認して接続
              </button>
              <button
                type="button"
                className="btn-reject"
                onClick={() => handleRespondRequest(req.peer.nodeId, false)}
              >
                拒否
              </button>
            </div>
          </div>
        ))}

        {/* Mode Tabs (3 tabs: Users, Rooms, Manual Ticket) */}
        <div className="tab-switcher three-tabs">
          <button
            type="button"
            className={activeTab === "server" ? "active" : ""}
            onClick={() => setActiveTab("server")}
          >
            🌐 ユーザ一覧
          </button>
          <button
            type="button"
            className={activeTab === "rooms" ? "active" : ""}
            onClick={() => {
              setActiveTab("rooms");
              refreshDirectory();
            }}
          >
            🏠 ルーム
          </button>
          <button
            type="button"
            className={activeTab === "manual" ? "active" : ""}
            onClick={() => setActiveTab("manual")}
          >
            🎫 チケット
          </button>
        </div>

        {activeTab === "server" && (
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
                <span>
                  オンラインユーザ一覧{" "}
                  {isConnected ? "(クリックでグループ追加)" : ""}
                </span>
                <button
                  type="button"
                  className="btn-link"
                  onClick={refreshDirectory}
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
                    .map((u) => {
                      const alreadyConnected = connectedPeers.some(
                        (p) => p.nodeId === u.nodeId
                      );
                      return (
                        <div key={u.username} className="user-item">
                          <div className="user-meta">
                            <span
                              className={`dot ${
                                u.online ? "online" : "offline"
                              }`}
                            />
                            <div>
                              <div className="user-name">
                                {u.displayName}{" "}
                                <span className="user-handle">
                                  @{u.username}
                                </span>
                              </div>
                              <div className="user-key">
                                🔑 {u.nodeId.slice(0, 12)}...
                              </div>
                            </div>
                          </div>
                          <button
                            type="button"
                            className="btn-connect-sm"
                            disabled={isBusy || alreadyConnected}
                            onClick={() => handleConnectByUsername(u.username)}
                          >
                            {alreadyConnected
                              ? "接続済"
                              : isConnected
                                ? "➕ グループ追加"
                                : "P2P接続"}
                          </button>
                        </div>
                      );
                    })
                )}
              </div>
            </div>
          </div>
        )}

        {activeTab === "rooms" && (
          <div className="tab-content">
            <form className="config-form" onSubmit={handleCreateRoom}>
              <label>
                新しいグループルームを作成 (または現在のグループに名前を設定)
                <div className="ticket-box">
                  <input
                    type="text"
                    value={newRoomNameInput}
                    onChange={(e) => setNewRoomNameInput(e.target.value)}
                    placeholder="例: 開発雑談ルーム"
                  />
                  <button
                    type="submit"
                    className="btn-primary no-margin"
                    disabled={isBusy || !newRoomNameInput.trim()}
                  >
                    🏠 作成
                  </button>
                </div>
              </label>
            </form>

            <div className="directory-section">
              <div className="section-header">
                <span>公開グループルーム一覧</span>
                <button
                  type="button"
                  className="btn-link"
                  onClick={refreshDirectory}
                >
                  更新 ↻
                </button>
              </div>
              <div className="user-list">
                {groupRooms.length === 0 ? (
                  <div className="empty-hint">
                    現在アクティブなルームはありません。上の欄からルームを作成するか、「ユーザ一覧」から複数の相手に直接接続してグループ化できます。
                  </div>
                ) : (
                  groupRooms.map((r) => {
                    const inThisRoom =
                      roomId === r.roomId ||
                      r.members.some((m) => m.nodeId === nodeId);
                    return (
                      <div key={r.roomId} className="user-item">
                        <div className="user-meta">
                          <span className="dot online" />
                          <div>
                            <div className="user-name">
                              🏠 {r.name}{" "}
                              <span className="user-handle">
                                ({r.members.length}人)
                              </span>
                            </div>
                            <div className="user-key">
                              メンバー:{" "}
                              {r.members.map((m) => m.nickname).join(", ")}
                            </div>
                          </div>
                        </div>
                        <button
                          type="button"
                          className="btn-connect-sm"
                          disabled={isBusy || inThisRoom}
                          onClick={() => handleJoinRoom(r)}
                        >
                          {inThisRoom ? "参加中" : "参加リクエスト"}
                        </button>
                      </div>
                    );
                  })
                )}
              </div>
            </div>
          </div>
        )}

        {activeTab === "manual" && (
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
                  相手の接続チケットを貼り付けて接続 / グループ追加
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
                  {isConnected
                    ? "➕ チケットでグループに追加"
                    : "🔗 チケットでP2P接続"}
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
                左パネルから相手（複数人可）と接続し、メッセージを送受信するとここに記録されます。
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
              connectedPeers.length > 1
                ? `${
                    roomName || "グループ"
                  } (${connectedPeers.length}人へ一斉送信)...`
                : connectedPeers.length === 1
                  ? `${connectedPeers[0].nickname} にメッセージを送信 (Enterで送信)...`
                  : "左パネルで相手とP2P接続すると送信できます"
            }
            disabled={!isConnected}
          />
          <button
            type="submit"
            className="btn-send"
            disabled={!isConnected || !chatInput.trim()}
          >
            送信
          </button>
        </form>
      </main>
    </div>
  );
};
