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
  const [matchingEnabled, setMatchingEnabled] = useState<boolean>(false);
  const [activeMatchingTag, setActiveMatchingTag] = useState<string>("");
  const [matchingTagInput, setMatchingTagInput] = useState<string>("");
  const [roomId, setRoomId] = useState<string | null>(null);
  const [roomName, setRoomName] = useState<string | null>(null);
  const [roomHasPassphrase, setRoomHasPassphrase] = useState<boolean>(false);
  const [activeChannelId, setActiveChannelId] = useState<string | null>(null);
  const [connectedPeers, setConnectedPeers] = useState<PeerInfo[]>([]);
  const [pendingRequests, setPendingRequests] = useState<PendingRequestInfo[]>(
    []
  );
  const [messages, setMessages] = useState<ChatMessagePayload[]>([]);
  const [selectedKindFilter, setSelectedKindFilter] = useState<
    "all" | "direct" | "group" | "match"
  >("all");
  const [selectedChannelId, setSelectedChannelId] = useState<string | null>(
    null
  );

  const [activeTab, setActiveTab] = useState<"server" | "rooms" | "manual">(
    "server"
  );
  const [onlineUsers, setOnlineUsers] = useState<DirectoryUser[]>([]);
  const [groupRooms, setGroupRooms] = useState<GroupRoom[]>([]);
  const [newRoomNameInput, setNewRoomNameInput] = useState<string>("");
  const [newRoomPassphraseInput, setNewRoomPassphraseInput] =
    useState<string>("");
  const [joinRoomPassphrases, setJoinRoomPassphrases] = useState<
    Record<string, string>
  >({});
  const [remoteTicketInput, setRemoteTicketInput] = useState<string>("");
  const [chatInput, setChatInput] = useState<string>("");
  const [statusMsg, setStatusMsg] = useState<string>("");
  const [errorMsg, setErrorMsg] = useState<string>("");
  const [isBusy, setIsBusy] = useState<boolean>(false);

  const timelineRef = useRef<HTMLDivElement | null>(null);
  const lastAutoChannelRef = useRef<string | null>(null);

  const applySnapshot = (snap: StateSnapshot, syncTagInput = false) => {
    setNodeId(snap.nodeId);
    setTicket(snap.ticket);
    setNickname(snap.nickname);
    if (snap.username) setUsername(snap.username);
    setServerUrl(snap.serverUrl);
    setRegisteredOnServer(snap.registeredOnServer);
    setMatchingEnabled(Boolean(snap.matchingEnabled));
    setActiveMatchingTag(snap.matchingTag || "");
    if (syncTagInput) {
      setMatchingTagInput(snap.matchingTag || "");
    }
    setRoomId(snap.roomId || null);
    setRoomName(snap.roomName || null);
    setRoomHasPassphrase(Boolean(snap.roomHasPassphrase));
    const nextActiveChannel = snap.activeChannelId || null;
    setActiveChannelId(nextActiveChannel);
    if (
      nextActiveChannel &&
      nextActiveChannel !== lastAutoChannelRef.current
    ) {
      lastAutoChannelRef.current = nextActiveChannel;
      setSelectedChannelId(nextActiveChannel);
      setSelectedKindFilter("all");
    } else if (!nextActiveChannel) {
      lastAutoChannelRef.current = null;
    }
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
      .then((snap) => applySnapshot(snap, true))
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
              matchedTag: payload.matchedTag,
            },
          ];
        });
        setStatusMsg(
          payload.matchedTag !== undefined && payload.matchedTag !== null
            ? `📡 マッチングチャットで ${payload.peer.nickname} さんがヒットしました！`
            : payload.introducedBy
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
        if (payload.message.channelId) {
          setSelectedChannelId((prev) => prev || payload.message.channelId!);
        }
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
    if (timelineRef.current) {
      timelineRef.current.scrollTop = timelineRef.current.scrollHeight;
    }
    window.scrollTo(0, 0);
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
      const pass = newRoomPassphraseInput.trim();
      const snap = await invoke<StateSnapshot>("create_group_room", {
        roomName: newRoomNameInput.trim(),
        passphrase: pass ? pass : null,
      });
      applySnapshot(snap);
      setNewRoomNameInput("");
      setNewRoomPassphraseInput("");
      setStatusMsg(
        pass
          ? `🔒 合言葉付きグループルーム「${snap.roomName}」を作成・公開しました`
          : `🏠 グループルーム「${snap.roomName}」を作成・公開しました`
      );
      await refreshDirectory();
    } catch (err) {
      setErrorMsg(String(err));
    } finally {
      setIsBusy(false);
    }
  };

  const handleJoinRoom = async (targetRoom: GroupRoom) => {
    setErrorMsg("");
    const isLocked = Boolean(
      targetRoom.hasPassphrase || targetRoom.roomId.startsWith("room-locked-")
    );
    const pass = (joinRoomPassphrases[targetRoom.roomId] || "").trim();
    if (isLocked && !pass) {
      setErrorMsg(
        `ルーム「${targetRoom.name}」に参加するには合言葉を入力してください`
      );
      return;
    }
    setStatusMsg(
      isLocked
        ? `🔑 合言葉でルーム「${targetRoom.name}」へ接続中...`
        : `⏳ ルーム「${targetRoom.name}」へ参加リクエストを送信中 (承認待ち)...`
    );
    setIsBusy(true);
    try {
      const snap = await invoke<StateSnapshot>("join_group_room", {
        roomId: targetRoom.roomId,
        passphrase: pass ? pass : null,
      });
      applySnapshot(snap);
      setJoinRoomPassphrases((prev) => ({
        ...prev,
        [targetRoom.roomId]: "",
      }));
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
        setStatusMsg("🛡️ 接続リクエストを見送りました");
      }
    } catch (err) {
      setErrorMsg(String(err));
    }
  };

  const handleToggleLanMatching = async (nextEnabled: boolean) => {
    setErrorMsg("");
    try {
      const snap = await invoke<StateSnapshot>("set_lan_matching", {
        enabled: nextEnabled,
        matchingTag: matchingTagInput.trim(),
      });
      applySnapshot(snap, true);
      if (snap.matchingEnabled) {
        setStatusMsg(
          snap.matchingTag
            ? `📡 「#${snap.matchingTag}」でマッチングチャットを開始しました`
            : "📡 マッチングチャット（誰でも）を開始しました"
        );
      } else {
        setStatusMsg("📡 マッチングチャットを停止しました");
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

  interface ChannelThread {
    channelId: string;
    channelKind: "direct" | "group" | "match";
    channelName: string;
    count: number;
    lastTimestamp: number;
    isActive: boolean;
  }

  const normalizeKind = (k?: string): "direct" | "group" | "match" => {
    if (k === "group" || k === "match") return k;
    return "direct";
  };

  const kindLabel = (k: "direct" | "group" | "match") => {
    if (k === "group") return "🏠 グループ";
    if (k === "match") return "📡 マッチングチャット";
    return "👤 1対1";
  };

  const kindIcon = (k: "direct" | "group" | "match") => {
    if (k === "group") return "🏠";
    if (k === "match") return "📡";
    return "👤";
  };

  const threadMap = new Map<string, ChannelThread>();
  for (const m of messages) {
    const cid =
      m.channelId && m.channelId.trim()
        ? m.channelId
        : `direct:${m.isSelf ? "legacy" : m.senderId}`;
    const ckind = normalizeKind(
      m.channelKind ||
        (cid.startsWith("group:")
          ? "group"
          : cid.startsWith("match:")
            ? "match"
            : "direct")
    );
    const cname =
      m.channelName && m.channelName.trim()
        ? m.channelName
        : m.isSelf
          ? "チャット相手"
          : m.senderName;
    const existing = threadMap.get(cid);
    if (existing) {
      existing.count += 1;
      if (m.timestamp >= existing.lastTimestamp) {
        existing.lastTimestamp = m.timestamp;
        if (m.channelName && m.channelName.trim()) {
          existing.channelName = m.channelName;
        }
      }
    } else {
      threadMap.set(cid, {
        channelId: cid,
        channelKind: ckind,
        channelName: cname,
        count: 1,
        lastTimestamp: m.timestamp,
        isActive: cid === activeChannelId,
      });
    }
  }

  if (activeChannelId && !threadMap.has(activeChannelId)) {
    const activeKind: "direct" | "group" | "match" = activeChannelId.startsWith(
      "group:"
    )
      ? "group"
      : activeChannelId.startsWith("match:")
        ? "match"
        : "direct";
    const activeName =
      roomName ||
      (connectedPeers.length > 1
        ? `グループ (${connectedPeers.map((p) => p.nickname).join(", ")})`
        : connectedPeers.length === 1
          ? activeKind === "match"
            ? `${connectedPeers[0].nickname} (${activeMatchingTag ? `#${activeMatchingTag}` : "誰でも"})`
            : connectedPeers[0].nickname
          : "現在のセッション");
    threadMap.set(activeChannelId, {
      channelId: activeChannelId,
      channelKind: activeKind,
      channelName: activeName,
      count: 0,
      lastTimestamp: Date.now(),
      isActive: true,
    });
  }

  const allThreads = Array.from(threadMap.values()).sort((a, b) => {
    if (a.isActive !== b.isActive) return a.isActive ? -1 : 1;
    return b.lastTimestamp - a.lastTimestamp;
  });

  const filteredThreads =
    selectedKindFilter === "all"
      ? allThreads
      : allThreads.filter((t) => t.channelKind === selectedKindFilter);

  const effectiveChannelId =
    selectedChannelId &&
    filteredThreads.some((t) => t.channelId === selectedChannelId)
      ? selectedChannelId
      : filteredThreads.length > 0
        ? filteredThreads[0].channelId
        : null;

  const currentThread =
    allThreads.find((t) => t.channelId === effectiveChannelId) || null;

  const visibleMessages = messages.filter((m) => {
    const cid =
      m.channelId && m.channelId.trim()
        ? m.channelId
        : `direct:${m.isSelf ? "legacy" : m.senderId}`;
    if (effectiveChannelId) {
      return cid === effectiveChannelId;
    }
    if (selectedKindFilter !== "all") {
      const ckind = normalizeKind(
        m.channelKind ||
          (cid.startsWith("group:")
            ? "group"
            : cid.startsWith("match:")
              ? "match"
              : "direct")
      );
      return ckind === selectedKindFilter;
    }
    return true;
  });

  const handleCopyTicket = async () => {
    try {
      await navigator.clipboard.writeText(ticket);
      setStatusMsg("📋 接続チケットをクリップボードにコピーしました");
    } catch {
      setErrorMsg("クリップボードへのコピーに失敗しました");
    }
  };

  const handleExportHistory = async () => {
    if (visibleMessages.length === 0) return;
    setErrorMsg("");
    try {
      const headerLine = currentThread
        ? `=== [${kindLabel(currentThread.channelKind)}] ${currentThread.channelName} ===\n`
        : "=== チャット履歴 ===\n";
      const lines = visibleMessages.map((m) => {
        const dt = new Date(m.timestamp).toLocaleString();
        const who = m.isSelf ? `${nickname} (自分)` : m.senderName;
        return `[${dt}] ${who}: ${m.content}`;
      });
      const savedPath = await invoke<string>("export_chat_history", {
        formattedText: headerLine + lines.join("\n") + "\n",
      });
      setStatusMsg(`💾 チャット履歴を保存しました: ${savedPath}`);
    } catch (err) {
      setErrorMsg(String(err));
    }
  };

  const handleClearHistory = async () => {
    if (visibleMessages.length === 0) return;
    setErrorMsg("");
    try {
      const targetChannelId = currentThread ? currentThread.channelId : null;
      const snap = await invoke<StateSnapshot>("clear_chat_history", {
        channelId: targetChannelId,
      });
      applySnapshot(snap);
      setStatusMsg(
        currentThread
          ? `🗑️ 「${currentThread.channelName}」の履歴を消去しました`
          : "🗑️ チャット履歴を消去しました"
      );
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
                  <div className="conn-sub">
                    RoomID: {roomId}{" "}
                    {roomHasPassphrase ? "🔒 合言葉保護" : "🌐 公開"}
                  </div>
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

        {/* Matching Chat (LAN Auto-Discovery) Card */}
        <div
          className={`lan-matching-card ${matchingEnabled ? "is-searching" : ""}`}
        >
          <div className="lan-matching-header">
            <div className="lan-matching-title">
              <span
                className={`radar-dot ${matchingEnabled ? "active" : ""}`}
              />
              <span className="lan-matching-heading">📡 マッチングチャット</span>
            </div>
            <span
              className={`matching-status-badge ${matchingEnabled ? "is-on" : "is-off"}`}
            >
              {matchingEnabled
                ? activeMatchingTag
                  ? `探索中 (#${activeMatchingTag})`
                  : "探索中 (誰でも)"
                : "停止中"}
            </span>
          </div>
          <div className="lan-matching-controls">
            <input
              type="text"
              className="lan-tag-input"
              value={matchingTagInput}
              disabled={matchingEnabled}
              onChange={(e) => setMatchingTagInput(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === "Enter" && !matchingEnabled) {
                  e.preventDefault();
                  handleToggleLanMatching(true);
                }
              }}
              placeholder="合言葉タグ (空欄で誰でも)"
            />
            {!matchingEnabled ? (
              <button
                type="button"
                className="btn-matching-action start"
                onClick={() => handleToggleLanMatching(true)}
              >
                🔍 マッチング開始
              </button>
            ) : (
              <button
                type="button"
                className="btn-matching-action stop"
                onClick={() => handleToggleLanMatching(false)}
              >
                ⏹️ 停止 (OFF)
              </button>
            )}
          </div>
        </div>

        {/* Incoming Connection / Group Introduction / Matching Chat Approval Cards */}
        {pendingRequests.map((req) => {
          const isLanMatch =
            req.matchedTag !== undefined && req.matchedTag !== null;
          return (
            <div
              key={req.peer.nodeId}
              className={`incoming-request-card ${isLanMatch ? "lan-match-card" : ""}`}
            >
              <div className="incoming-request-header">
                {isLanMatch
                  ? `📡 マッチングチャット発見！${req.matchedTag ? ` (#${req.matchedTag})` : " (誰でも)"}`
                  : req.introducedBy
                    ? `🔗 ${req.introducedBy} さんからのグループメンバー紹介`
                    : "📩 着信P2P接続リクエスト"}
              </div>
              <div className="incoming-request-peer">
                <strong>{req.peer.nickname}</strong>
                {req.peer.username ? ` (@${req.peer.username})` : ""}
                {req.roomName ? ` [🏠 ${req.roomName}]` : ""}
                {isLanMatch && (
                  <span className="match-hit-text">
                    {" "}
                    さんとヒットしました！接続しますか？
                  </span>
                )}
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
                  {isLanMatch ? "🤝 接続する" : "承認して接続"}
                </button>
                <button
                  type="button"
                  className="btn-reject"
                  onClick={() => handleRespondRequest(req.peer.nodeId, false)}
                >
                  {isLanMatch ? "見送る" : "拒否"}
                </button>
              </div>
            </div>
          );
        })}

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
                ルーム名 (新しいルーム作成 / 現在のグループに設定)
                <input
                  type="text"
                  value={newRoomNameInput}
                  onChange={(e) => setNewRoomNameInput(e.target.value)}
                  placeholder="例: 開発雑談ルーム"
                />
              </label>
              <label>
                🔑 合言葉 (任意・設定すると合言葉を知る人のみ参加可)
                <div className="ticket-box">
                  <input
                    type="text"
                    value={newRoomPassphraseInput}
                    onChange={(e) => setNewRoomPassphraseInput(e.target.value)}
                    placeholder="空欄なら誰でも参加リクエスト可"
                  />
                  <button
                    type="submit"
                    className="btn-primary no-margin"
                    disabled={isBusy || !newRoomNameInput.trim()}
                  >
                    {newRoomPassphraseInput.trim()
                      ? "🔒 鍵付き作成"
                      : "🏠 作成"}
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
                    const isLocked = Boolean(
                      r.hasPassphrase || r.roomId.startsWith("room-locked-")
                    );
                    const passVal = joinRoomPassphrases[r.roomId] || "";
                    return (
                      <div key={r.roomId} className="user-item room-item-card">
                        <div className="room-item-top">
                          <div className="user-meta">
                            <span className="dot online" />
                            <div>
                              <div className="user-name">
                                {isLocked ? "🔒" : "🏠"} {r.name}{" "}
                                <span className="user-handle">
                                  ({r.members.length}人)
                                </span>
                                <span
                                  className={`room-lock-badge ${
                                    isLocked ? "locked" : "open"
                                  }`}
                                >
                                  {isLocked ? "合言葉必要" : "オープン"}
                                </span>
                              </div>
                              <div className="user-key">
                                メンバー:{" "}
                                {r.members.map((m) => m.nickname).join(", ")}
                              </div>
                            </div>
                          </div>
                          {!isLocked || inThisRoom ? (
                            <button
                              type="button"
                              className="btn-connect-sm"
                              disabled={isBusy || inThisRoom}
                              onClick={() => handleJoinRoom(r)}
                            >
                              {inThisRoom ? "参加中" : "参加リクエスト"}
                            </button>
                          ) : null}
                        </div>

                        {isLocked && !inThisRoom && (
                          <div className="room-passphrase-row">
                            <input
                              type="text"
                              className="room-passphrase-input"
                              value={passVal}
                              onChange={(e) =>
                                setJoinRoomPassphrases((prev) => ({
                                  ...prev,
                                  [r.roomId]: e.target.value,
                                }))
                              }
                              onKeyDown={(e) => {
                                if (e.key === "Enter" && passVal.trim()) {
                                  e.preventDefault();
                                  handleJoinRoom(r);
                                }
                              }}
                              placeholder="🔑 合言葉を入力..."
                            />
                            <button
                              type="button"
                              className="btn-connect-sm"
                              disabled={isBusy || !passVal.trim()}
                              onClick={() => handleJoinRoom(r)}
                            >
                              🔑 参加
                            </button>
                          </div>
                        )}
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

      {/* Right Panel: Persistent Chat History Separated by Channel */}
      <main className="chat-history-panel">
        <header className="chat-header">
          <div className="chat-header-info">
            <div className="chat-title-row">
              <h3>チャット履歴</h3>
              {currentThread && (
                <span
                  className={`channel-kind-badge kind-${currentThread.channelKind}`}
                >
                  {kindLabel(currentThread.channelKind)}:{" "}
                  {currentThread.channelName}
                </span>
              )}
              <span className="history-count-badge">
                {visibleMessages.length} 件
              </span>
              <span className="self-node-tag" title={`My NodeID: ${nodeId}`}>
                ID: {nodeId ? `${nodeId.slice(0, 8)}...` : "..."}
              </span>
            </div>
            <p className="chat-subtitle">
              1対1（相手ごと）・グループ（ルームごと）・マッチングチャット（回ごと）に自動分類されます
            </p>
          </div>
          <div className="chat-header-actions">
            <button
              type="button"
              className="btn-header-action"
              disabled={visibleMessages.length === 0}
              onClick={handleExportHistory}
              title={
                currentThread
                  ? `表示中の「${currentThread.channelName}」の履歴を保存`
                  : "テキストファイルとしてダウンロードフォルダに保存"
              }
            >
              💾 ログ出力
            </button>
            <button
              type="button"
              className="btn-header-action danger-outline"
              disabled={visibleMessages.length === 0}
              onClick={handleClearHistory}
              title={
                currentThread
                  ? `表示中の「${currentThread.channelName}」の履歴を消去`
                  : "保存されているチャット履歴を消去"
              }
            >
              🗑️ 履歴消去
            </button>
          </div>
        </header>

        {/* Channel Category Filter & Thread Selector Bar */}
        <div className="channel-selector-bar">
          <div className="channel-kind-tabs">
            <button
              type="button"
              className={`kind-tab-btn ${selectedKindFilter === "all" ? "active" : ""}`}
              onClick={() => setSelectedKindFilter("all")}
            >
              すべて ({allThreads.length})
            </button>
            <button
              type="button"
              className={`kind-tab-btn ${selectedKindFilter === "direct" ? "active" : ""}`}
              onClick={() => setSelectedKindFilter("direct")}
            >
              👤 1対1 (
              {allThreads.filter((t) => t.channelKind === "direct").length})
            </button>
            <button
              type="button"
              className={`kind-tab-btn ${selectedKindFilter === "group" ? "active" : ""}`}
              onClick={() => setSelectedKindFilter("group")}
            >
              🏠 グループ (
              {allThreads.filter((t) => t.channelKind === "group").length})
            </button>
            <button
              type="button"
              className={`kind-tab-btn ${selectedKindFilter === "match" ? "active" : ""}`}
              onClick={() => setSelectedKindFilter("match")}
            >
              📡 マッチング (
              {allThreads.filter((t) => t.channelKind === "match").length})
            </button>
          </div>

          {filteredThreads.length > 0 && (
            <div className="channel-thread-pills">
              {filteredThreads.map((t) => {
                const isSelected = t.channelId === effectiveChannelId;
                return (
                  <button
                    key={t.channelId}
                    type="button"
                    className={`thread-pill kind-${t.channelKind} ${
                      isSelected ? "selected" : ""
                    } ${t.isActive ? "live" : ""}`}
                    onClick={() => setSelectedChannelId(t.channelId)}
                    title={`${kindLabel(t.channelKind)} - ${t.channelName}`}
                  >
                    <span className="thread-pill-icon">
                      {kindIcon(t.channelKind)}
                    </span>
                    <span className="thread-pill-name">{t.channelName}</span>
                    <span className="thread-pill-count">{t.count}</span>
                    {t.isActive && (
                      <span className="thread-live-dot" title="現在接続中" />
                    )}
                  </button>
                );
              })}
            </div>
          )}
        </div>

        <div className="messages-timeline" ref={timelineRef}>
          {visibleMessages.length === 0 ? (
            <div className="empty-chat">
              <p>
                {currentThread
                  ? `「${currentThread.channelName}」(${kindLabel(currentThread.channelKind)}) のメッセージはまだありません。`
                  : selectedKindFilter !== "all"
                    ? `${kindLabel(selectedKindFilter)} のチャット履歴はまだありません。`
                    : "まだチャット履歴がありません。"}
              </p>
              <p className="sub">
                左パネルから1対1接続・グループルーム・マッチングチャットを開始すると、それぞれの履歴タブに自動で振り分けられます。
              </p>
            </div>
          ) : (
            visibleMessages.map((m) => (
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
