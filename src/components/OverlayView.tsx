import React, { useEffect, useRef, useState } from "react";
import { invoke } from "@tauri-apps/api/core";
import { listen } from "@tauri-apps/api/event";
import { getCurrentWindow } from "@tauri-apps/api/window";
import { ChatMessagePayload, P2PEvent, PeerInfo, StateSnapshot } from "../types";

interface EphemeralBubble extends ChatMessagePayload {
  fading: boolean;
}

const BUBBLE_FADE_START_MS = 3800;
const BUBBLE_REMOVE_MS = 5000;

export const OverlayView: React.FC = () => {
  const [peer, setPeer] = useState<PeerInfo | null>(null);
  const [pendingRequest, setPendingRequest] = useState<PeerInfo | null>(null);
  const [nickname, setNickname] = useState<string>("Me");
  const [bubbles, setBubbles] = useState<EphemeralBubble[]>([]);
  const [quickText, setQuickText] = useState<string>("");
  const [showQuickInput, setShowQuickInput] = useState<boolean>(true);
  const [statusNotice, setStatusNotice] = useState<{
    text: string;
    fading: boolean;
  } | null>(null);
  const inputRef = useRef<HTMLInputElement | null>(null);

  const pushBubble = (msg: ChatMessagePayload) => {
    setBubbles((prev) => {
      if (prev.some((b) => b.id === msg.id)) return prev;
      return [...prev.slice(-2), { ...msg, fading: false }];
    });

    window.setTimeout(() => {
      setBubbles((prev) =>
        prev.map((b) => (b.id === msg.id ? { ...b, fading: true } : b))
      );
    }, BUBBLE_FADE_START_MS);

    window.setTimeout(() => {
      setBubbles((prev) => prev.filter((b) => b.id !== msg.id));
    }, BUBBLE_REMOVE_MS);
  };

  const showTransientNotice = (text: string) => {
    setStatusNotice({ text, fading: false });
    window.setTimeout(() => {
      setStatusNotice((curr) =>
        curr && curr.text === text ? { ...curr, fading: true } : curr
      );
    }, 2600);
    window.setTimeout(() => {
      setStatusNotice((curr) => (curr && curr.text === text ? null : curr));
    }, 3800);
  };

  useEffect(() => {
    document.documentElement.classList.add("overlay-mode");
    document.body.classList.add("overlay-mode");

    invoke<StateSnapshot>("get_state_snapshot")
      .then((snap) => {
        setPeer(snap.connectedPeer);
        setPendingRequest(snap.pendingRequest);
        setNickname(snap.nickname);
      })
      .catch((err) => console.error("Failed to get snapshot:", err));

    const unlistenPromise = listen<P2PEvent>("p2p-event", (event) => {
      const payload = event.payload;
      if (payload.type === "connection-requested") {
        setPendingRequest(payload.peer);
      } else if (payload.type === "peer-connected") {
        setPendingRequest(null);
        setPeer(payload.peer);
        showTransientNotice(`✨ ${payload.peer.nickname} と接続しました`);
      } else if (payload.type === "peer-disconnected") {
        setPeer(null);
        showTransientNotice("🔌 切断されました");
      } else if (payload.type === "chat-message") {
        pushBubble(payload.message);
      } else if (payload.type === "state-updated") {
        setPeer(payload.snapshot.connectedPeer);
        setPendingRequest(payload.snapshot.pendingRequest);
        setNickname(payload.snapshot.nickname);
      }
    });

    return () => {
      unlistenPromise.then((unlisten) => unlisten());
    };
  }, []);

  const handleDragMouseDown = (e: React.MouseEvent) => {
    if (e.button === 0) {
      getCurrentWindow().startDragging().catch(() => {});
    }
  };

  const handleQuickSend = async (e: React.FormEvent) => {
    e.preventDefault();
    const content = quickText.trim();
    if (!content) return;
    if (!peer) {
      showTransientNotice("先に履歴・接続画面から相手と接続してください");
      return;
    }
    try {
      setQuickText("");
      await invoke("send_message", { content });
    } catch (err) {
      showTransientNotice(`送信エラー: ${String(err)}`);
    }
  };

  const handleRespondRequest = async (accept: boolean) => {
    try {
      setPendingRequest(null);
      await invoke("respond_connection_request", { accept });
      if (!accept) {
        showTransientNotice("🛡️ 接続リクエストを拒否しました");
      }
    } catch (err) {
      showTransientNotice(`応答エラー: ${String(err)}`);
    }
  };

  const handleToggleMain = async () => {
    try {
      await invoke("toggle_main_window");
    } catch (err) {
      console.error(err);
    }
  };

  const handleQuit = async () => {
    try {
      await invoke("quit_app");
    } catch (err) {
      console.error(err);
    }
  };

  return (
    <div className="overlay-container">
      {/* Ephemeral speech bubbles stack (fixed safe zone at top) */}
      <div className="bubble-stack">
        {pendingRequest && (
          <div className="ephemeral-bubble request-bubble pop-in">
            <div className="request-title">📩 通信リクエスト</div>
            <div className="request-body">
              <strong>{pendingRequest.nickname}</strong>
              {pendingRequest.username ? ` (@${pendingRequest.username})` : ""} からP2P接続の要求があります
            </div>
            <div className="request-actions">
              <button
                type="button"
                className="btn-approve"
                onClick={() => handleRespondRequest(true)}
              >
                承認する
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
        {statusNotice && (
          <div
            className={`ephemeral-bubble notice-bubble ${
              statusNotice.fading ? "fade-out" : "pop-in"
            }`}
          >
            {statusNotice.text}
          </div>
        )}
        {bubbles.map((b) => (
          <div
            key={b.id}
            className={`ephemeral-bubble ${b.isSelf ? "self-bubble" : "peer-bubble"} ${
              b.fading ? "fade-out" : "pop-in"
            }`}
          >
            <div className="bubble-header">
              <span className="bubble-sender">
                {b.isSelf ? `${nickname} (あなた)` : b.senderName}
              </span>
            </div>
            <div className="bubble-text">{b.content}</div>
          </div>
        ))}
      </div>

      {/* Draggable Desktop Mascot Character */}
      <div className="mascot-stage">
        <div
          className={`mascot-avatar ${peer ? "is-connected" : "is-idle"}`}
          data-tauri-drag-region
          onMouseDown={handleDragMouseDown}
          title="ドラッグして移動"
        >
          <svg
            viewBox="0 0 120 120"
            width="104"
            height="104"
            className="mascot-svg"
            data-tauri-drag-region
          >
            <defs>
              <radialGradient id="bodyGrad" cx="35%" cy="30%" r="70%">
                <stop offset="0%" stopColor={peer ? "#6ee7b7" : "#93c5fd"} />
                <stop offset="60%" stopColor={peer ? "#10b981" : "#3b82f6"} />
                <stop offset="100%" stopColor={peer ? "#047857" : "#1d4ed8"} />
              </radialGradient>
            </defs>
            {/* Floating Ears / Antenna */}
            <circle
              cx="30"
              cy="26"
              r="10"
              fill="url(#bodyGrad)"
              stroke="#0f172a"
              strokeWidth="2"
            />
            <circle
              cx="90"
              cy="26"
              r="10"
              fill="url(#bodyGrad)"
              stroke="#0f172a"
              strokeWidth="2"
            />
            {/* Main Mascot Body */}
            <rect
              x="20"
              y="24"
              width="80"
              height="74"
              rx="36"
              fill="url(#bodyGrad)"
              stroke="#ffffff"
              strokeWidth="3"
            />
            {/* Visor / Face */}
            <rect
              x="31"
              y="40"
              width="58"
              height="34"
              rx="17"
              fill="#0f172a"
            />
            {/* Eyes */}
            <circle cx="46" cy="56" r="5.5" fill={peer ? "#34d399" : "#60a5fa"} />
            <circle cx="74" cy="56" r="5.5" fill={peer ? "#34d399" : "#60a5fa"} />
            <circle cx="44" cy="54" r="2" fill="#ffffff" />
            <circle cx="72" cy="54" r="2" fill="#ffffff" />
            {/* Cute Smile */}
            <path
              d="M 54 64 Q 60 69 66 64"
              stroke={peer ? "#6ee7b7" : "#93c5fd"}
              strokeWidth="2.5"
              fill="none"
              strokeLinecap="round"
            />
          </svg>

          <div className={`status-pill ${peer ? "online" : "offline"}`}>
            <span className="status-dot" />
            <span className="status-label">
              {peer ? `${peer.nickname}` : "未接続"}
            </span>
          </div>
        </div>

        {/* Floating Quick Action Buttons */}
        <div className="overlay-toolbar">
          <button
            type="button"
            className="overlay-icon-btn"
            onClick={() => {
              setShowQuickInput((v) => !v);
              setTimeout(() => inputRef.current?.focus(), 50);
            }}
            title="ミニ入力欄の表示/非表示"
          >
            💬
          </button>
          <button
            type="button"
            className="overlay-icon-btn"
            onClick={handleToggleMain}
            title="チャット履歴・接続設定ウィンドウを開く"
          >
            📜 履歴・接続
          </button>
          <button
            type="button"
            className="overlay-icon-btn danger"
            onClick={handleQuit}
            title="アプリを終了"
          >
            ✕
          </button>
        </div>

        {/* Quick Reply Input Bar */}
        {showQuickInput && (
          <form className="quick-input-form" onSubmit={handleQuickSend}>
            <input
              ref={inputRef}
              type="text"
              className="quick-input"
              placeholder={
                peer ? `${peer.nickname} へメッセージ...` : "履歴・接続から相手を選択"
              }
              value={quickText}
              onChange={(e) => setQuickText(e.target.value)}
            />
            <button type="submit" className="quick-send-btn" disabled={!peer}>
              送信
            </button>
          </form>
        )}
      </div>
    </div>
  );
};
