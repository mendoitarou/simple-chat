export interface PeerInfo {
  nodeId: string;
  nickname: string;
  username?: string | null;
}

export interface ChatMessagePayload {
  id: string;
  senderId: string;
  senderName: string;
  content: string;
  timestamp: number;
  isSelf: boolean;
}

export interface DirectoryUser {
  username: string;
  displayName: string;
  nodeId: string;
  online: boolean;
  lastSeen: number;
}

export interface StateSnapshot {
  nodeId: string;
  ticket: string;
  nickname: string;
  username: string | null;
  serverUrl: string;
  registeredOnServer: boolean;
  connectedPeer: PeerInfo | null;
  pendingRequest: PeerInfo | null;
  messages: ChatMessagePayload[];
}

export type P2PEvent =
  | {
      type: "connection-requested";
      peer: PeerInfo;
    }
  | {
      type: "peer-connected";
      peer: PeerInfo;
    }
  | {
      type: "peer-disconnected";
      reason?: string;
    }
  | {
      type: "chat-message";
      message: ChatMessagePayload;
    }
  | {
      type: "state-updated";
      snapshot: StateSnapshot;
    };
