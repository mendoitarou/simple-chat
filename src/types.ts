export interface PeerInfo {
  nodeId: string;
  nickname: string;
  username?: string | null;
}

export interface PendingRequestInfo {
  peer: PeerInfo;
  introducedBy?: string | null;
  roomName?: string | null;
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

export interface GroupRoomMember {
  nodeId: string;
  nickname: string;
  username?: string | null;
  ticket: string;
  lastSeen: number;
}

export interface GroupRoom {
  roomId: string;
  name: string;
  hostNodeId: string;
  hostName: string;
  members: GroupRoomMember[];
}

export interface StateSnapshot {
  nodeId: string;
  ticket: string;
  nickname: string;
  username: string | null;
  serverUrl: string;
  registeredOnServer: boolean;
  roomId: string | null;
  roomName: string | null;
  connectedPeer: PeerInfo | null;
  connectedPeers: PeerInfo[];
  pendingRequest: PeerInfo | null;
  pendingRequests: PendingRequestInfo[];
  messages: ChatMessagePayload[];
}

export type P2PEvent =
  | {
      type: "connection-requested";
      peer: PeerInfo;
      introducedBy?: string | null;
      roomName?: string | null;
    }
  | {
      type: "peer-connected";
      peer: PeerInfo;
    }
  | {
      type: "peer-disconnected";
      peer?: PeerInfo | null;
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
