export interface PeerInfo {
  nodeId: string;
  nickname: string;
  username?: string | null;
}

export interface PendingRequestInfo {
  peer: PeerInfo;
  introducedBy?: string | null;
  roomName?: string | null;
  matchedTag?: string | null;
}

export interface ChatMessagePayload {
  id: string;
  senderId: string;
  senderName: string;
  content: string;
  timestamp: number;
  isSelf: boolean;
  channelKind?: string;
  channelId?: string;
  channelName?: string;
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
  hasPassphrase?: boolean;
  members: GroupRoomMember[];
}

export interface StateSnapshot {
  nodeId: string;
  ticket: string;
  nickname: string;
  username: string | null;
  serverUrl: string;
  registeredOnServer: boolean;
  matchingEnabled: boolean;
  matchingTag: string;
  roomId: string | null;
  roomName: string | null;
  roomHasPassphrase?: boolean;
  activeChannelId?: string | null;
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
      matchedTag?: string | null;
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
