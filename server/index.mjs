import http from "node:http";
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";

const PORT = Number(process.env.PORT || 3100);
const DATA_FILE =
  process.env.DATA_FILE || path.resolve(process.cwd(), "data", "users.json");
const ONLINE_TTL_MS = 45_000;

// ASN.1 DER SPKI prefix for a 32-byte raw Ed25519 public key (OID 1.3.101.112)
const ED25519_SPKI_PREFIX = Buffer.from("302a300506032b6570032100", "hex");

/**
 * Verify Ed25519 signature using a 32-byte hex-encoded public key.
 */
export function verifyEd25519Signature(publicKeyHex, messageStr, signatureHex) {
  try {
    const rawPub = Buffer.from(publicKeyHex, "hex");
    const sig = Buffer.from(signatureHex, "hex");
    if (rawPub.length !== 32 || sig.length !== 64) {
      return false;
    }
    const derKey = Buffer.concat([ED25519_SPKI_PREFIX, rawPub]);
    const pubKey = crypto.createPublicKey({
      key: derKey,
      format: "der",
      type: "spki",
    });
    return crypto.verify(null, Buffer.from(messageStr, "utf8"), pubKey, sig);
  } catch {
    return false;
  }
}

function loadUsersFromDisk(filePath) {
  const map = new Map();
  try {
    if (fs.existsSync(filePath)) {
      const raw = fs.readFileSync(filePath, "utf8");
      const parsed = JSON.parse(raw);
      if (Array.isArray(parsed)) {
        for (const item of parsed) {
          if (item && typeof item.username === "string") {
            map.set(item.username, item);
          }
        }
      }
    }
  } catch (err) {
    console.error("[DirectoryServer] Failed to load users from disk:", err);
  }
  return map;
}

function saveUsersToDisk(filePath, usersMap) {
  try {
    const dir = path.dirname(filePath);
    fs.mkdirSync(dir, { recursive: true });
    const tempPath = `${filePath}.tmp`;
    const data = JSON.stringify(Array.from(usersMap.values()), null, 2);
    fs.writeFileSync(tempPath, data, "utf8");
    fs.renameSync(tempPath, filePath);
  } catch (err) {
    console.error("[DirectoryServer] Failed to save users to disk:", err);
  }
}

function loadRoomsFromDisk(filePath) {
  const map = new Map();
  try {
    if (fs.existsSync(filePath)) {
      const raw = fs.readFileSync(filePath, "utf8");
      const parsed = JSON.parse(raw);
      if (Array.isArray(parsed)) {
        for (const item of parsed) {
          if (item && typeof item.roomId === "string") {
            map.set(item.roomId, item);
          }
        }
      }
    }
  } catch (err) {
    console.error("[DirectoryServer] Failed to load rooms from disk:", err);
  }
  return map;
}

function saveRoomsToDisk(filePath, roomsMap) {
  try {
    const dir = path.dirname(filePath);
    fs.mkdirSync(dir, { recursive: true });
    const tempPath = `${filePath}.tmp`;
    const data = JSON.stringify(Array.from(roomsMap.values()), null, 2);
    fs.writeFileSync(tempPath, data, "utf8");
    fs.renameSync(tempPath, filePath);
  } catch (err) {
    console.error("[DirectoryServer] Failed to save rooms to disk:", err);
  }
}

function sendJson(res, statusCode, data) {
  const body = JSON.stringify(data);
  res.writeHead(statusCode, {
    "Content-Type": "application/json; charset=utf-8",
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
  });
  res.end(body);
}

function readJsonBody(req) {
  return new Promise((resolve, reject) => {
    let raw = "";
    req.on("data", (chunk) => {
      raw += chunk;
      if (raw.length > 64 * 1024) {
        reject(new Error("Payload too large"));
      }
    });
    req.on("end", () => {
      try {
        resolve(raw ? JSON.parse(raw) : {});
      } catch (e) {
        reject(e);
      }
    });
    req.on("error", reject);
  });
}

export function createDirectoryServer(storageFilePath = DATA_FILE) {
  const roomsFilePath = `${storageFilePath}.rooms.json`;
  const users = loadUsersFromDisk(storageFilePath);
  const rooms = loadRoomsFromDisk(roomsFilePath);

  return http.createServer(async (req, res) => {
    const url = new URL(req.url || "/", `http://${req.headers.host || "localhost"}`);

    if (req.method === "OPTIONS") {
      res.writeHead(204, {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
      });
      res.end();
      return;
    }

    if (req.method === "GET" && url.pathname === "/health") {
      sendJson(res, 200, {
        status: "ok",
        registeredCount: users.size,
        timestamp: Date.now(),
      });
      return;
    }

    // POST /api/register
    // Body: { username, displayName, nodeId, publicKeyHex, ticket, timestamp, signatureHex }
    if (req.method === "POST" && url.pathname === "/api/register") {
      try {
        const body = await readJsonBody(req);
        const rawUsername = String(body.username || "").trim().toLowerCase();
        const displayName = String(body.displayName || rawUsername).trim();
        const nodeId = String(body.nodeId || "").trim();
        const publicKeyHex = String(body.publicKeyHex || nodeId).trim().toLowerCase();
        const ticket = String(body.ticket || "").trim();
        const timestamp = Number(body.timestamp || 0);
        const signatureHex = String(body.signatureHex || "").trim().toLowerCase();

        if (!rawUsername || !/^[a-z0-9_-]{2,24}$/.test(rawUsername)) {
          sendJson(res, 400, {
            error: "Username must be 2-24 characters (a-z, 0-9, _, -)",
          });
          return;
        }
        if (!nodeId || !ticket || !publicKeyHex || !signatureHex) {
          sendJson(res, 400, { error: "Missing required cryptographic fields" });
          return;
        }

        // Verify signed payload: `${rawUsername}:${nodeId}:${timestamp}`
        const signedPayload = `${rawUsername}:${nodeId}:${timestamp}`;
        if (!verifyEd25519Signature(publicKeyHex, signedPayload, signatureHex)) {
          sendJson(res, 401, { error: "Invalid Ed25519 signature" });
          return;
        }

        // Prevent username hijacking (TOFU: Trust On First Use)
        const existing = users.get(rawUsername);
        if (existing && existing.publicKeyHex !== publicKeyHex) {
          sendJson(res, 403, {
            error: "Username is already bound to a different Ed25519 public key",
          });
          return;
        }

        const now = Date.now();
        const record = {
          username: rawUsername,
          displayName: displayName || rawUsername,
          nodeId,
          publicKeyHex,
          ticket,
          lastSeen: now,
        };
        users.set(rawUsername, record);
        saveUsersToDisk(storageFilePath, users);

        sendJson(res, 200, {
          ok: true,
          user: {
            username: record.username,
            displayName: record.displayName,
            nodeId: record.nodeId,
            online: true,
            lastSeen: record.lastSeen,
          },
        });
      } catch (err) {
        sendJson(res, 400, { error: String(err.message || err) });
      }
      return;
    }

    // GET /api/users - List all registered users with online status
    if (req.method === "GET" && url.pathname === "/api/users") {
      const now = Date.now();
      const list = Array.from(users.values()).map((u) => ({
        username: u.username,
        displayName: u.displayName,
        nodeId: u.nodeId,
        online: now - u.lastSeen <= ONLINE_TTL_MS,
        lastSeen: u.lastSeen,
      }));
      sendJson(res, 200, { users: list });
      return;
    }

    // GET /api/users/:username - Lookup user's verified public key & connection ticket
    if (req.method === "GET" && url.pathname.startsWith("/api/users/")) {
      const targetUsername = decodeURIComponent(
        url.pathname.slice("/api/users/".length)
      )
        .trim()
        .toLowerCase();
      const record = users.get(targetUsername);
      if (!record) {
        sendJson(res, 404, { error: `User @${targetUsername} not found` });
        return;
      }
      const now = Date.now();
      sendJson(res, 200, {
        username: record.username,
        displayName: record.displayName,
        nodeId: record.nodeId,
        publicKeyHex: record.publicKeyHex,
        ticket: record.ticket,
        online: now - record.lastSeen <= ONLINE_TTL_MS,
        lastSeen: record.lastSeen,
      });
      return;
    }

    // GET /api/rooms - List active group rooms
    if (req.method === "GET" && url.pathname === "/api/rooms") {
      const list = Array.from(rooms.values())
        .filter((r) => Array.isArray(r.members) && r.members.length > 0)
        .map((r) => ({
          roomId: r.roomId,
          name: r.name,
          hostNodeId: r.hostNodeId,
          hostName: r.hostName,
          hasPassphrase: Boolean(
            r.passphraseHash || String(r.roomId || "").startsWith("room-locked-")
          ),
          createdAt: r.createdAt,
          members: r.members,
        }));
      sendJson(res, 200, { rooms: list });
      return;
    }

    // POST /api/rooms - Create or join/update a group room (Ed25519 signed)
    // Body: { roomId, name, passphraseHash, nodeId, publicKeyHex, nickname, username, ticket, timestamp, signatureHex }
    if (req.method === "POST" && url.pathname === "/api/rooms") {
      try {
        const body = await readJsonBody(req);
        const roomId = String(body.roomId || "").trim();
        const name = String(body.name || "").trim();
        const passphraseHash = body.passphraseHash
          ? String(body.passphraseHash).trim().toLowerCase()
          : null;
        const nodeId = String(body.nodeId || "").trim().toLowerCase();
        const publicKeyHex = String(body.publicKeyHex || nodeId)
          .trim()
          .toLowerCase();
        const nickname = String(body.nickname || "User").trim();
        const username = body.username
          ? String(body.username).trim().toLowerCase()
          : null;
        const ticket = String(body.ticket || "").trim();
        const timestamp = Number(body.timestamp || 0);
        const signatureHex = String(body.signatureHex || "")
          .trim()
          .toLowerCase();

        if (!roomId || !nodeId || !ticket || !signatureHex) {
          sendJson(res, 400, { error: "Missing required room fields" });
          return;
        }

        const signedPayload = `room:${roomId}:${nodeId}:${timestamp}`;
        if (!verifyEd25519Signature(publicKeyHex, signedPayload, signatureHex)) {
          sendJson(res, 401, { error: "Invalid Ed25519 room signature" });
          return;
        }

        const now = Date.now();
        let room = rooms.get(roomId);
        if (!room) {
          room = {
            roomId,
            name: name || `Room-${roomId.slice(0, 6)}`,
            hostNodeId: nodeId,
            hostName: nickname,
            passphraseHash,
            hasPassphrase: Boolean(passphraseHash),
            createdAt: now,
            members: [],
          };
        } else {
          if (room.hostNodeId === nodeId) {
            if (name) room.name = name;
            if (body.passphraseHash !== undefined) {
              room.passphraseHash = passphraseHash;
              room.hasPassphrase = Boolean(passphraseHash);
            }
          } else if (room.passphraseHash && room.passphraseHash !== passphraseHash) {
            sendJson(res, 403, { error: "ルームの合言葉が一致しません" });
            return;
          }
        }

        const memberEntry = {
          nodeId,
          nickname,
          username,
          ticket,
          lastSeen: now,
        };
        const existingIdx = room.members.findIndex((m) => m.nodeId === nodeId);
        if (existingIdx >= 0) {
          room.members[existingIdx] = memberEntry;
        } else {
          room.members.push(memberEntry);
        }

        rooms.set(roomId, room);
        saveRoomsToDisk(roomsFilePath, rooms);
        sendJson(res, 200, {
          ok: true,
          room: {
            roomId: room.roomId,
            name: room.name,
            hostNodeId: room.hostNodeId,
            hostName: room.hostName,
            hasPassphrase: Boolean(room.passphraseHash),
            createdAt: room.createdAt,
            members: room.members,
          },
        });
      } catch (err) {
        sendJson(res, 400, { error: String(err.message || err) });
      }
      return;
    }

    // POST /api/rooms/:roomId/leave - Leave a group room
    if (
      req.method === "POST" &&
      url.pathname.startsWith("/api/rooms/") &&
      url.pathname.endsWith("/leave")
    ) {
      try {
        const roomId = decodeURIComponent(
          url.pathname.slice("/api/rooms/".length, -"/leave".length)
        ).trim();
        const body = await readJsonBody(req);
        const nodeId = String(body.nodeId || "").trim().toLowerCase();
        const publicKeyHex = String(body.publicKeyHex || nodeId)
          .trim()
          .toLowerCase();
        const timestamp = Number(body.timestamp || 0);
        const signatureHex = String(body.signatureHex || "")
          .trim()
          .toLowerCase();

        const signedPayload = `leave:${roomId}:${nodeId}:${timestamp}`;
        if (!verifyEd25519Signature(publicKeyHex, signedPayload, signatureHex)) {
          sendJson(res, 401, { error: "Invalid Ed25519 leave signature" });
          return;
        }

        const room = rooms.get(roomId);
        if (room) {
          room.members = room.members.filter((m) => m.nodeId !== nodeId);
          if (room.members.length === 0) {
            rooms.delete(roomId);
          } else {
            if (room.hostNodeId === nodeId) {
              room.hostNodeId = room.members[0].nodeId;
              room.hostName = room.members[0].nickname;
            }
            rooms.set(roomId, room);
          }
          saveRoomsToDisk(roomsFilePath, rooms);
        }
        sendJson(res, 200, { ok: true });
      } catch (err) {
        sendJson(res, 400, { error: String(err.message || err) });
      }
      return;
    }

    sendJson(res, 404, { error: "Not found" });
  });
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const server = createDirectoryServer(DATA_FILE);
  server.listen(PORT, "0.0.0.0", () => {
    console.log(
      `[DirectoryServer] Listening on http://0.0.0.0:${PORT} (storage: ${DATA_FILE})`
    );
  });
}
