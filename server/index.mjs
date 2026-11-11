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
  const users = loadUsersFromDisk(storageFilePath);

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
