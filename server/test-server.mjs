import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createDirectoryServer, verifyEd25519Signature } from "./index.mjs";

async function runTests() {
  const tempStorage = path.join(
    os.tmpdir(),
    `simple-chat-users-${crypto.randomUUID()}.json`
  );

  // 1. Generate Ed25519 keypair for Alice
  const aliceKeys = crypto.generateKeyPairSync("ed25519");
  const aliceSpki = aliceKeys.publicKey.export({ format: "der", type: "spki" });
  const aliceRawPubHex = aliceSpki.subarray(aliceSpki.length - 32).toString("hex");

  const timestamp = Date.now();
  const payload = `alice:${aliceRawPubHex}:${timestamp}`;
  const sigHex = crypto
    .sign(null, Buffer.from(payload, "utf8"), aliceKeys.privateKey)
    .toString("hex");

  assert.equal(
    verifyEd25519Signature(aliceRawPubHex, payload, sigHex),
    true,
    "Valid Ed25519 signature should pass verification"
  );
  assert.equal(
    verifyEd25519Signature(aliceRawPubHex, payload + "tampered", sigHex),
    false,
    "Tampered message should fail verification"
  );

  // 2. Test HTTP endpoints & disk persistence
  const server = createDirectoryServer(tempStorage);
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const port = server.address().port;
  const baseUrl = `http://127.0.0.1:${port}`;

  try {
    // Register Alice
    const regRes = await fetch(`${baseUrl}/api/register`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        username: "alice",
        displayName: "Alice Wonderland",
        nodeId: aliceRawPubHex,
        publicKeyHex: aliceRawPubHex,
        ticket: "mock-ticket-alice-123",
        timestamp,
        signatureHex: sigHex,
      }),
    });
    assert.equal(regRes.status, 200);
    const regData = await regRes.json();
    assert.equal(regData.ok, true);
    assert.equal(regData.user.username, "alice");

    // Create a group room signed by Alice
    const roomPayload = `room:room-dev-1:${aliceRawPubHex}:${timestamp}`;
    const roomSigHex = crypto
      .sign(null, Buffer.from(roomPayload, "utf8"), aliceKeys.privateKey)
      .toString("hex");
    const roomRes = await fetch(`${baseUrl}/api/rooms`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        roomId: "room-dev-1",
        name: "Dev Lounge",
        nodeId: aliceRawPubHex,
        publicKeyHex: aliceRawPubHex,
        nickname: "Alice Wonderland",
        username: "alice",
        ticket: "mock-ticket-alice-123",
        timestamp,
        signatureHex: roomSigHex,
      }),
    });
    assert.equal(roomRes.status, 200);
    const roomData = await roomRes.json();
    assert.equal(roomData.room.name, "Dev Lounge");
    assert.equal(roomData.room.members.length, 1);
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }

  // 3. Restart server from the same storage file and verify Alice & room are still bound
  const server2 = createDirectoryServer(tempStorage);
  await new Promise((resolve) => server2.listen(0, "127.0.0.1", resolve));
  const port2 = server2.address().port;
  const baseUrl2 = `http://127.0.0.1:${port2}`;

  try {
    // Lookup Alice after server restart
    const lookupRes = await fetch(`${baseUrl2}/api/users/alice`);
    assert.equal(lookupRes.status, 200);
    const lookupData = await lookupRes.json();
    assert.equal(lookupData.nodeId, aliceRawPubHex);
    assert.equal(lookupData.ticket, "mock-ticket-alice-123");

    // Verify group room persisted across restart
    const roomsRes = await fetch(`${baseUrl2}/api/rooms`);
    assert.equal(roomsRes.status, 200);
    const roomsData = await roomsRes.json();
    assert.equal(roomsData.rooms.length, 1);
    assert.equal(roomsData.rooms[0].name, "Dev Lounge");

    // Leave room and verify auto-cleanup when 0 members remain
    const leavePayload = `leave:room-dev-1:${aliceRawPubHex}:${timestamp}`;
    const leaveSigHex = crypto
      .sign(null, Buffer.from(leavePayload, "utf8"), aliceKeys.privateKey)
      .toString("hex");
    const leaveRes = await fetch(`${baseUrl2}/api/rooms/room-dev-1/leave`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        nodeId: aliceRawPubHex,
        publicKeyHex: aliceRawPubHex,
        timestamp,
        signatureHex: leaveSigHex,
      }),
    });
    assert.equal(leaveRes.status, 200);

    const roomsAfterLeave = await (await fetch(`${baseUrl2}/api/rooms`)).json();
    assert.equal(roomsAfterLeave.rooms.length, 0);

    // Attempt hijacking "alice" after server restart with a different keypair (Mallory)
    const malloryKeys = crypto.generateKeyPairSync("ed25519");
    const mallorySpki = malloryKeys.publicKey.export({ format: "der", type: "spki" });
    const malloryRawPubHex = mallorySpki
      .subarray(mallorySpki.length - 32)
      .toString("hex");
    const malloryPayload = `alice:${malloryRawPubHex}:${timestamp}`;
    const mallorySigHex = crypto
      .sign(null, Buffer.from(malloryPayload, "utf8"), malloryKeys.privateKey)
      .toString("hex");

    const hijackRes = await fetch(`${baseUrl2}/api/register`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        username: "alice",
        displayName: "Mallory Fake",
        nodeId: malloryRawPubHex,
        publicKeyHex: malloryRawPubHex,
        ticket: "fake-ticket",
        timestamp,
        signatureHex: mallorySigHex,
      }),
    });
    assert.equal(
      hijackRes.status,
      403,
      "Hijacking an existing username after server restart must still return 403"
    );

    console.log(
      "[test-server] All directory, Ed25519 verification & disk persistence tests passed!"
    );
  } finally {
    await new Promise((resolve) => server2.close(resolve));
    fs.rmSync(tempStorage, { force: true });
    fs.rmSync(`${tempStorage}.rooms.json`, { force: true });
  }
}

runTests().catch((err) => {
  console.error(err);
  process.exit(1);
});
