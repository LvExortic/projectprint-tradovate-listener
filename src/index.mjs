import WebSocket from "ws";

const baseUrl = String(process.env.PROJECTPRINT_BASE_URL || "").replace(/\/$/, "");
const secret = String(process.env.TRADOVATE_LISTENER_SECRET || "");
if (!baseUrl || !secret) throw new Error("PROJECTPRINT_BASE_URL and TRADOVATE_LISTENER_SECRET are required.");
const endpoint = `${baseUrl}/api/internal/tradovate/listener`;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
async function api(payload) {
  const response = await fetch(endpoint, {
    method: "POST",
    headers: { "content-type": "application/json", "x-tradovate-listener-secret": secret },
    body: JSON.stringify(payload),
    signal: AbortSignal.timeout(30_000),
  });
  const data = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(data.error || `ProjectPrint listener API returned ${response.status}.`);
  return data;
}

function parseFrame(raw) {
  const text = Buffer.isBuffer(raw) ? raw.toString("utf8") : String(raw);
  if (!text) return null;
  const kind = text[0];
  if (kind === "o" || kind === "h") return { kind, data: null };
  try { return { kind, data: JSON.parse(text.slice(1)) }; } catch { return { kind, data: null }; }
}

async function runConnection(config) {
  await api({ action: "status", status: "connecting" });
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(config.websocketUrl);
    let requestId = 1;
    let authorized = false;
    let syncStarted = false;
    let closed = false;
    const pending = new Map();
    let heartbeatTimer;
    let serverHeartbeatTimer;
    let recycleTimer;

    const sendRequest = (url, query = "", body = null) => {
      const id = requestId++;
      const message = `${url}\n${id}\n${query}\n${body === null ? "" : JSON.stringify(body)}`;
      ws.send(message);
      return new Promise((res, rej) => {
        pending.set(id, { res, rej });
        setTimeout(() => { if (pending.has(id)) { pending.delete(id); rej(new Error(`${url} timed out.`)); } }, 30_000);
      });
    };

    const cleanup = () => {
      clearInterval(heartbeatTimer);
      clearInterval(serverHeartbeatTimer);
      clearTimeout(recycleTimer);
      for (const item of pending.values()) item.rej(new Error("WebSocket disconnected."));
      pending.clear();
    };

    const startSync = async () => {
      if (syncStarted) return;
      syncStarted = true;
      const users = config.userId ? [Number(config.userId)] : [];
      const accounts = Array.isArray(config.accountIds) ? config.accountIds.map(Number) : [];
      const response = await sendRequest("user/syncrequest", "", {
        users,
        accounts,
        cutoffTimestamp: config.cutoffTimestamp,
        splitResponses: false,
      });
      if (response?.d && typeof response.d === "object") await api({ action: "snapshot", snapshot: response.d });
      await api({ action: "status", status: "online" });
    };

    ws.on("open", () => {
      heartbeatTimer = setInterval(() => { if (ws.readyState === WebSocket.OPEN) ws.send("[]"); }, 2500);
      serverHeartbeatTimer = setInterval(() => api({ action: "heartbeat" }).catch(console.error), 30_000);
      recycleTimer = setTimeout(() => ws.close(1000, "Refresh Tradovate access token"), 45 * 60_000);
    });

    ws.on("message", async (raw) => {
      try {
        const frame = parseFrame(raw);
        if (!frame) return;
        if (frame.kind === "o" && !authorized) {
          const id = requestId++;
          const authorization = new Promise((res, rej) => {
            pending.set(id, { res, rej });
            setTimeout(() => { if (pending.has(id)) { pending.delete(id); rej(new Error("authorize timed out.")); } }, 30_000);
          });
          ws.send(`authorize\n${id}\n\n${config.accessToken}`);
          const result = await authorization;
          if (result?.s !== 200) throw new Error(result?.d?.errorText || "Tradovate WebSocket authorization failed.");
          authorized = true;
          await startSync();
          return;
        }
        if (frame.kind !== "a" || !Array.isArray(frame.data)) return;
        for (const message of frame.data) {
          if (message && typeof message.i === "number" && pending.has(message.i)) {
            const waiter = pending.get(message.i); pending.delete(message.i);
            if (message.s >= 200 && message.s < 300) waiter.res(message);
            else waiter.rej(new Error(message?.d?.errorText || `Tradovate request failed (${message.s}).`));
            continue;
          }
          const updates = message?.d;
          if (Array.isArray(updates)) {
            for (const event of updates) if (event?.entityType && event?.entity) await api({ action: "event", event });
          } else if (updates?.entityType && updates?.entity) {
            await api({ action: "event", event: updates });
          }
        }
      } catch (error) {
        console.error(error);
        api({ action: "status", status: "error", error: error.message }).catch(console.error);
        ws.close();
      }
    });

    ws.on("error", (error) => { cleanup(); if (!closed) reject(error); });
    ws.on("close", () => { closed = true; cleanup(); resolve(); });
  });
}

let backoff = 3000;
while (true) {
  try {
    const config = await api({ action: "bootstrap" });
    if (!config.enabled || !config.accessToken || !config.userId || !config.accountIds?.length) {
      await api({ action: "status", status: "offline" }).catch(() => {});
      await sleep(30_000);
      continue;
    }
    await runConnection(config);
    backoff = 3000;
  } catch (error) {
    console.error(new Date().toISOString(), error);
    await api({ action: "status", status: "error", error: error.message }).catch(() => {});
    await sleep(backoff);
    backoff = Math.min(backoff * 2, 60_000);
  }
}
