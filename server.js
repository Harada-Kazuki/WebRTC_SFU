// server.js - SFU (Selective Forwarding Unit) 実装
import express from "express";
import { WebSocketServer } from "ws";
import http from "http";
import path from "path";
import { fileURLToPath } from "url";
import { randomUUID } from "crypto";
import wrtc from "@roamhq/wrtc";

const { RTCPeerConnection, RTCSessionDescription, RTCIceCandidate } = wrtc;

const __filename = fileURLToPath(import.meta.url);
const __dirname  = path.dirname(__filename);

const app    = express();
const server = http.createServer(app);
const wss    = new WebSocketServer({ server, perMessageDeflate: false, maxPayload: 10 * 1024 * 1024 });

app.use(express.static(path.join(__dirname, "public")));
app.get("/",       (_, res) => res.sendFile(path.join(__dirname, "public", "index.html")));
app.get("/health", (_, res) => res.json({
  uptime: process.uptime(),
  broadcaster: broadcasterPeer ? "connected" : "disconnected",
  viewers: viewerPeers.size,
  memory: process.memoryUsage()
}));
app.get("/ping", (_, res) => res.send("pong"));

// ─── 状態管理 ─────────────────────────────────────────────────
let broadcasterPeer = null;  // { ws, pc, videoTrack }
let broadcasterWs   = null;

const viewerPeers      = new Map(); // viewerId → { ws, pc }
const pendingCandidates = new Map(); // viewerId → RTCIceCandidate[]

const ICE_SERVERS = [
  { urls: "stun:stun.l.google.com:19302" },
  { urls: "stun:stun1.l.google.com:19302" },
];

const PING_INTERVAL       = 20000;
const BROADCASTER_TIMEOUT = 30000;
let broadcasterTimeoutTimer = null;

// ─── ログ ──────────────────────────────────────────────────────
function log(msg, level = "info") {
  const p = { info: "ℹ️", warn: "⚠️", error: "❌", success: "✅" }[level] || "ℹ️";
  console.log(`[${new Date().toISOString()}] ${p} ${msg}`);
}

// ─── WebSocket 送信ヘルパー ─────────────────────────────────────
function safeSend(ws, data) {
  if (ws && ws.readyState === 1) {
    try { ws.send(JSON.stringify(data)); return true; }
    catch (e) { log(`send error: ${e.message}`, "error"); }
  }
  return false;
}

// ─── 視聴者数を配信者に通知 ────────────────────────────────────
function notifyViewerCount() {
  safeSend(broadcasterWs, { type: "viewerCount", count: viewerPeers.size });
}

// ─── 全視聴者に通知 ────────────────────────────────────────────
function notifyAllViewers(data) {
  for (const [, peer] of viewerPeers) safeSend(peer.ws, data);
}

// ─── 視聴者 PeerConnection を作成して Offer 送信 ───────────────
async function createViewerPeer(viewerId, ws) {
  cleanupViewer(viewerId);
  log(`Creating viewer peer: ${viewerId}`);

  const pc = new RTCPeerConnection({ iceServers: ICE_SERVERS });
  viewerPeers.set(viewerId, { ws, pc });

  // 配信者のトラックが既にあれば追加
  // ※ addTrack でなく addTransceiver(track) を使って方向を明示
  if (broadcasterPeer?.videoTrack) {
    pc.addTransceiver(broadcasterPeer.videoTrack, { direction: "sendonly" });
    log(`Added existing video track to viewer ${viewerId}`);
  } else {
    // トラックがまだ来ていない場合もsendonly transceiver を予約して
    // 後から replaceTrack で差し込む
    pc.addTransceiver("video", { direction: "sendonly" });
  }

  pc.onicecandidate = ({ candidate }) => {
    if (candidate) safeSend(ws, { type: "candidate", candidate });
  };

  pc.oniceconnectionstatechange = () => {
    log(`Viewer ${viewerId} ICE: ${pc.iceConnectionState}`);
    if (pc.iceConnectionState === "failed") restartViewerIce(viewerId);
  };

  pc.onconnectionstatechange = () => {
    log(`Viewer ${viewerId} conn: ${pc.connectionState}`);
    if (pc.connectionState === "failed" || pc.connectionState === "closed") {
      cleanupViewer(viewerId);
    }
  };

  await sendOfferToViewer(viewerId);
}

// ─── Offer を視聴者へ送る ──────────────────────────────────────
async function sendOfferToViewer(viewerId) {
  const peer = viewerPeers.get(viewerId);
  if (!peer) return;
  try {
    const offer = await peer.pc.createOffer();
    await peer.pc.setLocalDescription(offer);
    safeSend(peer.ws, { type: "offer", offer: peer.pc.localDescription });
    log(`Offer sent to viewer ${viewerId}`, "success");
  } catch (e) {
    log(`createOffer error [${viewerId}]: ${e.message}`, "error");
    cleanupViewer(viewerId);
  }
}

// ─── ICE Restart ───────────────────────────────────────────────
async function restartViewerIce(viewerId) {
  const peer = viewerPeers.get(viewerId);
  if (!peer) return;
  try {
    const offer = await peer.pc.createOffer({ iceRestart: true });
    await peer.pc.setLocalDescription(offer);
    safeSend(peer.ws, { type: "offer", offer: peer.pc.localDescription });
    log(`ICE restart offer sent to ${viewerId}`);
  } catch (e) {
    log(`ICE restart error [${viewerId}]: ${e.message}`, "error");
  }
}

// ─── 視聴者クリーンアップ ──────────────────────────────────────
function cleanupViewer(viewerId) {
  const peer = viewerPeers.get(viewerId);
  if (!peer) return;
  try { peer.pc.close(); } catch (e) {}
  viewerPeers.delete(viewerId);
  pendingCandidates.delete(viewerId);
  log(`Viewer ${viewerId} cleaned up (remaining: ${viewerPeers.size})`);
  notifyViewerCount();
}

// ─── 配信者クリーンアップ ──────────────────────────────────────
function cleanupBroadcaster() {
  if (!broadcasterPeer) return;
  try { broadcasterPeer.pc.close(); } catch (e) {}
  broadcasterPeer = null;
  broadcasterWs   = null;
  log("Broadcaster cleaned up", "warn");
}

// ─── 配信者 PC を作成（サーバーが受信側） ──────────────────────
async function createBroadcasterPeer(ws) {
  cleanupBroadcaster();
  log("Creating broadcaster peer (server-side receiver)");

  const pc = new RTCPeerConnection({ iceServers: ICE_SERVERS });
  broadcasterPeer = { ws, pc, videoTrack: null };
  broadcasterWs   = ws;

  // ★ ここが重要: recvonly transceiver を追加することで
  //    ブラウザからの Offer に video m-line が含まれ ontrack が発火する
  pc.addTransceiver("video", { direction: "recvonly" });

  pc.ontrack = ({ track }) => {
    log(`Broadcaster track received: ${track.kind}`, "success");
    if (track.kind !== "video") return;

    broadcasterPeer.videoTrack = track;

    // 既存の全視聴者のトランシーバーにトラックを差し込む
    for (const [viewerId, peer] of viewerPeers) {
      const senders = peer.pc.getSenders();
      const sender  = senders.find(s => s.track === null || (s.track && s.track.kind === "video"));
      if (sender) {
        sender.replaceTrack(track).then(() => {
          log(`Track forwarded to viewer ${viewerId}`, "success");
          // replaceTrack後は再ネゴシエーション不要（同一 transceiver で更新）
        }).catch(e => log(`replaceTrack error [${viewerId}]: ${e.message}`, "error"));
      }
    }

    track.onended = () => {
      log("Broadcaster video track ended", "warn");
      if (broadcasterPeer) broadcasterPeer.videoTrack = null;
    };
  };

  pc.onicecandidate = ({ candidate }) => {
    if (candidate) safeSend(ws, { type: "candidate", candidate });
  };

  pc.oniceconnectionstatechange = () => log(`Broadcaster ICE: ${pc.iceConnectionState}`);
  pc.onconnectionstatechange = () => {
    log(`Broadcaster conn: ${pc.connectionState}`);
    if (pc.connectionState === "failed") {
      notifyAllViewers({ type: "broadcasterDisconnected", permanent: false });
    }
  };
}

// ─── WebSocket 接続処理 ────────────────────────────────────────
wss.on("connection", (ws, req) => {
  const ip = req.headers["x-forwarded-for"] || req.socket.remoteAddress;
  log(`New WS connection from ${ip}`);

  let role     = null;
  let viewerId = null;
  const pingTimer = setInterval(() => {
    if (ws.readyState === 1) ws.ping(); else clearInterval(pingTimer);
  }, PING_INTERVAL);

  ws.on("message", async (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch (e) { return; }

    // ── 配信者登録 ────────────────────────────────────────────
    if (msg.type === "register" && msg.role === "broadcaster") {
      role = "broadcaster";
      if (broadcasterTimeoutTimer) { clearTimeout(broadcasterTimeoutTimer); broadcasterTimeoutTimer = null; }
      log("Broadcaster registering...");
      await createBroadcasterPeer(ws);
      safeSend(ws, { type: "registered", role: "broadcaster", viewerCount: viewerPeers.size });
      return;
    }

    // ── 視聴者登録 ────────────────────────────────────────────
    if (msg.type === "register" && msg.role === "viewer") {
      role     = "viewer";
      viewerId = msg.viewerId || randomUUID();
      log(`Viewer registering: ${viewerId}`);
      await createViewerPeer(viewerId, ws);
      safeSend(ws, { type: "registered", role: "viewer", viewerId });
      notifyViewerCount();
      if (!broadcasterPeer) safeSend(ws, { type: "broadcasterDisconnected", permanent: false });
      return;
    }

    // ── 配信者 → サーバー: Offer ──────────────────────────────
    if (msg.type === "offer" && role === "broadcaster") {
      if (!broadcasterPeer) return;
      const pc = broadcasterPeer.pc;
      try {
        await pc.setRemoteDescription(new RTCSessionDescription(msg.offer));
        const answer = await pc.createAnswer();
        await pc.setLocalDescription(answer);
        safeSend(ws, { type: "answer", answer: pc.localDescription });
        log("Broadcaster offer handled, answer sent", "success");
      } catch (e) {
        log(`Broadcaster offer error: ${e.message}`, "error");
      }
      return;
    }

    // ── 視聴者 → サーバー: Answer ─────────────────────────────
    if (msg.type === "answer" && role === "viewer") {
      const peer = viewerPeers.get(viewerId);
      if (!peer) return;
      try {
        await peer.pc.setRemoteDescription(new RTCSessionDescription(msg.answer));
        log(`Viewer ${viewerId} answer set`, "success");

        // 蓄積 ICE 候補を適用
        const pending = pendingCandidates.get(viewerId) || [];
        for (const c of pending) {
          try { await peer.pc.addIceCandidate(new RTCIceCandidate(c)); } catch (e) {}
        }
        pendingCandidates.delete(viewerId);

        // answer 受理後にトラックがあれば replaceTrack で差し込む
        if (broadcasterPeer?.videoTrack) {
          const sender = peer.pc.getSenders().find(s => s.track === null || (s.track && s.track.kind === "video"));
          if (sender) {
            sender.replaceTrack(broadcasterPeer.videoTrack)
              .then(() => log(`Late track forwarded to viewer ${viewerId}`, "success"))
              .catch(e => log(`Late replaceTrack error: ${e.message}`, "error"));
          }
        }
      } catch (e) {
        log(`Viewer answer error [${viewerId}]: ${e.message}`, "error");
      }
      return;
    }

    // ── ICE 候補 ──────────────────────────────────────────────
    if (msg.type === "candidate") {
      if (role === "broadcaster" && broadcasterPeer) {
        try { await broadcasterPeer.pc.addIceCandidate(new RTCIceCandidate(msg.candidate)); } catch (e) {}
      } else if (role === "viewer" && viewerId) {
        const peer = viewerPeers.get(viewerId);
        if (peer?.pc.remoteDescription) {
          try { await peer.pc.addIceCandidate(new RTCIceCandidate(msg.candidate)); } catch (e) {}
        } else {
          if (!pendingCandidates.has(viewerId)) pendingCandidates.set(viewerId, []);
          pendingCandidates.get(viewerId).push(msg.candidate);
        }
      }
      return;
    }
  });

  ws.on("close", () => {
    clearInterval(pingTimer);
    log(`WS closed (role: ${role}, id: ${viewerId || "n/a"})`);

    if (role === "broadcaster") {
      cleanupBroadcaster();
      broadcasterTimeoutTimer = setTimeout(() => {
        log("Broadcaster timeout - notifying viewers", "warn");
        notifyAllViewers({ type: "broadcasterDisconnected", permanent: false });
      }, BROADCASTER_TIMEOUT);
    } else if (role === "viewer" && viewerId) {
      cleanupViewer(viewerId);
    }
  });

  ws.on("error", (e) => log(`WS error: ${e.message}`, "error"));
});

// ─── 定期ステータスログ ────────────────────────────────────────
setInterval(() => {
  log(`Status: Broadcaster=${broadcasterPeer ? "ON" : "OFF"}, Viewers=${viewerPeers.size}`);
}, 60000);

// ─── メモリ監視 ────────────────────────────────────────────────
setInterval(() => {
  const u    = process.memoryUsage();
  const heap = (u.heapUsed / 1024 / 1024).toFixed(1);
  const tot  = (u.heapTotal / 1024 / 1024).toFixed(1);
  log(`Memory: ${heap}MB / ${tot}MB`);
  if (u.heapUsed / u.heapTotal > 0.85 && global.gc) { global.gc(); log("GC triggered"); }
}, 300000);

// ─── サーバー起動 ──────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => log(`SFU Server running on port ${PORT}`, "success"));

// ─── Graceful shutdown ─────────────────────────────────────────
const shutdown = () => {
  log("Shutting down...");
  notifyAllViewers({ type: "broadcasterDisconnected", permanent: true });
  cleanupBroadcaster();
  viewerPeers.forEach((_, id) => cleanupViewer(id));
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(1), 10000);
};
process.on("SIGTERM", shutdown);
process.on("SIGINT",  shutdown);
process.on("uncaughtException",  (e) => log(`Uncaught: ${e.message}\n${e.stack}`, "error"));
process.on("unhandledRejection", (r) => log(`UnhandledRejection: ${r}`, "error"));
