// server.js - SFU (Selective Forwarding Unit) 実装
import express from "express";
import { WebSocketServer } from "ws";
import http from "http";
import path from "path";
import { fileURLToPath } from "url";
import { randomUUID } from "crypto";
import wrtc from "@roamhq/wrtc";

const { RTCPeerConnection, RTCSessionDescription, RTCIceCandidate, nonstandard } = wrtc;
const { RTCVideoSink, RTCAudioSink } = nonstandard;

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({
  server,
  perMessageDeflate: false,
  maxPayload: 10 * 1024 * 1024
});

app.use(express.static(path.join(__dirname, "public")));
app.get("/", (req, res) => res.sendFile(path.join(__dirname, "public", "index.html")));
app.get("/health", (req, res) => res.json({
  uptime: process.uptime(),
  broadcaster: broadcasterPeer ? "connected" : "disconnected",
  viewers: viewerPeers.size,
  memory: process.memoryUsage()
}));
app.get("/ping", (req, res) => res.send("pong"));

// ─── 状態管理 ────────────────────────────────────────────────
// 配信者側PeerConnection（サーバーが受信側）
let broadcasterPeer = null;       // { ws, pc, videoTrack, audioTrack, sinks }
let broadcasterWs = null;

// 視聴者側PeerConnection（サーバーが送信側）Map<viewerId, { ws, pc }>
const viewerPeers = new Map();

// 蓄積されたICE候補（PC準備前に届いた分）
const pendingCandidates = new Map(); // viewerId → []

const ICE_SERVERS = [
  { urls: "stun:stun.l.google.com:19302" },
  { urls: "stun:stun1.l.google.com:19302" },
];

const PING_INTERVAL = 20000;
const BROADCASTER_TIMEOUT = 30000;
let broadcasterTimeoutTimer = null;

// ─── ログ ─────────────────────────────────────────────────────
function log(msg, level = "info") {
  const prefix = { info: "ℹ️", warn: "⚠️", error: "❌", success: "✅" }[level] || "ℹ️";
  console.log(`[${new Date().toISOString()}] ${prefix} ${msg}`);
}

// ─── WebSocket送信ヘルパー ────────────────────────────────────
function safeSend(ws, data) {
  if (ws && ws.readyState === 1) {
    try { ws.send(JSON.stringify(data)); return true; }
    catch (e) { log(`send error: ${e.message}`, "error"); }
  }
  return false;
}

// ─── 配信者からのトラックを全視聴者に転送 ─────────────────────
function forwardTrackToViewer(viewerId, track) {
  const peer = viewerPeers.get(viewerId);
  if (!peer || !peer.pc) return;

  const senders = peer.pc.getSenders();
  const existing = senders.find(s => s.track && s.track.kind === track.kind);

  if (existing) {
    existing.replaceTrack(track).catch(e =>
      log(`replaceTrack error [${viewerId}]: ${e.message}`, "error")
    );
  } else {
    peer.pc.addTrack(track);
  }
}

// ─── 視聴者PeerConnection作成・Offer送信 ─────────────────────
async function createViewerPeer(viewerId, ws) {
  // 既存があればクリーンアップ
  cleanupViewer(viewerId);

  log(`Creating viewer peer: ${viewerId}`);

  const pc = new RTCPeerConnection({ iceServers: ICE_SERVERS });

  viewerPeers.set(viewerId, { ws, pc });

  // 配信者のトラックが既にあれば追加
  if (broadcasterPeer?.videoTrack) {
    pc.addTrack(broadcasterPeer.videoTrack);
    log(`Added existing video track to viewer ${viewerId}`);
  }

  pc.onicecandidate = ({ candidate }) => {
    if (candidate) {
      safeSend(ws, { type: "candidate", candidate });
    }
  };

  pc.oniceconnectionstatechange = () => {
    log(`Viewer ${viewerId} ICE: ${pc.iceConnectionState}`);
    if (pc.iceConnectionState === "failed") {
      log(`ICE failed for viewer ${viewerId}, restarting`, "warn");
      restartViewerIce(viewerId);
    }
  };

  pc.onconnectionstatechange = () => {
    log(`Viewer ${viewerId} conn: ${pc.connectionState}`);
    if (pc.connectionState === "failed" || pc.connectionState === "closed") {
      cleanupViewer(viewerId);
    }
  };

  try {
    const offer = await pc.createOffer();
    await pc.setLocalDescription(offer);
    safeSend(ws, { type: "offer", offer: pc.localDescription });
    log(`Offer sent to viewer ${viewerId}`, "success");
  } catch (e) {
    log(`createOffer error [${viewerId}]: ${e.message}`, "error");
    cleanupViewer(viewerId);
  }
}

// ─── ICE Restart ─────────────────────────────────────────────
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

// ─── 視聴者数通知 ────────────────────────────────────────────
function broadcastViewerCount() {
  const count = viewerPeers.size;
  if (broadcasterWs && broadcasterWs.readyState === 1) {
    try { broadcasterWs.send(JSON.stringify({ type: "viewerCount", count })); } catch(e) {}
  }
}

// ─── 視聴者クリーンアップ ─────────────────────────────────────
function cleanupViewer(viewerId) {
  const peer = viewerPeers.get(viewerId);
  if (!peer) return;
  try { peer.pc.close(); } catch (e) {}
  viewerPeers.delete(viewerId);
  pendingCandidates.delete(viewerId);
  log(`Viewer ${viewerId} cleaned up (remaining: ${viewerPeers.size})`);
  broadcastViewerCount();
}

// ─── 配信者クリーンアップ ─────────────────────────────────────
function cleanupBroadcaster() {
  if (!broadcasterPeer) return;

  // sinkを停止
  if (broadcasterPeer.sinks) {
    for (const sink of broadcasterPeer.sinks) {
      try { sink.stop(); } catch (e) {}
    }
  }
  try { broadcasterPeer.pc.close(); } catch (e) {}

  broadcasterPeer = null;
  broadcasterWs = null;
  log("Broadcaster cleaned up", "warn");
}

// ─── 配信者PeerConnection作成（サーバーが受信） ───────────────
async function createBroadcasterPeer(ws) {
  cleanupBroadcaster();

  log("Creating broadcaster peer (server-side receiver)");

  const pc = new RTCPeerConnection({ iceServers: ICE_SERVERS });

  // トラック受信時の処理
  pc.ontrack = ({ track, streams }) => {
    log(`Broadcaster track received: ${track.kind}`, "success");

    if (track.kind === "video") {
      broadcasterPeer.videoTrack = track;

      // 既存の全視聴者に転送
      for (const [viewerId] of viewerPeers) {
        forwardTrackToViewer(viewerId, track);
        renegotiateWithViewer(viewerId);
      }

      track.onended = () => {
        log("Broadcaster video track ended", "warn");
        broadcasterPeer && (broadcasterPeer.videoTrack = null);
      };
    }
  };

  pc.onicecandidate = ({ candidate }) => {
    if (candidate) safeSend(ws, { type: "candidate", candidate });
  };

  pc.oniceconnectionstatechange = () => {
    log(`Broadcaster ICE: ${pc.iceConnectionState}`);
  };

  pc.onconnectionstatechange = () => {
    log(`Broadcaster conn: ${pc.connectionState}`);
    if (pc.connectionState === "failed") {
      log("Broadcaster connection failed", "error");
      notifyAllViewers({ type: "broadcasterDisconnected", permanent: false });
    }
  };

  broadcasterPeer = { ws, pc, videoTrack: null, sinks: [] };
  broadcasterWs = ws;

  return pc;
}

// ─── 視聴者との再ネゴシエーション ────────────────────────────
async function renegotiateWithViewer(viewerId) {
  const peer = viewerPeers.get(viewerId);
  if (!peer) return;

  try {
    const offer = await peer.pc.createOffer();
    await peer.pc.setLocalDescription(offer);
    safeSend(peer.ws, { type: "offer", offer: peer.pc.localDescription });
    log(`Renegotiation offer sent to ${viewerId}`);
  } catch (e) {
    log(`Renegotiation error [${viewerId}]: ${e.message}`, "error");
  }
}

// ─── 全視聴者に通知 ──────────────────────────────────────────
function notifyAllViewers(data) {
  for (const [, peer] of viewerPeers) {
    safeSend(peer.ws, data);
  }
}

// ─── WebSocket接続処理 ────────────────────────────────────────
wss.on("connection", (ws, req) => {
  const ip = req.headers["x-forwarded-for"] || req.socket.remoteAddress;
  log(`New WS connection from ${ip}`);

  let role = null;
  let viewerId = null;
  let pingTimer = null;

  // Ping/Pong keepalive
  pingTimer = setInterval(() => {
    if (ws.readyState === 1) ws.ping();
    else clearInterval(pingTimer);
  }, PING_INTERVAL);

  ws.on("message", async (raw) => {
    let msg;
    try { msg = JSON.parse(raw); }
    catch (e) { return; }

    // ── 配信者登録 ──────────────────────────────────────────
    if (msg.type === "register" && msg.role === "broadcaster") {
      role = "broadcaster";

      if (broadcasterTimeoutTimer) {
        clearTimeout(broadcasterTimeoutTimer);
        broadcasterTimeoutTimer = null;
      }

      log("Broadcaster registering...");
      const pc = await createBroadcasterPeer(ws);

      // 視聴者からのofferを受け付ける準備が整ったことを通知
      safeSend(ws, { type: "registered", role: "broadcaster", viewerCount: viewerPeers.size });
      return;
    }

    // ── 視聴者登録 ──────────────────────────────────────────
    if (msg.type === "register" && msg.role === "viewer") {
      role = "viewer";
      viewerId = msg.viewerId || randomUUID();

      log(`Viewer registering: ${viewerId}`);
      await createViewerPeer(viewerId, ws);

      safeSend(ws, { type: "registered", role: "viewer", viewerId });
      broadcastViewerCount();

      // 配信者がいなければ通知
      if (!broadcasterPeer) {
        safeSend(ws, { type: "broadcasterDisconnected", permanent: false });
      }
      return;
    }

    // ── 配信者 → サーバー: Offer ────────────────────────────
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

    // ── 視聴者 → サーバー: Answer ───────────────────────────
    if (msg.type === "answer" && role === "viewer") {
      const peer = viewerPeers.get(viewerId);
      if (!peer) return;
      try {
        await peer.pc.setRemoteDescription(new RTCSessionDescription(msg.answer));
        log(`Viewer ${viewerId} answer set`, "success");

        // 蓄積されていたICE候補を適用
        const pending = pendingCandidates.get(viewerId) || [];
        for (const c of pending) {
          try { await peer.pc.addIceCandidate(new RTCIceCandidate(c)); }
          catch (e) {}
        }
        pendingCandidates.delete(viewerId);
      } catch (e) {
        log(`Viewer answer error [${viewerId}]: ${e.message}`, "error");
      }
      return;
    }

    // ── ICE候補 ─────────────────────────────────────────────
    if (msg.type === "candidate") {
      if (role === "broadcaster" && broadcasterPeer) {
        try {
          await broadcasterPeer.pc.addIceCandidate(new RTCIceCandidate(msg.candidate));
        } catch (e) {}
      } else if (role === "viewer" && viewerId) {
        const peer = viewerPeers.get(viewerId);
        if (peer && peer.pc.remoteDescription) {
          try { await peer.pc.addIceCandidate(new RTCIceCandidate(msg.candidate)); }
          catch (e) {}
        } else {
          // Remote Descriptionがまだなければ蓄積
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

      // 一定時間待って視聴者に通知
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

// ─── 定期ステータスログ ───────────────────────────────────────
setInterval(() => {
  log(`Status: Broadcaster=${broadcasterPeer ? "ON" : "OFF"}, Viewers=${viewerPeers.size}`);
}, 60000);

// ─── メモリ監視 ───────────────────────────────────────────────
setInterval(() => {
  const u = process.memoryUsage();
  const heap = (u.heapUsed / 1024 / 1024).toFixed(1);
  const total = (u.heapTotal / 1024 / 1024).toFixed(1);
  log(`Memory: ${heap}MB / ${total}MB`);
  if (u.heapUsed / u.heapTotal > 0.85 && global.gc) {
    global.gc();
    log("GC triggered");
  }
}, 300000);

// ─── サーバー起動 ─────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  log(`SFU Server running on port ${PORT}`, "success");
});

// ─── Graceful shutdown ────────────────────────────────────────
const shutdown = () => {
  log("Shutting down...");
  notifyAllViewers({ type: "broadcasterDisconnected", permanent: true });
  cleanupBroadcaster();
  viewerPeers.forEach((_, id) => cleanupViewer(id));
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(1), 10000);
};
process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
process.on("uncaughtException", (e) => log(`Uncaught: ${e.message}\n${e.stack}`, "error"));
process.on("unhandledRejection", (r) => log(`UnhandledRejection: ${r}`, "error"));