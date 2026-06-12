/**
 * Tafil Signaling Server — y-webrtc compatible WebSocket relay.
 *
 * Protocol:
 *   { type: 'subscribe', topics: string[] }   → join rooms
 *   { type: 'unsubscribe', topics: string[] } → leave rooms
 *   { type: 'publish', topic: string, data: any } → relay to room peers
 *   { type: 'ping' }                          → responds with { type: 'pong' }
 *
 * Deployment: Render.com free tier (or any Node.js host with WebSocket support)
 * Health check: GET / → 200 OK (required by Render)
 */

const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 4444;

// ─── Topic Registry ───────────────────────────────────────────────────────
// topic → Set<WebSocket>
const topics = new Map();

function getTopicSubscribers(topic) {
  if (!topics.has(topic)) {
    topics.set(topic, new Set());
  }
  return topics.get(topic);
}

function removeFromAllTopics(ws) {
  if (!ws.subscribedTopics) return;
  for (const topic of ws.subscribedTopics) {
    const subscribers = topics.get(topic);
    if (subscribers) {
      subscribers.delete(ws);
      if (subscribers.size === 0) {
        topics.delete(topic);
      }
    }
  }
  ws.subscribedTopics.clear();
}

// ─── HTTP Server (health check for Render) ────────────────────────────────
const server = http.createServer((req, res) => {
  if (req.method === 'GET' && (req.url === '/' || req.url === '/health')) {
    const stats = {
      status: 'ok',
      uptime: Math.floor(process.uptime()),
      connections: wss ? wss.clients.size : 0,
      topics: topics.size,
      timestamp: new Date().toISOString(),
    };
    res.writeHead(200, {
      'Content-Type': 'application/json',
      'Access-Control-Allow-Origin': '*',
    });
    res.end(JSON.stringify(stats));
    return;
  }

  // CORS preflight
  if (req.method === 'OPTIONS') {
    res.writeHead(204, {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, OPTIONS',
      'Access-Control-Allow-Headers': '*',
    });
    res.end();
    return;
  }

  res.writeHead(404);
  res.end('Not found');
});

// ─── WebSocket Server ─────────────────────────────────────────────────────
const wss = new WebSocketServer({
  server,
  path: '/ws/signaling',
  maxPayload: 1024 * 1024, // 1MB max message size
});

// Ping/pong keepalive — detect dead connections
const KEEPALIVE_INTERVAL = 30_000;
const keepaliveTimer = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) {
      console.log('[Signaling] Dead connection detected, terminating');
      removeFromAllTopics(ws);
      return ws.terminate();
    }
    ws.isAlive = false;
    ws.ping();
  });
}, KEEPALIVE_INTERVAL);

wss.on('close', () => {
  clearInterval(keepaliveTimer);
});

wss.on('connection', (ws, req) => {
  ws.isAlive = true;
  ws.subscribedTopics = new Set();

  const clientIp = req.headers['x-forwarded-for'] || req.socket.remoteAddress;
  console.log(`[Signaling] Client connected from ${clientIp} (total: ${wss.clients.size})`);

  ws.on('pong', () => {
    ws.isAlive = true;
  });

  ws.on('message', (rawMessage) => {
    let message;
    try {
      message = typeof rawMessage === 'string'
        ? JSON.parse(rawMessage)
        : JSON.parse(rawMessage.toString());
    } catch {
      // Not JSON — ignore (binary frames from WebRTC go peer-to-peer, not through signaling)
      return;
    }

    if (!message || !message.type) return;

    switch (message.type) {
      case 'subscribe': {
        const subTopics = message.topics || [];
        for (const topic of subTopics) {
          const subscribers = getTopicSubscribers(topic);
          subscribers.add(ws);
          ws.subscribedTopics.add(topic);
        }
        break;
      }

      case 'unsubscribe': {
        const unsubTopics = message.topics || [];
        for (const topic of unsubTopics) {
          const subscribers = topics.get(topic);
          if (subscribers) {
            subscribers.delete(ws);
            if (subscribers.size === 0) {
              topics.delete(topic);
            }
          }
          ws.subscribedTopics.delete(topic);
        }
        break;
      }

      case 'publish': {
        const { topic } = message;
        if (!topic) break;

        const subscribers = topics.get(topic);
        if (!subscribers) break;

        const payload = JSON.stringify(message);
        for (const receiver of subscribers) {
          if (receiver !== ws && receiver.readyState === 1 /* OPEN */) {
            try {
              receiver.send(payload);
            } catch {
              // Connection broken — will be cleaned up by keepalive
            }
          }
        }
        break;
      }

      case 'ping': {
        try {
          ws.send(JSON.stringify({ type: 'pong' }));
        } catch {
          // ignore
        }
        break;
      }
    }
  });

  ws.on('close', () => {
    removeFromAllTopics(ws);
    console.log(`[Signaling] Client disconnected (total: ${wss.clients.size})`);
  });

  ws.on('error', (err) => {
    console.error('[Signaling] WebSocket error:', err.message);
    removeFromAllTopics(ws);
  });
});

// ─── Start ────────────────────────────────────────────────────────────────
server.listen(PORT, () => {
  console.log(`🚀 Tafil Signaling Server running on port ${PORT}`);
  console.log(`   WebSocket: ws://localhost:${PORT}/ws/signaling`);
  console.log(`   Health:    http://localhost:${PORT}/health`);
});

// Graceful shutdown
process.on('SIGTERM', () => {
  console.log('[Signaling] SIGTERM received, shutting down...');
  clearInterval(keepaliveTimer);
  wss.close(() => {
    server.close(() => {
      process.exit(0);
    });
  });
});
