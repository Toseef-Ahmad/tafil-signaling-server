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

// ─── Security Limits ──────────────────────────────────────────────────────
const MAX_MESSAGES_PER_SECOND = 30;       // Sliding window rate limit
const MAX_RATE_LIMIT_STRIKES = 3;         // Consecutive violations before disconnect
const MAX_TOPICS_PER_CLIENT = 5;          // Max topic subscriptions per client
const MAX_CONNECTIONS_PER_IP = 10;         // Max concurrent connections per IP
const MAX_PUBLISH_PAYLOAD_BYTES = 512 * 1024; // 512KB application-level message limit

// ─── Per-IP Connection Tracking ───────────────────────────────────────────
/** @type {Map<string, number>} */
const connectionsByIp = new Map();

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

// ─── Rate Limiting ────────────────────────────────────────────────────────
/**
 * Check if a client has exceeded the message rate limit.
 * Uses a sliding 1-second window: resets the counter when the window expires.
 * Returns true if the message should be DROPPED.
 */
function isRateLimited(ws) {
  const now = Date.now();

  // Reset the window if 1 second has elapsed
  if (now - ws.messageWindowStart >= 1000) {
    ws.messageWindowStart = now;
    ws.messageCount = 0;
    // Reset consecutive strikes on a clean window
    ws.rateLimitStrikes = ws._wasLimitedLastWindow ? ws.rateLimitStrikes : 0;
    ws._wasLimitedLastWindow = false;
  }

  ws.messageCount++;

  if (ws.messageCount > MAX_MESSAGES_PER_SECOND) {
    ws._wasLimitedLastWindow = true;
    ws.rateLimitStrikes++;
    console.warn(
      `[Signaling] Rate limit exceeded for client (${ws.messageCount} msgs/s, strike ${ws.rateLimitStrikes}/${MAX_RATE_LIMIT_STRIKES})`
    );

    // Disconnect after 3 consecutive violations
    if (ws.rateLimitStrikes >= MAX_RATE_LIMIT_STRIKES) {
      console.warn('[Signaling] Client exceeded rate limit 3 times in a row, disconnecting');
      ws.close(1008, 'Rate limit exceeded');
    }

    return true; // Drop the message
  }

  return false; // Message is OK
}

// ─── HTTP Server (health check for Render) ────────────────────────────────
const server = http.createServer((req, res) => {
  if (req.method === 'GET' && (req.url === '/' || req.url === '/health')) {
    const stats = {
      status: 'ok',
      uptime: Math.floor(process.uptime()),
      connections: wss ? wss.clients.size : 0,
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
  const clientIp = req.headers['x-forwarded-for'] || req.socket.remoteAddress;

  // ── Per-IP connection limit ─────────────────────────────────────────────
  const currentCount = connectionsByIp.get(clientIp) || 0;
  if (currentCount >= MAX_CONNECTIONS_PER_IP) {
    console.warn(`[Signaling] Connection limit exceeded for IP ${clientIp} (${currentCount}/${MAX_CONNECTIONS_PER_IP})`);
    ws.close(1008, 'Too many connections');
    return;
  }
  connectionsByIp.set(clientIp, currentCount + 1);

  ws.isAlive = true;
  ws.subscribedTopics = new Set();
  ws.clientIp = clientIp; // Store for cleanup on close

  // Rate limiting state
  ws.messageCount = 0;
  ws.messageWindowStart = Date.now();
  ws.rateLimitStrikes = 0;
  ws._wasLimitedLastWindow = false;

  console.log(`[Signaling] Client connected from ${clientIp} (total: ${wss.clients.size})`);

  ws.on('pong', () => {
    ws.isAlive = true;
  });

  ws.on('message', (rawMessage) => {
    // ── Rate limiting check ───────────────────────────────────────────────
    if (isRateLimited(ws)) return;

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
          // ── Per-client topic limit ────────────────────────────────────────
          if (ws.subscribedTopics.size >= MAX_TOPICS_PER_CLIENT) {
            console.warn(
              `[Signaling] Client exceeded max topic limit (${MAX_TOPICS_PER_CLIENT}), ignoring subscription to "${topic}"`
            );
            break;
          }
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

        // ── Application-level message size validation ─────────────────────
        if (payload.length > MAX_PUBLISH_PAYLOAD_BYTES) {
          console.warn(
            `[Signaling] Publish payload too large (${payload.length} bytes, limit ${MAX_PUBLISH_PAYLOAD_BYTES}), dropping`
          );
          break;
        }

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

    // ── Decrement per-IP connection count ─────────────────────────────────
    const ip = ws.clientIp;
    if (ip) {
      const count = (connectionsByIp.get(ip) || 1) - 1;
      if (count <= 0) {
        connectionsByIp.delete(ip);
      } else {
        connectionsByIp.set(ip, count);
      }
    }

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
