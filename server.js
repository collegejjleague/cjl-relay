const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;

// Track connected clients per mat channel
const channels = {};

// =========================================================================
// CENTRALIZED DISASTER RECOVERY MEMORY LAYER
// Keeps the absolute latest state of each mat cached in server RAM.
// Only replayed to newly-joined clients if it's still fresh (see TTL below) —
// otherwise leftover state from an earlier session/tournament could get
// replayed into a brand-new one just because the relay never restarted.
// =========================================================================
const stateCache = {};
const STATE_CACHE_TTL_MS = 2 * 60 * 1000; // 2 minutes — covers a real refresh/reconnect; anything older is a different session

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('CJL Relay OK');
});

const wss = new WebSocketServer({ server });

wss.on('connection', (ws) => {
  let joinedChannel = null;

  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    if (msg.type === 'join') {
      const ch = msg.channel || ('mat_' + msg.mat);
      joinedChannel = ch;
      if (!channels[ch]) channels[ch] = new Set();
      channels[ch].add(ws);
      
      ws.send(JSON.stringify({ type: 'joined', channel: ch }));

      // =========================================================================
      // PUSH CACHED STATE TO NEWLY CONNECTED COMPUTER — ONLY IF STILL FRESH
      // If a second control panel or scoreboard opens, immediately feed it the
      // truth — but only if that cached truth is recent. Stale entries are
      // dropped instead of replayed.
      // =========================================================================
      const cached = stateCache[ch];
      if (cached) {
        if (Date.now() - cached.ts <= STATE_CACHE_TTL_MS) {
          ws.send(JSON.stringify({ type: 'sync_state', state: cached.msg }));
        } else {
          delete stateCache[ch];
        }
      }

    } else if (msg.type === 'state') {
      const ch = 'mat_' + msg.mat;
      
      // Save the state to server memory, tagged with when it arrived
      stateCache[ch] = { msg, ts: Date.now() };

      if (!channels[ch]) return;
      const payload = JSON.stringify(msg);
      channels[ch].forEach(client => {
        if (client !== ws && client.readyState === 1) {
          client.send(payload);
        }
      });

    } else if (msg.type === 'bracket') {
      const ch = 'bracket';
      if (!channels[ch]) return;
      const payload = JSON.stringify(msg);
      channels[ch].forEach(client => {
        if (client !== ws && client.readyState === 1) {
          client.send(payload);
        }
      });

    } else if (msg.type === 'ping') {
      ws.send(JSON.stringify({ type: 'pong' }));
    }
  });

  ws.on('close', () => {
    if (joinedChannel && channels[joinedChannel]) {
      channels[joinedChannel].delete(ws);
    }
  });

  ws.on('error', () => {
    if (joinedChannel && channels[joinedChannel]) {
      channels[joinedChannel].delete(ws);
    }
  });
});

const heartbeat = setInterval(() => {
  wss.clients.forEach(ws => {
    if (ws.isAlive === false) {
      ws.terminate();
      return;
    }
    ws.isAlive = false;
    ws.ping();
  });
}, 25000);

wss.on('close', () => {
  clearInterval(heartbeat);
});

server.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
