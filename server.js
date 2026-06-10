const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;

// Track connected clients per mat channel
// channels: { 'mat_1': Set<ws>, 'mat_2': Set<ws>, ... }
const channels = {};

const server = http.createServer((req, res) => {
  // Health check endpoint — keeps Render happy and lets us wake the server
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('CJL Relay OK');
});

const wss = new WebSocketServer({ server });

wss.on('connection', (ws) => {
  let joinedChannel = null;

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    if (msg.type === 'join') {
      // Client identifies which mat channel to join
      const ch = 'mat_' + msg.mat;
      joinedChannel = ch;
      if (!channels[ch]) channels[ch] = new Set();
      channels[ch].add(ws);
      ws.send(JSON.stringify({ type: 'joined', mat: msg.mat }));

    } else if (msg.type === 'state') {
      // Control panel broadcasting state for a mat
      const ch = 'mat_' + msg.mat;
      if (!channels[ch]) return;
      const payload = JSON.stringify(msg);
      channels[ch].forEach(client => {
        if (client !== ws && client.readyState === 1) {
          client.send(payload);
        }
      });
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

server.listen(PORT, () => {
  console.log('CJL relay server running on port ' + PORT);
});
