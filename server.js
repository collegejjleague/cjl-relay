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

// =========================================================================
// BOUT HISTORY — built entirely from real-time 'state' traffic already
// flowing through this relay. No external API calls of any kind: nothing is
// fetched from Apps Script, Sheets, or anywhere else. When a mat's state
// transitions to a new bout (or its matchResults gets cleared for the next
// bout), whatever completed matches were in the outgoing state get archived
// here in memory and broadcast to anyone subscribed to the 'history' channel.
//
// Trade-off vs. an Apps Script–backed version: this only remembers bouts
// completed since the relay process last started — a relay restart clears
// it. In exchange, there is zero ongoing API/network cost of any kind.
// =========================================================================
const archivedBouts = [];       // most-recent-first array of completed bouts
const HISTORY_MAX_ENTRIES = 50; // safety cap so memory can't grow unbounded over a long event
const HISTORY_MAX_AGE_MS = 24 * 60 * 60 * 1000; // drop entries older than a day

function pruneHistory() {
  const cutoff = Date.now() - HISTORY_MAX_AGE_MS;
  while (archivedBouts.length && archivedBouts[archivedBouts.length - 1].archivedAt < cutoff) {
    archivedBouts.pop();
  }
  if (archivedBouts.length > HISTORY_MAX_ENTRIES) {
    archivedBouts.length = HISTORY_MAX_ENTRIES;
  }
}

function broadcastHistory() {
  const ch = 'history';
  if (!channels[ch]) return;
  const payload = JSON.stringify({ type: 'history_update', bouts: archivedBouts });
  channels[ch].forEach(client => {
    if (client.readyState === 1) client.send(payload);
  });
}

// Detects a bout boundary between an outgoing cached state and an incoming
// one for the same mat, and archives the outgoing bout's completed matches
// if there were any. Mirrors the same transition logic the status page uses
// client-side for the sync_state fix, just applied server-side instead.
function archiveIfBoutEnded(oldState, newState) {
  if (!oldState || !Array.isArray(oldState.matchResults) || oldState.matchResults.length === 0) return;

  const boutChanged = (newState && newState.boutNum || '') !== (oldState.boutNum || '');
  const resultsCleared = newState && Array.isArray(newState.matchResults) && newState.matchResults.length === 0;

  if (!boutChanged && !resultsCleared) return;

  const alreadyArchived = archivedBouts.some(b =>
    b.boutNum === oldState.boutNum && b.school1 === oldState.school1 && b.school2 === oldState.school2
  );
  if (alreadyArchived) return;

  archivedBouts.unshift({
    boutNum: oldState.boutNum || '',
    school1: oldState.school1 || '',
    school2: oldState.school2 || '',
    color1: oldState.color1 || '',
    color2: oldState.color2 || '',
    mode: oldState.mode || '',
    teamPts1: oldState.teamPts1 ?? '',
    teamPts2: oldState.teamPts2 ?? '',
    matches: oldState.matchResults,
    archivedAt: Date.now()
  });

  pruneHistory();
  broadcastHistory();
}

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

      // Newly joined 'history' subscribers get whatever's been archived so far.
      if (ch === 'history') {
        pruneHistory();
        ws.send(JSON.stringify({ type: 'history_update', bouts: archivedBouts }));
      }

    } else if (msg.type === 'state') {
      const ch = 'mat_' + msg.mat;

      // Detect a bout boundary against whatever was cached before, and
      // archive the outgoing bout's completed matches if applicable —
      // purely from data already passing through this relay.
      const previous = stateCache[ch];
      archiveIfBoutEnded(previous ? previous.msg : null, msg);

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
