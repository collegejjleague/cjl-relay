const http = require('http');
const https = require('https');
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
// Latest bracket payload per division (mens_champ, mens_con, womens_champ,
// womens_con, supermatches, ...). Same idea as stateCache above, but keyed by
// division instead of mat — replayed to newly-joined 'bracket' clients so a
// status page opened mid-tournament isn't blank until the next operator push.
const bracketCache = {};

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

// Broadcasts the current count of sockets connected to the 'presence'
// channel to everyone on it — used for the "X watching" indicator on the
// public status page. No external API involved; purely a socket count.
function broadcastPresence() {
  const ch = 'presence';
  const count = channels[ch] ? channels[ch].size : 0;
  if (!channels[ch]) return;
  const payload = JSON.stringify({ type: 'presence_count', count });
  channels[ch].forEach(client => {
    if (client.readyState === 1) client.send(payload);
  });
}


function boutKey(state) {
  return (state.boutNum || '') + '::' + (state.school1 || '') + '::' + (state.school2 || '');
}

// Upserts the live/completed entry for whatever bout this state belongs to.
// Only touches history (and only broadcasts) when the submitted matches for
// that bout actually changed, so a plain timer tick doesn't cause a rebroadcast
// (which would otherwise slam every viewer's open match-history dropdown shut).
function upsertBoutHistory(state) {
  if (!state || !Array.isArray(state.matchResults) || state.matchResults.length === 0) return;

  const key = boutKey(state);
  const idx = archivedBouts.findIndex(b => b.key === key);
  const existing = idx !== -1 ? archivedBouts[idx] : null;

  const matchesChanged = !existing || JSON.stringify(existing.matches) !== JSON.stringify(state.matchResults);
  const teamPtsChanged = !existing || existing.teamPts1 !== (state.teamPts1 ?? '') || existing.teamPts2 !== (state.teamPts2 ?? '');
  if (!matchesChanged && !teamPtsChanged) return;

  const entry = {
    key,
    boutNum: state.boutNum || '',
    school1: state.school1 || '',
    school2: state.school2 || '',
    color1: state.color1 || '',
    color2: state.color2 || '',
    mode: state.mode || '',
    teamPts1: state.teamPts1 ?? '',
    teamPts2: state.teamPts2 ?? '',
    matches: state.matchResults,
    archivedAt: Date.now()
  };

  if (idx !== -1) archivedBouts.splice(idx, 1);
  archivedBouts.unshift(entry);

  pruneHistory();
  broadcastHistory();
}


const server = http.createServer((req, res) => {
  if (req.url === '/clear-history') {
    archivedBouts.length = 0;
    broadcastHistory();
    console.log('Match history cleared via /clear-history (new tournament started).');
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('History cleared');
    return;
  }
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

      // 'presence' is a lightweight viewer-count channel — anyone connected
      // to it is counted as a current page viewer. No external API involved;
      // this is purely a count of open sockets on this one channel.
      if (ch === 'presence') {
        broadcastPresence();
      }

      // Newly joined 'bracket' subscribers get whatever's been cached so far,
      // one message per division.
      if (ch === 'bracket') {
        Object.keys(bracketCache).forEach(division => {
          ws.send(JSON.stringify({ type: 'bracket', division, data: bracketCache[division] }));
        });
      }

    } else if (msg.type === 'state') {
      const ch = 'mat_' + msg.mat;

      // Keep the bout's history entry current with whatever matches have
      // been submitted so far — fires on every submitted match, not just
      // once when the bout ends.
      upsertBoutHistory(msg);

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
      if (msg.division) bracketCache[msg.division] = msg.data;
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
      if (joinedChannel === 'presence') broadcastPresence();
    }
  });

  ws.on('error', () => {
    if (joinedChannel && channels[joinedChannel]) {
      channels[joinedChannel].delete(ws);
      if (joinedChannel === 'presence') broadcastPresence();
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
