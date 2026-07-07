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

// =========================================================================
// YOUTUBE LIVE STATUS — hybrid trigger + confirm
//
// Two different YouTube page-HTML formats have already broken this feature
// by changing without notice (regex-hunting for a "live" badge in scraped
// markup is inherently fragile — YouTube can redesign that markup any time,
// with no warning and no version number). To stop chasing that indefinitely,
// live/offline status is now ALWAYS decided by YouTube's own official Data
// API (search.list), which returns a small, stable, versioned JSON contract
// instead of raw page HTML. That endpoint is the single source of truth for
// what gets shown to viewers. It is never bypassed.
//
// The catch: search.list costs 100 quota units per call against a
// 10,000/day free quota — enough for roughly one call every ~15 minutes on
// a fixed timer, too slow for near-real-time detection. So instead of a
// fixed timer, this uses a trigger/confirm pattern:
//
//   1. TRIGGER (free, every 60s): the old page-scrape, but now purely a
//      *hint* that something might have changed. It is NEVER used to set
//      status directly — so if YouTube changes their page markup again,
//      the worst case is a missed/delayed hint, not a wrong status shown
//      to viewers.
//   2. CONFIRM (metered, only when needed): fires as soon as the trigger's
//      hint disagrees with our last confirmed status, subject to a short
//      cooldown so a flaky/flapping hint can't burn through quota.
//   3. BACKSTOP (metered, every 20 min regardless): a periodic confirm even
//      without a mismatch, in case the free trigger ever fails to catch a
//      real transition at all — this bounds the maximum staleness of the
//      status to 20 minutes no matter what happens to the scrape.
//
// Worst case quota usage: 24h / 20min = 72 backstop calls/day (7,200
// units), leaving ~2,800 units (28 calls) of headroom for real go-live/
// go-offline transitions — plenty for a normal event schedule. In
// practice, real transitions get caught within ~60-90s of the trigger
// noticing them, well under the 20-minute backstop ceiling.
//
// Requires YOUTUBE_API_KEY (Render → Environment tab). Without one, this
// falls back to scrape-only behavior — faster, free, but exactly as
// fragile to YouTube markup changes as before. Setting the key is
// strongly recommended; you already have one configured.
// =========================================================================
const YOUTUBE_CHANNEL_ID = 'UCkmZDvKGkG1AkX9A8idH1KA'; // @CollegiateJJLeague
const YOUTUBE_CHECK_ENABLED = process.env.ENABLE_YOUTUBE_CHECK !== 'false'; // set to 'false' in Render env vars to disable entirely
const YOUTUBE_API_KEY = process.env.YOUTUBE_API_KEY || null;

const TRIGGER_POLL_MS = 60 * 1000;          // free scrape hint, every 60s
const CONFIRM_COOLDOWN_MS = 90 * 1000;      // min gap between API confirm calls
const CONFIRM_BACKSTOP_MS = 20 * 60 * 1000; // force a confirm at least this often regardless of the hint

let youtubeStatus = { isLive: false, viewers: null };
let lastConfirmAt = 0;
let confirmInFlight = false;

function pollYoutube() {
  if (!YOUTUBE_CHECK_ENABLED) return;

  if (!YOUTUBE_API_KEY) {
    // No key configured: legacy scrape-only behavior. Fragile to YouTube
    // markup changes (as we've seen), but works with zero setup.
    const url = `https://www.youtube.com/channel/${YOUTUBE_CHANNEL_ID}/live`;
    fetchTriggerHint(url, 2, (guessLive) => applyYoutubeStatus(guessLive, null));
    return;
  }

  const url = `https://www.youtube.com/channel/${YOUTUBE_CHANNEL_ID}/live`;
  fetchTriggerHint(url, 2, (guessLive) => maybeConfirm(guessLive));
}

// Runs the page-scrape purely to produce a best-effort guess of whether the
// channel might currently be live. In hybrid (API-key) mode this guess is
// NEVER applied directly to youtubeStatus — it only decides whether to
// spend a confirm call. A wrong guess here costs one wasted-but-harmless
// API confirm; it can never cause a wrong status to reach viewers.
function fetchTriggerHint(url, redirectsLeft, callback) {
  https.get(url, {
    headers: {
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36',
      'Accept-Language': 'en-US,en;q=0.9',
      'Cookie': 'CONSENT=YES+cb.20240101-00-p0.en+FX+000'
    }
  }, (res) => {
    if ([301, 302, 303, 307, 308].includes(res.statusCode) && res.headers.location && redirectsLeft > 0) {
      res.resume();
      fetchTriggerHint(res.headers.location, redirectsLeft - 1, callback);
      return;
    }
    let body = '';
    res.on('data', chunk => { body += chunk; });
    res.on('end', () => {
      const guessLive = guessLiveFromBody(body);
      console.log(`YouTube trigger hint: guessLive=${guessLive}, bodyLength=${body.length}`);
      callback(guessLive);
    });
  }).on('error', (err) => {
    console.error('YouTube trigger fetch failed:', err.message);
    callback(youtubeStatus.isLive);
  });
}

function guessLiveFromBody(body) {
  let idx = 0;
  while (true) {
    const foundIdx = body.indexOf('"videoDetails"', idx);
    if (foundIdx === -1) break;
    const windowStr = body.slice(foundIdx, foundIdx + 1200);
    if (/"isLive":true/.test(windowStr) || /"isLiveContent":true/.test(windowStr)) return true;
    idx = foundIdx + 1;
  }
  let badgeIdx = 0;
  while (true) {
    const foundIdx = body.indexOf('THUMBNAIL_OVERLAY_BADGE_STYLE_LIVE', badgeIdx);
    if (foundIdx === -1) break;
    const windowStr = body.slice(Math.max(0, foundIdx - 4000), foundIdx + 2000);
    if (windowStr.includes(YOUTUBE_CHANNEL_ID)) return true;
    badgeIdx = foundIdx + 1;
  }
  return false;
}

function maybeConfirm(guessLive) {
  const now = Date.now();
  const mismatch = guessLive !== youtubeStatus.isLive;
  const dueForBackstop = now - lastConfirmAt >= CONFIRM_BACKSTOP_MS;
  const cooledDown = now - lastConfirmAt >= CONFIRM_COOLDOWN_MS;

  if (confirmInFlight) return;
  if ((mismatch && cooledDown) || dueForBackstop) {
    console.log(`YouTube: confirming via official API (mismatch=${mismatch}, dueForBackstop=${dueForBackstop})...`);
    lastConfirmAt = now;
    confirmViaApi();
  }
}

function confirmViaApi() {
  confirmInFlight = true;
  const url = `https://www.googleapis.com/youtube/v3/search?part=snippet&channelId=${YOUTUBE_CHANNEL_ID}&eventType=live&type=video&key=${YOUTUBE_API_KEY}`;
  https.get(url, (res) => {
    let body = '';
    res.on('data', chunk => { body += chunk; });
    res.on('end', () => {
      confirmInFlight = false;
      try {
        const data = JSON.parse(body);
        if (data.error) {
          console.error('YouTube search.list error:', data.error.message);
          return;
        }
        const item = data.items && data.items[0];
        if (!item || !item.id || !item.id.videoId) {
          console.log('YouTube API confirm: not live.');
          applyYoutubeStatus(false, null);
          return;
        }
        console.log(`YouTube API confirm: live, videoId=${item.id.videoId}`);
        getVideoDetails(item.id.videoId);
      } catch (err) {
        console.error('Failed to parse YouTube search.list response:', err.message);
      }
    });
  }).on('error', (err) => {
    confirmInFlight = false;
    console.error('YouTube search.list fetch failed:', err.message);
  });
}

function getVideoDetails(videoId) {
  const url = `https://www.googleapis.com/youtube/v3/videos?part=liveStreamingDetails&id=${videoId}&key=${YOUTUBE_API_KEY}`;
  https.get(url, (res) => {
    let body = '';
    res.on('data', chunk => { body += chunk; });
    res.on('end', () => {
      try {
        const data = JSON.parse(body);
        const item = data.items && data.items[0];
        const viewers = item && item.liveStreamingDetails && item.liveStreamingDetails.concurrentViewers
          ? parseInt(item.liveStreamingDetails.concurrentViewers, 10)
          : null;
        applyYoutubeStatus(true, viewers);
      } catch (err) {
        console.error('Failed to parse YouTube videos.list response:', err.message);
        applyYoutubeStatus(true, null);
      }
    });
  }).on('error', (err) => {
    console.error('YouTube videos.list fetch failed:', err.message);
    applyYoutubeStatus(true, null);
  });
}

function applyYoutubeStatus(isLive, viewers) {
  const changed = youtubeStatus.isLive !== isLive || youtubeStatus.viewers !== viewers;
  youtubeStatus = { isLive, viewers };
  console.log(`YouTube status set: isLive=${isLive}, viewers=${viewers}, changed=${changed}`);
  if (changed) {
    console.log('Broadcasting YouTube status update...');
    broadcastYoutubeStatus();
  }
}

function broadcastYoutubeStatus() {
  const ch = 'youtube_status';
  if (!channels[ch]) return;
  const payload = JSON.stringify({ type: 'youtube_status', ...youtubeStatus });
  channels[ch].forEach(client => {
    if (client.readyState === 1) client.send(payload);
  });
}

if (YOUTUBE_CHECK_ENABLED) {
  console.log(`YouTube live-status check enabled. Trigger polling every ${TRIGGER_POLL_MS}ms. Hybrid API-confirm mode: ${!!YOUTUBE_API_KEY}. Channel ID: ${YOUTUBE_CHANNEL_ID}`);
  pollYoutube();
  setInterval(pollYoutube, TRIGGER_POLL_MS);
} else {
  console.log('YouTube live-status check disabled via ENABLE_YOUTUBE_CHECK=false.');
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

      // Newly joined 'youtube_status' subscribers get the last known status immediately.
      if (ch === 'youtube_status') {
        ws.send(JSON.stringify({ type: 'youtube_status', ...youtubeStatus }));
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
