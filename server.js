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
// YOUTUBE LIVE STATUS
// Deliberately avoids the official YouTube Data API (search.list for live
// broadcasts costs 100 quota units per call against a 10,000/day free quota —
// roughly one check every 15 minutes to stay safe). Instead, this fetches
// the channel's public /live page directly (a plain page load, not a metered
// API call, no key required) and reads live status + concurrent viewers out
// of the page's own embedded data. Kill switch below if you'd rather turn
// this off entirely.
// =========================================================================
const YOUTUBE_CHANNEL_ID = 'UCkmZDvKGkG1AkX9A8idH1KA'; // @CollegiateJJLeague
const YOUTUBE_CHECK_ENABLED = process.env.ENABLE_YOUTUBE_CHECK !== 'false'; // set to 'false' in Render env vars to disable entirely
const YOUTUBE_POLL_MS = 60 * 1000; // 60s

// Optional: set YOUTUBE_API_KEY in Render's Environment tab to unlock the
// accurate path below. Without it, this still works using only the free
// page-check (video ID + a regex-guessed live flag/viewer count). With it,
// the video ID found by the free page-check gets confirmed and its viewer
// count read from YouTube's own official videos.list endpoint — 1 quota
// unit per call, so even polling every 60s all day (~1,440 calls) uses a
// small fraction of the 10,000/day free quota. No code changes needed to
// switch it on — just add the key and redeploy.
const YOUTUBE_API_KEY = process.env.YOUTUBE_API_KEY || null;

let youtubeStatus = { isLive: false, viewers: null };

// Detection ALWAYS runs via the free page-scrape, whether or not an API key
// is configured. This is deliberate: YouTube's official search.list?eventType=live
// endpoint is index-based, not real-time, and is well documented to be slow
// or unreliable to reflect a stream that just went live — sometimes taking
// several minutes, sometimes never surfacing it in time. The public /live
// page, by contrast, reads live status directly off the page's own embedded
// data with no such delay. If an API key is present, it's only used
// afterward, as an enhancement, to fetch a precise concurrentViewers count
// for a video ID we already know is live from the scrape.
function fetchYoutubeLiveStatus() {
  if (!YOUTUBE_CHECK_ENABLED) return;
  console.log('Checking YouTube live status...');

  const url = `https://www.youtube.com/channel/${YOUTUBE_CHANNEL_ID}/live`;
  https.get(url, { headers: { 'User-Agent': 'Mozilla/5.0' } }, (res) => {
    let body = '';
    res.on('data', chunk => { body += chunk; });
    res.on('end', () => {
      try {
        let videoId = null;
        const canonicalMatch = body.match(/"canonicalBaseUrl":"\/watch\?v=([a-zA-Z0-9_-]+)"/) ||
                                body.match(/<link rel="canonical" href="https:\/\/www\.youtube\.com\/watch\?v=([a-zA-Z0-9_-]+)"/);
        if (canonicalMatch) videoId = canonicalMatch[1];

        const freeSignalLive = !!videoId && (/"isLiveNow"\s*:\s*true/.test(body) || /"style"\s*:\s*"LIVE"/.test(body));
        console.log(`YouTube page check: videoId=${videoId}, freeSignalLive=${freeSignalLive}`);

        if (!freeSignalLive) {
          applyYoutubeStatus(false, null);
          return;
        }

        if (YOUTUBE_API_KEY) {
          // Enhancement only: get a precise, official viewer count for the
          // video ID we already confirmed is live. If this call fails for
          // any reason, still report live using whatever the free page gave us.
          getVideoDetails(videoId, body);
        } else {
          let viewers = null;
          const viewMatch = body.match(/"concurrentViewers"\s*:\s*"(\d+)"/);
          if (viewMatch) viewers = parseInt(viewMatch[1], 10);
          console.log(`Using free page check: viewers=${viewers}`);
          applyYoutubeStatus(true, viewers);
        }
      } catch (err) {
        console.error('Failed to parse YouTube live page:', err.message);
      }
    });
  }).on('error', (err) => {
    console.error('YouTube live-status fetch failed:', err.message);
  });
}

function freeViewerCountFrom(pageBody) {
  const viewMatch = pageBody.match(/"concurrentViewers"\s*:\s*"(\d+)"/);
  return viewMatch ? parseInt(viewMatch[1], 10) : null;
}

// videoId and pageBody come from fetchYoutubeLiveStatus, which has already
// confirmed via page-scrape that this video is live right now. This function
// only tries to get a more precise official viewer count — a failure here
// should never flip status back to offline, since we already know it's live.
function getVideoDetails(videoId, pageBody) {
  const fallbackViewers = freeViewerCountFrom(pageBody);
  const url = `https://www.googleapis.com/youtube/v3/videos?part=snippet,liveStreamingDetails&id=${videoId}&key=${YOUTUBE_API_KEY}`;
  https.get(url, (res) => {
    let body = '';
    res.on('data', chunk => { body += chunk; });
    res.on('end', () => {
      try {
        const data = JSON.parse(body);
        const item = data.items && data.items[0];
        if (!item) {
          console.log('YouTube API: no video item found, using free page check as fallback');
          applyYoutubeStatus(true, fallbackViewers);
          return;
        }

        const viewers = item.liveStreamingDetails && item.liveStreamingDetails.concurrentViewers
          ? parseInt(item.liveStreamingDetails.concurrentViewers, 10)
          : fallbackViewers;

        console.log(`YouTube API confirmed live, viewers=${viewers}`);
        applyYoutubeStatus(true, viewers);
      } catch (err) {
        console.error('Failed to parse YouTube Data API response, using free page check as fallback:', err.message);
        applyYoutubeStatus(true, fallbackViewers);
      }
    });
  }).on('error', (err) => {
    console.error('YouTube Data API fetch failed, using free page check as fallback:', err.message);
    applyYoutubeStatus(true, fallbackViewers);
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
  console.log(`YouTube live-status check enabled. Polling every ${YOUTUBE_POLL_MS}ms. Channel ID: ${YOUTUBE_CHANNEL_ID}`);
  fetchYoutubeLiveStatus();
  setInterval(fetchYoutubeLiveStatus, YOUTUBE_POLL_MS);
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
