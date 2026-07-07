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
  fetchYoutubeLivePage(url, 2);
}

function fetchYoutubeLivePage(url, redirectsLeft) {
  https.get(url, {
    headers: {
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36',
      'Accept-Language': 'en-US,en;q=0.9',
      // Bypasses YouTube's cookie-consent interstitial page, which is
      // frequently served instead of real content to requests coming from
      // datacenter/cloud IPs (like Render's) rather than residential ones.
      // Without this, the scrape can silently get a consent page with none
      // of the expected canonicalBaseUrl/isLiveNow data in it.
      'Cookie': 'CONSENT=YES+cb.20240101-00-p0.en+FX+000'
    }
  }, (res) => {
    if ([301, 302, 303, 307, 308].includes(res.statusCode) && res.headers.location && redirectsLeft > 0) {
      console.log(`YouTube fetch redirected (${res.statusCode}) to ${res.headers.location}`);
      res.resume(); // discard this response body
      fetchYoutubeLivePage(res.headers.location, redirectsLeft - 1);
      return;
    }

    let body = '';
    res.on('data', chunk => { body += chunk; });
    res.on('end', () => {
      try {
        const titleMatch = body.match(/<title>([^<]*)<\/title>/);
        console.log(`YouTube fetch: status=${res.statusCode}, bodyLength=${body.length}, title="${titleMatch ? titleMatch[1] : '(none found)'}"`);
        console.log(`YouTube fetch diagnostics: hasCanonicalBaseUrl=${body.includes('"canonicalBaseUrl"')}, hasVideoDetails=${body.includes('"videoDetails"')}, hasIsLiveNow=${body.includes('"isLiveNow"')}, hasLiveBadge=${body.includes('THUMBNAIL_OVERLAY_BADGE_STYLE_LIVE')}, hasOgUrl=${body.includes('og:url')}`);

        let videoId = null;
        let isLiveFromDetails = false;

        // Primary source: videoDetails is the block YouTube embeds describing
        // THE ACTUAL VIDEO this page is rendering — its videoId and isLive
        // flag live right next to each other here. This avoids accidentally
        // grabbing an unrelated video's ID from elsewhere on the page (e.g.
        // a recommended video or another creator's live stream shown in a
        // sidebar), which a bare "first videoId on the page" scan can do.
        //
        // There can be more than one "videoDetails" block on the page (e.g.
        // hover-preview players for recommended videos also carry one), so
        // we scan every occurrence and prefer whichever one actually reports
        // isLive true, rather than just taking the first one found.
        let fallbackVideoId = null;
        let fallbackIdx = -1;
        let matchedIdx = -1;
        let searchFrom = 0;
        while (true) {
          const idx = body.indexOf('"videoDetails"', searchFrom);
          if (idx === -1) break;
          const windowStr = body.slice(idx, idx + 1200);
          const vidMatch = windowStr.match(/"videoId":"([a-zA-Z0-9_-]{11})"/);
          const liveHere = /"isLive":true/.test(windowStr) || /"isLiveContent":true/.test(windowStr);
          if (vidMatch && !fallbackVideoId) { fallbackVideoId = vidMatch[1]; fallbackIdx = idx; }
          if (vidMatch && liveHere) {
            videoId = vidMatch[1];
            isLiveFromDetails = true;
            matchedIdx = idx;
            break;
          }
          searchFrom = idx + 1;
        }
        if (!videoId && fallbackVideoId) { videoId = fallbackVideoId; matchedIdx = fallbackIdx; }

        // Secondary confirmation: canonical link patterns, if videoDetails
        // wasn't found for some reason.
        if (!videoId) {
          const canonicalMatch = body.match(/"canonicalBaseUrl":"\/watch\?v=([a-zA-Z0-9_-]+)"/) ||
                                  body.match(/<link rel="canonical" href="https:\/\/www\.youtube\.com\/watch\?v=([a-zA-Z0-9_-]+)"/) ||
                                  body.match(/<meta property="og:url" content="https:\/\/www\.youtube\.com\/watch\?v=([a-zA-Z0-9_-]+)"/);
          if (canonicalMatch) videoId = canonicalMatch[1];
        }

        // Third strategy: the page may not be the live video's own watch
        // page at all — it could be rendering the channel's feed, with the
        // live stream just shown as a badged thumbnail card. As of YouTube's
        // current markup, those cards use a lockupViewModel structure: the
        // live badge appears as badgeStyle:"THUMBNAIL_OVERLAY_BADGE_STYLE_LIVE"
        // (not the older bare "style":"LIVE"), and the card's video ID lives
        // in its own "contentId" field rather than "videoId". contentId
        // reliably appears shortly BEFORE the badge within the same lockup
        // object, so we find the badge, then look backward for the nearest
        // preceding contentId.
        if (!videoId) {
          const badgeIdx = body.indexOf('THUMBNAIL_OVERLAY_BADGE_STYLE_LIVE');
          if (badgeIdx !== -1) {
            const precedingChunk = body.slice(Math.max(0, badgeIdx - 4000), badgeIdx);
            const allContentIds = [...precedingChunk.matchAll(/"contentId":"([a-zA-Z0-9_-]{11})"/g)];
            if (allContentIds.length > 0) {
              videoId = allContentIds[allContentIds.length - 1][1]; // closest one before the badge
              isLiveFromDetails = true; // the badge itself is our live confirmation here
              matchedIdx = badgeIdx;
              console.log(`YouTube fetch: used lockup badge-proximity match: ${videoId}`);
            }
          }
        }

        const freeSignalLive = !!videoId && isLiveFromDetails;
        console.log(`YouTube page check: videoId=${videoId}, isLiveFromDetails=${isLiveFromDetails}, freeSignalLive=${freeSignalLive}`);


        if (!freeSignalLive) {
          applyYoutubeStatus(false, null);
          return;
        }

        // Reuse the same bounded window we already found videoId/isLive in,
        // so the viewer count we read also belongs to OUR video and not to
        // an unrelated one elsewhere on the page.
        const nearbyWindow = matchedIdx !== -1 ? body.slice(Math.max(0, matchedIdx - 2000), matchedIdx + 5000) : body;

        if (YOUTUBE_API_KEY) {
          // Enhancement only: get a precise, official viewer count for the
          // video ID we already confirmed is live. If this call fails for
          // any reason, still report live using whatever the free page gave us.
          getVideoDetails(videoId, nearbyWindow);
        } else {
          const viewers = freeViewerCountFrom(nearbyWindow);
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
  if (viewMatch) return parseInt(viewMatch[1], 10);

  // concurrentViewers only appears on the video's own watch page. When we
  // detected liveness via the channel-feed lockup card instead (see the
  // badge-proximity strategy above), there's no such field nearby — but the
  // card itself usually renders a plain display string like "558 watching"
  // or "1.2K watching" in its metadata text. That's an approximate,
  // YouTube-rounded figure rather than an exact live count, but it's the
  // only viewer signal available on this page type.
  const watchingMatch = pageBody.match(/([\d,.]+\s*[KMB]?)\s*watching/i);
  if (!watchingMatch) return null;

  const approx = parseApproxCount(watchingMatch[1]);
  if (approx !== null) console.log(`YouTube fetch: used approximate "watching" text fallback: "${watchingMatch[1]}" -> ${approx}`);
  return approx;
}

// Converts YouTube's rounded display strings ("558", "1.2K", "3.4M") into a
// plain integer. Returns null if the string doesn't look like a count.
function parseApproxCount(str) {
  const m = str.replace(/,/g, '').trim().match(/^([\d.]+)\s*([KMB]?)$/i);
  if (!m) return null;
  const num = parseFloat(m[1]);
  if (Number.isNaN(num)) return null;
  const mult = { K: 1e3, M: 1e6, B: 1e9 }[m[2].toUpperCase()] || 1;
  return Math.round(num * mult);
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
