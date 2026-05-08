// Service worker: receives auction-WS URLs from content.js, gathers the full
// Whatnot cookie via chrome.cookies (incl. HttpOnly), and POSTs to the local
// scanner's /auth/refresh endpoint.

const SCANNER_URL    = 'http://localhost:5000/auth/refresh';
const CAPTURE_URL    = 'http://localhost:5000/capture/join';
const CATEGORIES_URL = 'http://localhost:5000/categories/discovered';
// No automatic force_reconnect anymore. We discovered (the hard way) that
// every force_reconnect itself causes a cliff: closing the page's Phoenix
// sockets makes Whatnot's server rotate the session, which invalidates
// any in-flight tokens the scanner is using. So proactive refreshing was
// counterproductive — every tick we tried to "save" tokens, we were
// actually killing the ones the scanner was using.
//
// What we keep:
//   - Token capture from the page's organic WS opens (inject.js → ws_url)
//   - Manual "Force refresh now" button in the popup (user-initiated)
//   - Service-worker keepalive (harmless, helps debugging)
//
// What we removed:
//   - The auto-refresh alarm that fired every N minutes
//   - The dashboard-driven refresh trigger
//   - All "smart skip" logic that was only there to manage alarm cliffs

// In-memory dedup so we don't POST the same (id,label) on every page nav.
// Persisted across SW restarts via chrome.storage.local.
const discoveredCats = new Map();
chrome.storage.local.get('discoveredCats').then(({ discoveredCats: stored }) => {
  if (stored && typeof stored === 'object') {
    for (const [k, v] of Object.entries(stored)) discoveredCats.set(k, v);
  }
}).catch(() => {});

async function pushCategory(id, label) {
  if (!id || !label) return;
  const key = `${id}::${label}`;
  if (discoveredCats.has(key)) return;
  discoveredCats.set(key, Date.now());
  // Persist
  try {
    await chrome.storage.local.set({
      discoveredCats: Object.fromEntries(discoveredCats),
    });
  } catch (_) {}

  try {
    await fetch(CATEGORIES_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ categories: [{ id, label }] }),
    });
  } catch (_) { /* server may not be running */ }
}

async function pushJoin(socketKind, topic, payload) {
  try {
    await fetch(CAPTURE_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ socketKind, topic, payload }),
    });
  } catch (_) { /* server may not be running */ }
}

// Most recent tokens we've sent up, per socket kind, for change detection.
const lastPushed = { auction: null, live: null };

// `source` is one of:
//   'page'   — the page itself opened a WS (inject.js capture path)
//   'alarm'  — alarm-driven cookie-fallback push
//   'manual' — user clicked "Force refresh now" or "Push manually"
// The server uses this to surface in the dashboard so we can tell whether
// auto-refresh is actually minting new tokens vs. recycling stale ones.
async function pushTokens(wsUrl, socketKind, source) {
  let csrf, sessionToken, clientVersion;
  try {
    const u = new URL(wsUrl);
    const params  = u.searchParams;
    csrf          = params.get('_csrf_token');
    sessionToken  = params.get('sessionExtensionToken');
    clientVersion = params.get('client_version');
    if (!socketKind) {
      socketKind = u.pathname.includes('/auction/') ? 'auction' : 'live';
    }
  } catch (e) {
    console.warn('[wnn] pushTokens: bad URL', e.message);
    return { ok: false, error: 'Bad WS URL: ' + e.message };
  }
  if (!csrf || !sessionToken) {
    console.warn('[wnn] pushTokens: missing csrf/session in URL');
    return { ok: false, error: 'WS URL missing csrf or session token' };
  }

  const tokenKey = csrf + '|' + sessionToken;
  const changed  = lastPushed[socketKind] !== tokenKey;
  lastPushed[socketKind] = tokenKey;

  const cookies   = await chrome.cookies.getAll({ domain: 'whatnot.com' });
  const cookieStr = cookies.map(c => `${c.name}=${c.value}`).join('; ');
  if (!cookieStr) {
    console.warn('[wnn] pushTokens: no whatnot.com cookies — not logged in?');
    return { ok: false, error: 'No whatnot.com cookies found — are you logged in?' };
  }

  console.log(`[wnn] pushTokens(${socketKind}) — tokens ${changed ? 'CHANGED ✓' : 'unchanged'}, cookie ${cookieStr.length}ch`);

  try {
    const resp = await fetch(SCANNER_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        cookie:         cookieStr,
        csrf_token:     csrf,
        session_token:  sessionToken,
        client_version: clientVersion || '',
        socket_kind:    socketKind,
        source:         source || 'unknown',
      }),
    });
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);

    const at = Date.now();
    // Cache per-socket URL so the manual-refresh path (popup button) can
    // re-push both auction and live with whatever was most recently seen.
    const update = { lastPushedAt: at, lastWsUrl: wsUrl };
    update[`lastWsUrl_${socketKind}`] = wsUrl;
    await chrome.storage.local.set(update);
    flashBadge('OK', '#00aa55');
    return { ok: true, at };
  } catch (e) {
    flashBadge('!', '#cc4400');
    return { ok: false, error: 'Scanner unreachable: ' + e.message };
  }
}

function flashBadge(text, color) {
  chrome.action.setBadgeText({ text });
  chrome.action.setBadgeBackgroundColor({ color });
  setTimeout(() => chrome.action.setBadgeText({ text: '' }), 2500);
}

// ── Auto-refresh ──────────────────────────────────────────────────────────
// Periodically re-push the most recent URL for each socket kind so the
// scanner stays warm even when no Whatnot tab is open. Cookies are read
// fresh from chrome.cookies at every tick.
async function autoRefresh(source = 'manual') {
  console.log(`[wnn refresh] start (source=${source}) at ${new Date().toISOString()}`);

  // Step 1: find Whatnot tabs and ask each to drop its WebSocket.
  let tabs = [];
  try {
    tabs = await chrome.tabs.query({ url: ['*://*.whatnot.com/*'] });
  } catch (e) {
    console.warn('[wnn auto-refresh] tabs.query failed:', e.message);
  }
  console.log(`[wnn auto-refresh] found ${tabs.length} whatnot.com tab(s)`);

  let triggered = 0;
  for (const t of tabs) {
    try {
      await chrome.tabs.sendMessage(t.id, { type: 'force_reconnect' });
      triggered++;
      console.log(`[wnn auto-refresh] sent force_reconnect to tab ${t.id} (${t.url})`);
    } catch (e) {
      console.warn(`[wnn auto-refresh] tab ${t.id} sendMessage failed (no content script?):`, e.message);
    }
  }

  // Phoenix typically reconnects within ~1s. Give it 3s to mint and capture
  // a fresh URL before we do the cookie-fallback push.
  if (triggered > 0) {
    await new Promise(r => setTimeout(r, 3000));
  }

  // Step 2: cookie-fallback push (re-pushes most recent URL with fresh cookie).
  const cache = await chrome.storage.local.get(['lastWsUrl_auction', 'lastWsUrl_live']);
  let pushed = 0, changedCount = 0;
  for (const kind of ['auction', 'live']) {
    const url = cache[`lastWsUrl_${kind}`];
    if (!url) {
      console.log(`[wnn auto-refresh] no cached URL for ${kind} — skip`);
      continue;
    }
    // Snapshot tokens before push so we can tell whether the inject.js push
    // (Step 1's reconnect path) already updated them.
    const beforeKey = lastPushed[kind];
    const r = await pushTokens(url, kind, source);
    const afterKey  = lastPushed[kind];
    if (r.ok) {
      pushed++;
      if (beforeKey !== afterKey) changedCount++;
    }
  }

  console.log(`[wnn auto-refresh] DONE: tabs=${tabs.length}, force_reconnect=${triggered}, cookie_push=${pushed}, tokens_changed=${changedCount}`);
}

// Make sure no leftover auto-refresh alarm survives a code-update reload.
// Older builds of this extension created one; clear it on startup so we
// don't keep self-inflicting cliffs.
chrome.alarms.clear('autoRefresh').catch(() => {});

// ── Service-worker keepalive ───────────────────────────────────────────────
// MV3 service workers get evicted when idle. Browsers (especially Brave)
// throttle hard once that happens — alarms can drift or stop firing entirely
// for backgrounded extensions. Holding a long-running fetch open keeps the
// SW alive so the periodic alarm can do its job reliably.
//
// We connect to main.py's /extension/keepalive SSE endpoint on every SW
// startup and just pump bytes forever. If the connection drops (server
// restarted, network blip), we reconnect after a short delay.
const KEEPALIVE_URL = 'http://localhost:5000/extension/keepalive';
let keepaliveAbort = null;

async function startKeepalive() {
  if (keepaliveAbort) return;  // already running
  keepaliveAbort = new AbortController();
  console.log('[wnn keepalive] connecting to', KEEPALIVE_URL);
  try {
    const resp = await fetch(KEEPALIVE_URL, { signal: keepaliveAbort.signal });
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const reader  = resp.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      // SSE frames are separated by blank lines.
      let idx;
      while ((idx = buffer.indexOf('\n\n')) !== -1) {
        const frame = buffer.slice(0, idx);
        buffer = buffer.slice(idx + 2);
        let eventName = '';
        for (const line of frame.split('\n')) {
          if (line.startsWith('event: ')) eventName = line.slice(7).trim();
        }
        if (eventName === 'refresh') {
          console.log('[wnn keepalive] refresh event received');
          autoRefresh('server').catch(e =>
            console.warn('[wnn keepalive] refresh failed:', e));
        }
      }
    }
  } catch (e) {
    if (e.name !== 'AbortError') {
      console.warn('[wnn keepalive] error:', e.message);
    }
  } finally {
    keepaliveAbort = null;
    setTimeout(() => startKeepalive(), 5000);
  }
}

startKeepalive();

chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
  (async () => {
    if (msg.type === 'ws_url') {
      console.log(`[wnn] page opened ${msg.socketKind} WS — tokensChanged=${msg.tokensChanged}`);
      const result = await pushTokens(msg.url, msg.socketKind, 'page');
      sendResponse(result);
      return;
    }
    if (msg.type === 'phx_join') {
      await pushJoin(msg.socketKind, msg.topic, msg.payload);
      sendResponse({ ok: true });
      return;
    }
    if (msg.type === 'category') {
      await pushCategory(msg.id, msg.label);
      sendResponse({ ok: true });
      return;
    }
    if (msg.type === 'force_refresh_now') {
      try {
        await autoRefresh('manual');
        sendResponse({ ok: true });
      } catch (e) {
        sendResponse({ ok: false, error: String(e) });
      }
      return;
    }
    if (msg.type === 'reconnect_ack') {
      console.log(`[wnn] page reported ${msg.closed} sockets closed`);
      sendResponse({ ok: true });
      return;
    }
    if (msg.type === 'manual_push') {
      const { lastWsUrl } = await chrome.storage.local.get('lastWsUrl');
      if (!lastWsUrl) {
        sendResponse({ ok: false, error: 'No WS URL captured yet — open a Whatnot livestream first.' });
        return;
      }
      sendResponse(await pushTokens(lastWsUrl, null, 'manual'));
      return;
    }
    if (msg.type === 'status') {
      const { lastPushedAt, lastWsUrl } = await chrome.storage.local.get(['lastPushedAt', 'lastWsUrl']);
      sendResponse({ lastPushedAt: lastPushedAt || 0, hasUrl: !!lastWsUrl });
      return;
    }
  })();
  return true; // keep the message channel open for async sendResponse
});
