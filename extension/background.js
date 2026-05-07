// Service worker: receives auction-WS URLs from content.js, gathers the full
// Whatnot cookie via chrome.cookies (incl. HttpOnly), and POSTs to the local
// scanner's /auth/refresh endpoint.

const SCANNER_URL = 'http://localhost:5000/auth/refresh';
const CAPTURE_URL = 'http://localhost:5000/capture/join';

async function pushJoin(socketKind, topic, payload) {
  try {
    await fetch(CAPTURE_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ socketKind, topic, payload }),
    });
  } catch (_) { /* server may not be running */ }
}

async function pushTokens(wsUrl, socketKind) {
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
    return { ok: false, error: 'Bad WS URL: ' + e.message };
  }
  if (!csrf || !sessionToken) {
    return { ok: false, error: 'WS URL missing csrf or session token' };
  }

  const cookies   = await chrome.cookies.getAll({ domain: 'whatnot.com' });
  const cookieStr = cookies.map(c => `${c.name}=${c.value}`).join('; ');
  if (!cookieStr) {
    return { ok: false, error: 'No whatnot.com cookies found — are you logged in?' };
  }

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
      }),
    });
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);

    const at = Date.now();
    await chrome.storage.local.set({ lastPushedAt: at, lastWsUrl: wsUrl });
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

chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
  (async () => {
    if (msg.type === 'ws_url') {
      const result = await pushTokens(msg.url, msg.socketKind);
      sendResponse(result);
      return;
    }
    if (msg.type === 'phx_join') {
      await pushJoin(msg.socketKind, msg.topic, msg.payload);
      sendResponse({ ok: true });
      return;
    }
    if (msg.type === 'manual_push') {
      const { lastWsUrl } = await chrome.storage.local.get('lastWsUrl');
      if (!lastWsUrl) {
        sendResponse({ ok: false, error: 'No WS URL captured yet — open a Whatnot livestream first.' });
        return;
      }
      sendResponse(await pushTokens(lastWsUrl, null));
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
