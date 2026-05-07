// Runs in the page's MAIN world. Monkey-patches WebSocket so we can capture:
//   1. the socket URL (csrf_token, sessionExtensionToken, client_version)
//   2. every outgoing phx_join (topic + payload)
//   3. references to active Whatnot sockets — so we can force-close them
//      on demand, triggering Phoenix's auto-reconnect with fresh tokens.
(() => {
  const Orig = window.WebSocket;
  if (!Orig || Orig.__wn_patched) return;

  function extractTokens(u) {
    try {
      const url = new URL(u);
      return (
        url.searchParams.get('_csrf_token') + '|' +
        url.searchParams.get('sessionExtensionToken')
      );
    } catch (_) { return ''; }
  }

  // Track currently-open Whatnot WebSockets so the extension can ask us
  // to close+reconnect them when its auto-refresh alarm fires.
  const activeSockets = new Set();
  window.__wn_sockets = activeSockets;

  // Remember the most recent URL we've seen per socket kind so we can detect
  // whether Phoenix is actually getting fresh tokens on reconnect or just
  // re-using stale ones. Tells us whether close-and-reconnect is sufficient.
  const lastUrlByKind = { auction: null, live: null };

  const Patched = function (url, protocols) {
    const isWhatnot = typeof url === 'string' &&
      (url.includes('/auction/socket/websocket') || url.includes('/live/socket/websocket'));

    let socketKind = null;
    if (isWhatnot) {
      socketKind = url.includes('/auction/') ? 'auction' : 'live';
      const prev = lastUrlByKind[socketKind];
      const tokensChanged = prev ? (extractTokens(prev) !== extractTokens(url)) : true;
      lastUrlByKind[socketKind] = url;
      try {
        window.postMessage({
          type: 'WHATNOT_AUCTION_WS',
          url,
          socketKind,
          tokensChanged,
        }, '*');
      } catch (_) {}
    }

    const ws = protocols !== undefined ? new Orig(url, protocols) : new Orig(url);

    if (isWhatnot) {
      activeSockets.add(ws);
      ws.addEventListener('close', () => activeSockets.delete(ws));

      const origSend = ws.send.bind(ws);
      ws.send = function (data) {
        try {
          if (typeof data === 'string' && data.includes('phx_join')) {
            const msg = JSON.parse(data);
            if (Array.isArray(msg) && msg.length >= 5 && msg[3] === 'phx_join') {
              window.postMessage({
                type: 'WHATNOT_PHX_JOIN',
                socketKind,
                topic:   msg[2],
                payload: msg[4],
              }, '*');
            }
          }
        } catch (_) {}
        return origSend(data);
      };
    }
    return ws;
  };

  Patched.prototype = Orig.prototype;
  Patched.CONNECTING = Orig.CONNECTING;
  Patched.OPEN       = Orig.OPEN;
  Patched.CLOSING    = Orig.CLOSING;
  Patched.CLOSED     = Orig.CLOSED;
  Patched.__wn_patched = true;

  window.WebSocket = Patched;

  // Force-reconnect handler: close every active Whatnot WS. Phoenix's
  // built-in client will auto-reconnect within ~1s, which mints a fresh
  // URL with new csrf+session tokens — captured by the constructor patch
  // above and pushed to the scanner via content.js → background.js.
  window.addEventListener('message', (e) => {
    if (e.source !== window || !e.data) return;
    if (e.data.type !== 'WHATNOT_FORCE_RECONNECT') return;
    let closed = 0;
    for (const ws of Array.from(activeSockets)) {
      try {
        if (ws.readyState === Orig.OPEN || ws.readyState === Orig.CONNECTING) {
          ws.close(1000, 'wnn_refresh');
          closed++;
        }
      } catch (_) {}
    }
    // ack so background.js can log / verify
    try { window.postMessage({ type: 'WHATNOT_RECONNECT_DONE', closed }, '*'); } catch (_) {}
  });
})();


// ── fetch monkey-patch for category discovery ────────────────────────────
// When whatnot.com makes LiveStreamExplore GraphQL calls, capture
// (variables.id → category label) pairs from the request + response so the
// scanner can offer them as checkable categories without DevTools work.
(() => {
  const origFetch = window.fetch;
  if (!origFetch || origFetch.__wn_patched) return;

  const wrapped = async function (input, init) {
    const url = typeof input === 'string' ? input : (input && input.url) || '';
    const isGql = typeof url === 'string' && url.includes('/services/graphql/');
    if (!isGql) return origFetch.apply(this, arguments);

    // Pull operationName + variables.id from the request body
    let opName = null, varId = null;
    try {
      const body = (init && init.body) || (input && input.body) || null;
      if (typeof body === 'string') {
        const parsed = JSON.parse(body);
        opName = parsed.operationName || null;
        varId  = parsed.variables && parsed.variables.id ? String(parsed.variables.id) : null;
      }
    } catch (_) {}

    const resp = await origFetch.apply(this, arguments);

    // Only inspect bodies for the queries we care about
    if (opName !== 'LiveStreamExplore' || !varId) return resp;

    try {
      const cloned = resp.clone();
      cloned.json().then((data) => {
        // Walk the LiveStreamExplore response: pick the most common label
        // from the streams' livestreamCategories[0].label
        const edges =
          (((data || {}).data || {}).liveStream || {}).explore &&
          ((((data || {}).data || {}).liveStream || {}).explore.objects || {}).edges;
        if (!Array.isArray(edges) || !edges.length) return;
        const counts = {};
        for (const e of edges) {
          const obj = e && e.node && e.node.object;
          if (!obj) continue;
          const cats = obj.livestreamCategories || [];
          for (const c of cats) {
            const lbl = c && c.label;
            if (typeof lbl === 'string' && lbl.length > 0 && lbl.length < 100) {
              counts[lbl] = (counts[lbl] || 0) + 1;
            }
          }
        }
        const top = Object.entries(counts).sort((a, b) => b[1] - a[1])[0];
        if (!top) return;
        window.postMessage({
          type:  'WHATNOT_CATEGORY',
          id:    varId,
          label: top[0],
        }, '*');
      }).catch(() => {});
    } catch (_) {}

    return resp;
  };
  wrapped.__wn_patched = true;
  window.fetch = wrapped;
})();
