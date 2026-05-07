// Runs in the page's MAIN world. Monkey-patches WebSocket so we can capture:
//   1. the socket URL (csrf_token, sessionExtensionToken, client_version)
//   2. every outgoing phx_join (topic + payload)
(() => {
  const Orig = window.WebSocket;
  if (!Orig || Orig.__wn_patched) return;

  const Patched = function (url, protocols) {
    const isWhatnot = typeof url === 'string' &&
      (url.includes('/auction/socket/websocket') || url.includes('/live/socket/websocket'));

    let socketKind = null;
    if (isWhatnot) {
      socketKind = url.includes('/auction/') ? 'auction' : 'live';
      try {
        window.postMessage({ type: 'WHATNOT_AUCTION_WS', url, socketKind }, '*');
      } catch (_) {}
    }

    const ws = protocols !== undefined ? new Orig(url, protocols) : new Orig(url);

    if (isWhatnot) {
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
})();
