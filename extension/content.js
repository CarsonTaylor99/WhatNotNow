// Runs in the ISOLATED content-script world. Bridges window.postMessage
// from inject.js (MAIN world) up to the background service worker.
window.addEventListener('message', (event) => {
  if (event.source !== window) return;
  const data = event.data;
  if (!data) return;

  if (data.type === 'WHATNOT_AUCTION_WS') {
    chrome.runtime.sendMessage({
      type: 'ws_url',
      url:  data.url,
      socketKind: data.socketKind,
    }, () => { void chrome.runtime.lastError; });
  }

  if (data.type === 'WHATNOT_PHX_JOIN') {
    chrome.runtime.sendMessage({
      type:    'phx_join',
      socketKind: data.socketKind,
      topic:   data.topic,
      payload: data.payload,
    }, () => { void chrome.runtime.lastError; });
  }
});
