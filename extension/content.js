// Runs in the ISOLATED content-script world. Bridges window.postMessage
// from inject.js (MAIN world) up to the background service worker.
//
// Also relays runtime messages from background → inject.js for actions
// the SW can't perform itself (e.g., closing the page's WebSockets).
chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
  if (msg && msg.type === 'force_reconnect') {
    window.postMessage({ type: 'WHATNOT_FORCE_RECONNECT' }, '*');
    sendResponse({ ok: true });
  }
  return true;
});

window.addEventListener('message', (event) => {
  if (event.source !== window) return;
  const data = event.data;
  if (!data) return;

  if (data.type === 'WHATNOT_AUCTION_WS') {
    chrome.runtime.sendMessage({
      type: 'ws_url',
      url:  data.url,
      socketKind: data.socketKind,
      tokensChanged: data.tokensChanged,
    }, () => { void chrome.runtime.lastError; });
  }

  if (data.type === 'WHATNOT_RECONNECT_DONE') {
    chrome.runtime.sendMessage({
      type: 'reconnect_ack',
      closed: data.closed,
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

  if (data.type === 'WHATNOT_CATEGORY') {
    chrome.runtime.sendMessage({
      type:  'category',
      id:    data.id,
      label: data.label,
    }, () => { void chrome.runtime.lastError; });
  }
});
