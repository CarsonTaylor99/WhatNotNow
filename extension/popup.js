const $ = (id) => document.getElementById(id);

function fmtAgo(ms) {
  const s = Math.floor((Date.now() - ms) / 1000);
  if (s < 60) return `${s}s ago`;
  if (s < 3600) return `${Math.floor(s / 60)}m ago`;
  return `${Math.floor(s / 3600)}h ago`;
}

async function refreshStatus() {
  const resp = await chrome.runtime.sendMessage({ type: 'status' });
  if (resp.lastPushedAt) {
    $('status').innerHTML = `Last pushed <b>${fmtAgo(resp.lastPushedAt)}</b>`;
  } else {
    $('status').textContent = 'Never pushed yet';
  }
  $('hint').textContent = resp.hasUrl
    ? 'Auto-pushes when you open a livestream.'
    : 'Open a Whatnot livestream first to capture tokens.';
}

$('push-btn').addEventListener('click', async () => {
  $('push-btn').disabled    = true;
  $('push-btn').textContent = 'Pushing…';
  $('result').textContent   = '';
  $('result').className     = '';

  const resp = await chrome.runtime.sendMessage({ type: 'manual_push' });

  $('push-btn').disabled    = false;
  $('push-btn').textContent = 'Push now';
  if (resp.ok) {
    $('result').textContent = '✓ Tokens sent';
    $('result').className   = 'ok';
  } else {
    $('result').textContent = '✗ ' + (resp.error || 'Failed');
    $('result').className   = 'fail';
  }
  refreshStatus();
});

refreshStatus();
