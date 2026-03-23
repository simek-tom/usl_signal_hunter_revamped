/**
 * Content script injected into the Signal Hunter app tab.
 * Bridges chrome.runtime messages to window.postMessage so React can receive them.
 */

chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  if (message.action !== 'FILL_ENRICHMENT') return

  window.postMessage(
    { type: 'HUNTER_FILL_ENRICHMENT', payload: message.payload },
    '*',
  )

  sendResponse({ ok: true })
})
