/**
 * Background service worker.
 * Relays FILL_ENRICHMENT messages from LinkedIn tabs to the app tab.
 */

chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  if (message.action !== 'FILL_ENRICHMENT') return

  // Find the Signal Hunter app tab
  chrome.tabs.query(
    { url: ['http://localhost:8000/*', 'http://localhost:5173/*', 'http://127.0.0.1:8000/*'] },
    (tabs) => {
      if (!tabs || tabs.length === 0) {
        console.warn('[Hunter] No Signal Hunter app tab found.')
        sendResponse({ ok: false, error: 'App tab not found' })
        return
      }

      // Send to the first matching tab
      const appTab = tabs[0]
      chrome.tabs.sendMessage(
        appTab.id,
        { action: 'FILL_ENRICHMENT', payload: message.payload },
        (response) => {
          if (chrome.runtime.lastError) {
            console.warn('[Hunter] Failed to relay:', chrome.runtime.lastError.message)
            sendResponse({ ok: false, error: chrome.runtime.lastError.message })
          } else {
            sendResponse({ ok: true })
          }
        },
      )
    },
  )

  // Keep the message channel open for async sendResponse
  return true
})
