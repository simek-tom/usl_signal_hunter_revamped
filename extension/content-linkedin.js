/**
 * Content script for LinkedIn pages.
 * Detects person vs company page, injects "Send to Hunter" button,
 * scrapes relevant data from the DOM on click.
 */

;(function () {
  'use strict'

  const BUTTON_ID = 'usl-hunter-send-btn'

  // ── Detection ────────────────────────────────────────────────────────────

  function getPageMode() {
    const path = window.location.pathname
    if (path.startsWith('/in/')) return 'person'
    if (path.startsWith('/company/')) return 'company'
    return null
  }

  // ── Selectors (LinkedIn changes these periodically) ──────────────────────

  function qs(selectors) {
    for (const sel of selectors) {
      const el = document.querySelector(sel)
      if (el) return el
    }
    return null
  }

  function cleanUrl(url) {
    try {
      const u = new URL(url)
      // Remove query params and trailing slash
      return u.origin + u.pathname.replace(/\/$/, '')
    } catch {
      return url
    }
  }

  // ── Person extraction ────────────────────────────────────────────────────

  function extractPerson() {
    // Name
    const nameEl = qs([
      'h1.text-heading-xlarge',
      '.pv-text-details__left-panel h1',
      '.ph5 h1',
    ])
    const name = nameEl?.innerText?.trim() || ''

    // Headline (position)
    const headlineEl = qs([
      '.text-body-medium.break-words',
      '.pv-text-details__left-panel .text-body-medium',
    ])
    const position = headlineEl?.innerText?.trim() || ''

    // LinkedIn URL
    const linkedinUrl = cleanUrl(window.location.href)

    // Company — try the experience/about section link
    let companyName = ''
    let companyLinkedin = ''
    const companyLink = qs([
      '.pv-text-details__right-panel a[href*="/company/"]',
      'a[data-field="experience_company_logo"][href*="/company/"]',
      '.experience-group-position a[href*="/company/"]',
      'button[aria-label] + a[href*="/company/"]',
    ])
    if (companyLink) {
      companyName = companyLink.innerText?.trim() || ''
      companyLinkedin = companyLink.href ? cleanUrl(companyLink.href) : ''
    }

    return {
      enriched_contact_name: name,
      enriched_contact_position: position,
      enriched_contact_linkedin: linkedinUrl,
      enriched_company_name: companyName,
      enriched_company_linkedin: companyLinkedin,
    }
  }

  // ── Company extraction ───────────────────────────────────────────────────

  function extractCompany() {
    // Company name
    const nameEl = qs([
      'h1.org-top-card-summary__title span',
      'h1.org-top-card-summary__title',
      '.org-top-card-summary-info-list + h1',
      'h1 span[dir="ltr"]',
    ])
    const companyName = nameEl?.innerText?.trim() || ''

    // LinkedIn URL
    const companyLinkedin = cleanUrl(window.location.href)

    // Website — from the About section or top card
    let website = ''
    const websiteLink = qs([
      '.org-top-card-primary-actions__inner a[href*="http"]',
      'a[data-control-name="top_card_website"]',
      '.org-about-company-module__company-page-url a',
      '.link-without-visited-state[href*="http"]',
    ])
    if (websiteLink) {
      website = websiteLink.href || ''
    }

    return {
      enriched_company_name: companyName,
      enriched_company_linkedin: companyLinkedin,
      enriched_company_website: website,
    }
  }

  // ── Button injection ─────────────────────────────────────────────────────

  function injectButton() {
    if (document.getElementById(BUTTON_ID)) return

    const mode = getPageMode()
    if (!mode) return

    const btn = document.createElement('button')
    btn.id = BUTTON_ID
    btn.className = 'usl-hunter-btn'
    btn.textContent = 'Send to Hunter'
    btn.addEventListener('click', () => {
      const payload = mode === 'person' ? extractPerson() : extractCompany()
      payload._mode = mode

      // Flash feedback
      btn.textContent = 'Sent!'
      btn.classList.add('usl-hunter-btn--sent')
      setTimeout(() => {
        btn.textContent = 'Send to Hunter'
        btn.classList.remove('usl-hunter-btn--sent')
      }, 1500)

      chrome.runtime.sendMessage({ action: 'FILL_ENRICHMENT', payload })
    })

    // Find anchor point
    const anchor = mode === 'person'
      ? qs(['.pv-top-card--list', '.pv-text-details__left-panel', '.ph5'])
      : qs(['.org-top-card-summary__title', '.org-top-card-primary-actions__inner'])

    if (anchor) {
      anchor.parentElement.insertBefore(btn, anchor.nextSibling)
    } else {
      // Fallback: fixed-position button
      btn.classList.add('usl-hunter-btn--floating')
      document.body.appendChild(btn)
    }
  }

  // ── Init ─────────────────────────────────────────────────────────────────

  // LinkedIn is an SPA — re-inject on URL changes
  let lastUrl = location.href
  const observer = new MutationObserver(() => {
    if (location.href !== lastUrl) {
      lastUrl = location.href
      const old = document.getElementById(BUTTON_ID)
      if (old) old.remove()
      setTimeout(injectButton, 1500) // wait for DOM to settle
    }
  })
  observer.observe(document.body, { childList: true, subtree: true })

  // Initial injection (with delay for LinkedIn's lazy rendering)
  if (document.readyState === 'complete') {
    setTimeout(injectButton, 1200)
  } else {
    window.addEventListener('load', () => setTimeout(injectButton, 1200))
  }
})()
