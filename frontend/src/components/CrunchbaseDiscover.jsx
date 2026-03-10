import { useEffect, useMemo, useState } from 'react'
import md5 from 'js-md5'

function normalizeName(value) {
  return String(value || '').trim()
}

function slugifyCompanyName(value) {
  return normalizeName(value)
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
}

export function crunchbaseHash(companyName) {
  const normalized = normalizeName(companyName)
  if (!normalized) {
    return ''
  }
  return md5(normalized)
}

export function crunchbaseDiscoverUrl(companyName) {
  const hash = crunchbaseHash(companyName)
  if (!hash) {
    return ''
  }
  return `https://www.crunchbase.com/discover/organization.companies/${hash}`
}

export function crunchbaseTextSearchUrl(query) {
  const term = normalizeName(query)
  if (!term) {
    return ''
  }
  return `https://www.crunchbase.com/textsearch/?q=${encodeURIComponent(term)}`
}

export function crunchbaseOrganizationUrl(companyName) {
  const slug = slugifyCompanyName(companyName)
  if (!slug) {
    return ''
  }
  return `https://www.crunchbase.com/organization/${slug}`
}

function isMd5Hex(value) {
  return /^[a-f0-9]{32}$/i.test(String(value || '').trim())
}

export function crunchbaseOpenUrl(value) {
  const raw = normalizeName(value)
  if (!raw) {
    return ''
  }
  if (/^https?:\/\//i.test(raw)) {
    return raw
  }
  if (isMd5Hex(raw)) {
    return `https://www.crunchbase.com/discover/organization.companies/${raw.toLowerCase()}`
  }
  return crunchbaseOrganizationUrl(raw)
}

export default function CrunchbaseDiscover({
  companyName = '',
  withInput = false,
  buttonLabel = 'Open on Crunchbase',
  discoverButtonLabel = 'Open Discover (MD5)',
  placeholder = 'Company name',
  showDiscoverButton = true,
}) {
  const [term, setTerm] = useState(companyName || '')

  useEffect(() => {
    setTerm(companyName || '')
  }, [companyName])

  const url = useMemo(() => crunchbaseOpenUrl(term), [term])
  const discoverUrl = useMemo(() => {
    const raw = normalizeName(term)
    if (!raw) {
      return ''
    }
    if (isMd5Hex(raw)) {
      return `https://www.crunchbase.com/discover/organization.companies/${raw.toLowerCase()}`
    }
    if (/^https?:\/\//i.test(raw)) {
      return ''
    }
    return crunchbaseTextSearchUrl(raw)
  }, [term])

  function openCrunchbase() {
    if (!url) {
      return
    }
    window.open(url, '_blank', 'noopener,noreferrer')
  }

  function openCrunchbaseDiscover() {
    if (!discoverUrl) {
      return
    }
    window.open(discoverUrl, '_blank', 'noopener,noreferrer')
  }

  if (!withInput) {
    return (
      <div style={{ display: 'flex', gap: '0.4rem', flexWrap: 'wrap' }}>
        <button className="btn" disabled={!url} onClick={openCrunchbase}>
          {buttonLabel}
        </button>
        {showDiscoverButton ? (
          <button className="btn" disabled={!discoverUrl} onClick={openCrunchbaseDiscover}>
            {discoverButtonLabel}
          </button>
        ) : null}
      </div>
    )
  }

  return (
    <div style={{ display: 'grid', gap: '0.4rem' }}>
      <input
        value={term}
        placeholder={placeholder}
        onChange={(e) => setTerm(e.target.value)}
      />
      <div style={{ display: 'flex', gap: '0.4rem', flexWrap: 'wrap' }}>
        <button className="btn" disabled={!url} onClick={openCrunchbase}>
          {buttonLabel}
        </button>
        {showDiscoverButton ? (
          <button className="btn" disabled={!discoverUrl} onClick={openCrunchbaseDiscover}>
            {discoverButtonLabel}
          </button>
        ) : null}
      </div>
    </div>
  )
}
