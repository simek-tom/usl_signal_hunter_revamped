import { useState, useEffect, useRef } from 'react'
import { api } from '../lib/api'

const STYLES = {
  high: { background: '#fee2e2', color: '#991b1b', border: '1px solid #fca5a5' },
  medium: { background: '#fff7ed', color: '#9a3412', border: '1px solid #fdba74' },
}

export default function BlacklistBadge({ company_name, company_linkedin, company_website }) {
  const [result, setResult] = useState(null)
  const timerRef = useRef(null)

  useEffect(() => {
    // Debounce 300ms
    if (timerRef.current) clearTimeout(timerRef.current)

    if (!company_name && !company_linkedin && !company_website) {
      setResult(null)
      return
    }

    timerRef.current = setTimeout(async () => {
      try {
        const r = await api.checkBlacklist({ company_name, company_linkedin, company_website })
        setResult(r)
      } catch {
        setResult(null)
      }
    }, 300)

    return () => { if (timerRef.current) clearTimeout(timerRef.current) }
  }, [company_name, company_linkedin, company_website])

  if (!result || result.severity === 'low') return null

  const isHigh = result.severity === 'high'
  const style = isHigh ? STYLES.high : STYLES.medium

  return (
    <div style={{
      ...style,
      borderRadius: 8,
      padding: '0.4rem 0.7rem',
      fontSize: '0.78rem',
      fontWeight: 600,
      display: 'inline-flex',
      alignItems: 'center',
      gap: '0.4rem',
    }}>
      <span>{isHigh ? 'Already in System' : 'Potential Match'}</span>
      <span style={{ fontWeight: 400, fontSize: '0.72rem' }}>
        {result.match_type === 'linkedin_slug' && '(LinkedIn slug)'}
        {result.match_type === 'root_domain' && '(domain)'}
        {result.match_type === 'name_exact' && '(exact name)'}
        {result.match_type === 'name_substring' && '(name substring)'}
        {result.matched_entry && ` — ${result.matched_entry.company_name}`}
        {(result.matched_entry?.company_linkedin || company_linkedin) && (
          <button onClick={() => {
            if (company_linkedin) window.open(company_linkedin, '_blank')
            if (result.matched_entry?.company_linkedin) window.open(result.matched_entry.company_linkedin, '_blank')
          }} style={{
            marginLeft: '0.4rem', padding: '0.15rem 0.45rem', borderRadius: 4,
            background: isHigh ? '#fca5a5' : '#fdba74', border: 'none',
            color: style.color, fontWeight: 700, fontSize: '0.7rem',
            cursor: 'pointer', lineHeight: 1.4,
          }}>
            LinkedIn
          </button>
        )}
        {(result.matched_entry?.company_website || company_website) && (
          <button onClick={() => {
            const toUrl = u => u?.startsWith('http') ? u : `https://${u}`
            if (company_website) window.open(toUrl(company_website), '_blank')
            if (result.matched_entry?.company_website) window.open(toUrl(result.matched_entry.company_website), '_blank')
          }} style={{
            marginLeft: '0.3rem', padding: '0.15rem 0.45rem', borderRadius: 4,
            background: isHigh ? '#fca5a5' : '#fdba74', border: 'none',
            color: style.color, fontWeight: 700, fontSize: '0.7rem',
            cursor: 'pointer', lineHeight: 1.4,
          }}>
            Website
          </button>
        )}
      </span>
    </div>
  )
}
