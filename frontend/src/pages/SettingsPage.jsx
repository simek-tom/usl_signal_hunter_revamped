import { useEffect, useMemo, useState } from 'react'
import { Link } from 'react-router-dom'
import { api } from '../lib/api'
import MessageBox from '../components/MessageBox'

const NUMBER_KEYS = new Set([
  'push_row_limit',
  'news_default_page_size',
  'news_default_max_pages',
  'news_default_days_back',
])

const TEXTAREA_KEYS = new Set([
  'gemini_system_prompt',
  'news_default_query',
  'news_default_domains',
])

const GROUP_ORDER = ['Leadspicker', 'Airtable', 'NewsAPI', 'Gemini', 'Behavior']

function emptyToNull(value) {
  if (value === null || value === undefined) {
    return null
  }
  if (typeof value === 'string' && value.trim() === '') {
    return null
  }
  return value
}

function valueForInput(value) {
  if (value === null || value === undefined) {
    return ''
  }
  if (typeof value === 'object') {
    try {
      return JSON.stringify(value, null, 2)
    } catch {
      return String(value)
    }
  }
  return String(value)
}

export default function SettingsPage() {
  const [items, setItems] = useState([])
  const [draft, setDraft] = useState({})
  const [loading, setLoading] = useState(true)
  const [saving, setSaving] = useState(false)
  const [message, setMessage] = useState({ kind: 'info', text: '' })

  useEffect(() => {
    let alive = true

    async function load() {
      setLoading(true)
      setMessage({ kind: 'info', text: '' })
      try {
        const res = await api.getRuntimeSettings()
        if (!alive) {
          return
        }
        const rows = res.items || []
        setItems(rows)

        const nextDraft = {}
        for (const row of rows) {
          nextDraft[row.key] = valueForInput(row?.value)
        }
        setDraft(nextDraft)
      } catch (err) {
        if (alive) {
          setMessage({ kind: 'error', text: String(err.message || err) })
        }
      } finally {
        if (alive) {
          setLoading(false)
        }
      }
    }

    load()
    return () => {
      alive = false
    }
  }, [])

  const grouped = useMemo(() => {
    const map = new Map()
    for (const item of items) {
      const group = item.group || 'Other'
      if (!map.has(group)) {
        map.set(group, [])
      }
      map.get(group).push(item)
    }

    const ordered = []
    for (const g of GROUP_ORDER) {
      if (map.has(g)) {
        ordered.push([g, map.get(g)])
        map.delete(g)
      }
    }
    for (const [g, rows] of map.entries()) {
      ordered.push([g, rows])
    }
    return ordered
  }, [items])

  async function save() {
    if (saving) {
      return
    }

    setSaving(true)
    setMessage({ kind: 'info', text: '' })

    try {
      const payload = {}
      for (const row of items) {
        const key = row.key
        let value = draft[key]

        if (NUMBER_KEYS.has(key)) {
          const trimmed = String(value || '').trim()
          if (trimmed === '') {
            value = null
          } else {
            const parsed = Number(trimmed)
            value = Number.isFinite(parsed) ? parsed : trimmed
          }
        } else {
          value = emptyToNull(value)
        }

        if (key === 'gemini_system_prompt' && typeof value === 'string') {
          const trimmed = value.trim()
          if (trimmed) {
            try {
              value = JSON.parse(trimmed)
            } catch {
              value = trimmed
            }
          }
        }

        payload[key] = value
      }

      const res = await api.updateRuntimeSettings(payload)
      const rows = res.items || []
      setItems(rows)

      const nextDraft = {}
      for (const row of rows) {
        nextDraft[row.key] = valueForInput(row?.value)
      }
      setDraft(nextDraft)

      setMessage({
        kind: 'info',
        text: 'Settings saved. Empty values were cleared and now fall back to env/defaults.',
      })
    } catch (err) {
      setMessage({ kind: 'error', text: String(err.message || err) })
    } finally {
      setSaving(false)
    }
  }

  return (
    <>
      <section className="panel" style={{ display: 'grid', gap: '0.8rem' }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: '0.6rem', flexWrap: 'wrap' }}>
          <div>
            <h1 style={{ margin: 0, fontSize: '1.25rem' }}>Settings</h1>
            <div style={{ fontSize: '0.8rem', color: 'var(--ink-soft)' }}>
              Configure source/destination credentials and runtime defaults used by imports, pushes, and AI.
            </div>
          </div>
          <div style={{ display: 'flex', gap: '0.45rem', flexWrap: 'wrap' }}>
            <Link className="btn" to="/">
              Back Home
            </Link>
            <button className="btn primary" disabled={loading || saving} onClick={save}>
              {saving ? 'Saving...' : 'Save Settings'}
            </button>
          </div>
        </div>

        <MessageBox kind={message.kind} text={message.text} />
        {loading ? <MessageBox kind="info" text="Loading settings..." /> : null}
        <div style={{ fontSize: '0.76rem', color: 'var(--ink-soft)' }}>
          Note: leaving a field empty clears its override and reverts to `.env` or built-in defaults.
        </div>
      </section>

      {grouped.map(([groupName, rows]) => (
        <section key={groupName} className="panel" style={{ display: 'grid', gap: '0.6rem' }}>
          <h2 style={{ margin: 0, fontSize: '1rem' }}>{groupName}</h2>

          <div style={{ display: 'grid', gap: '0.55rem' }}>
            {rows.map((row) => {
              const inputType = row.secret ? 'password' : 'text'
              const key = row.key
              const value = draft[key] ?? ''
              const sourceLabel = row.source || 'unknown'

              return (
                <div key={key} style={{ display: 'grid', gap: '0.28rem' }}>
                  <label style={{ fontSize: '0.78rem', fontWeight: 700 }}>{row.label}</label>

                  {TEXTAREA_KEYS.has(key) ? (
                    <textarea
                      value={value}
                      onChange={(e) => setDraft((prev) => ({ ...prev, [key]: e.target.value }))}
                      placeholder={key}
                      style={{ minHeight: key === 'gemini_system_prompt' ? 140 : 86 }}
                    />
                  ) : (
                    <input
                      type={inputType}
                      value={value}
                      onChange={(e) => setDraft((prev) => ({ ...prev, [key]: e.target.value }))}
                      placeholder={key}
                    />
                  )}

                  <div style={{ fontSize: '0.72rem', color: 'var(--ink-soft)' }}>
                    key: <code>{key}</code> · source: <code>{sourceLabel}</code>
                  </div>
                </div>
              )
            })}
          </div>
        </section>
      ))}
    </>
  )
}
