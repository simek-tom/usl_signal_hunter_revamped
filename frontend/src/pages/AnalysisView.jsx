import { useCallback, useEffect, useMemo, useState } from 'react'
import { Link, useParams, useSearchParams } from 'react-router-dom'
import { api } from '../lib/api'
import { highlightText } from '../lib/keywords'
import MessageBox from '../components/MessageBox'
import ProgressBar from '../components/ProgressBar'
import CrunchbaseDiscover from '../components/CrunchbaseDiscover'

const LINKEDIN_KEYWORDS = [
  'country manager',
  'business development',
  'partnerships',
  'sales lead',
  'head of growth',
  'founder',
]

function normalizeText(value) {
  return String(value || '')
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
}

function normalizeDomain(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
    .replace(/^https?:\/\//, '')
    .replace(/^www\./, '')
    .split('/')[0]
    .split('?')[0]
    .split('#')[0]
}

function cbMeta(signal) {
  const raw = signal?.source_metadata
  return raw && typeof raw === 'object' ? raw : {}
}

function firstPresent(obj, keys) {
  for (const key of keys) {
    const val = obj?.[key]
    if (val !== undefined && val !== null && String(val).trim()) {
      return val
    }
  }
  return ''
}

function isLinkedinLeadspickerPipeline(pipelineType) {
  return ['lp_general', 'lp_czech', 'linkedin', 'leadspicker'].includes(String(pipelineType || ''))
}

export default function AnalysisView() {
  const { batchId } = useParams()
  const [searchParams] = useSearchParams()

  const [loading, setLoading] = useState(true)
  const [entries, setEntries] = useState([])
  const [currentIndex, setCurrentIndex] = useState(0)
  const [context, setContext] = useState({ blacklist: [], contacted_fingerprints: [] })

  const [searchQuery, setSearchQuery] = useState('')
  const [searchResult, setSearchResult] = useState(null)

  const [enrichOpen, setEnrichOpen] = useState(false)
  const [enrichForm, setEnrichForm] = useState({
    full_name: '',
    linkedin_url: '',
    relation_to_company: '',
    company_name: '',
    company_website: '',
    company_linkedin: '',
  })
  const [cbForm, setCbForm] = useState({
    message_fin: '',
    main_contact: '',
    secondary_contact_1: '',
    secondary_contact_2: '',
    secondary_contact_3: '',
  })

  const [message, setMessage] = useState({ kind: 'info', text: '' })
  const [busy, setBusy] = useState(false)

  const aiFilterEnabled = useMemo(() => {
    const raw = String(searchParams.get('ai_filter') || '')
      .trim()
      .toLowerCase()
    return raw === 'yes' || raw === 'true' || raw === '1'
  }, [searchParams])

  useEffect(() => {
    let alive = true

    async function load() {
      setLoading(true)
      setMessage({ kind: 'info', text: '' })
      try {
        const [entriesData, contextData] = await Promise.all([
          api.getBatchEntries(batchId),
          api.getBatchContext(batchId),
        ])
        if (!alive) {
          return
        }

        const filteredEntries = aiFilterEnabled
          ? entriesData.filter((row) => String(row?.signals?.ai_classifier || '').trim().toLowerCase() !== 'no')
          : entriesData
        setEntries(filteredEntries)
        setContext(contextData)
        setCurrentIndex(0)
        if (aiFilterEnabled && filteredEntries.length !== entriesData.length) {
          setMessage({
            kind: 'info',
            text: `AI classifier filter active: ${entriesData.length - filteredEntries.length} NO rows hidden.`,
          })
        }
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
  }, [batchId, aiFilterEnabled])

  const current = entries[currentIndex] || null
  const signal = current?.signals || {}
  const company = signal?.companies || {}
  const contact = current?.contacts || {}
  const aiClassifier = String(signal.ai_classifier || '').trim()
  const aiClassifierLower = aiClassifier.toLowerCase()
  const isCrunchbase = current?.pipeline_type === 'crunchbase'
  const isNews = current?.pipeline_type === 'news'
  const isLinkedinLeadspicker = isLinkedinLeadspickerPipeline(current?.pipeline_type)
  const meta = cbMeta(signal)

  const blacklistedTerms = useMemo(
    () =>
      (context.blacklist || [])
        .flatMap((row) => [row.company_name, row.company_name_normalized])
        .filter(Boolean)
        .map((s) => normalizeText(s)),
    [context.blacklist],
  )

  const contactedFingerprints = useMemo(
    () => new Set((context.contacted_fingerprints || []).map((row) => row.fingerprint).filter(Boolean)),
    [context.contacted_fingerprints],
  )

  const isBlacklisted = useMemo(() => {
    const haystack = normalizeText(`${company.name_raw || ''} ${signal.content_text || ''} ${signal.content_summary || ''}`)
    if (!haystack) {
      return false
    }
    return blacklistedTerms.some((term) => term && haystack.includes(term))
  }, [blacklistedTerms, company.name_raw, signal.content_text, signal.content_summary])

  const inferredNewsFingerprint = useMemo(() => {
    if (!isNews) {
      return ''
    }
    const name = String(enrichForm.company_name || company.name_raw || '')
      .trim()
      .toLowerCase()
    const domain = normalizeDomain(enrichForm.company_website || company.website || company.domain_normalized || '')
    if (!name && !domain) {
      return ''
    }
    return `${name}|${domain}`
  }, [isNews, enrichForm.company_name, enrichForm.company_website, company.name_raw, company.website, company.domain_normalized])

  const isAlreadyContacted = Boolean(
    (company.fingerprint && contactedFingerprints.has(company.fingerprint))
      || (inferredNewsFingerprint && contactedFingerprints.has(inferredNewsFingerprint)),
  )

  const progress = useMemo(() => {
    const total = entries.length
    let labeled = 0
    let yes = 0
    let no = 0
    let cc = 0
    let pushedReady = 0
    for (const e of entries) {
      if (e.relevant) {
        labeled += 1
      }
      if (e.relevant === 'yes') {
        yes += 1
      }
      if (e.relevant === 'no') {
        no += 1
      }
      if (e.relevant === 'cc') {
        cc += 1
      }
      const eMeta = cbMeta(e?.signals || {})
      if (String(e.status || eMeta.entry_workflow_status || '').trim().toLowerCase() === 'pushed-ready') {
        pushedReady += 1
      }
    }
    return { total, labeled, yes, no, cc, pushedReady }
  }, [entries])

  useEffect(() => {
    if (!current) {
      return
    }
    if (isCrunchbase) {
      setCbForm({
        message_fin: String(meta.message_fin || meta['Message fin'] || ''),
        main_contact: String(meta.main_contact || meta['Main Contact'] || contact.linkedin_url || ''),
        secondary_contact_1: String(meta.secondary_contact_1 || meta['Secondary Contact #1'] || ''),
        secondary_contact_2: String(meta.secondary_contact_2 || meta['Secondary Contact #2'] || ''),
        secondary_contact_3: String(meta.secondary_contact_3 || meta['Secondary Contact #3'] || ''),
      })
      return
    }

    setEnrichForm({
      full_name: contact.full_name || `${contact.first_name || ''} ${contact.last_name || ''}`.trim(),
      linkedin_url: contact.linkedin_url || '',
      relation_to_company: contact.relation_to_company || '',
      company_name: company.name_raw || '',
      company_website: company.website || '',
      company_linkedin: company.linkedin_url || '',
    })

    if (current.relevant !== 'yes') {
      setEnrichOpen(false)
    }
  }, [currentIndex, current, company, contact, isCrunchbase, meta])

  const goNext = useCallback(() => {
    setCurrentIndex((idx) => Math.min(entries.length - 1, idx + 1))
  }, [entries.length])

  const goPrev = useCallback(() => {
    setCurrentIndex((idx) => Math.max(0, idx - 1))
  }, [])

  async function applyLabel(label, learningData = null) {
    if (!current) {
      return
    }

    setBusy(true)
    try {
      await api.labelEntry({
        entryId: current.id,
        label,
        learningData,
      })

      setEntries((prev) => {
        const copy = [...prev]
        copy[currentIndex] = {
          ...copy[currentIndex],
          relevant: label,
          learning_data: Boolean(learningData),
          status: 'analyzed',
          analyzed_at: new Date().toISOString(),
        }
        return copy
      })

      if (label === 'yes') {
        setEnrichOpen(true)
      }

      setMessage({ kind: 'info', text: `Saved label ${label.toUpperCase()}.` })
    } catch (err) {
      setMessage({ kind: 'error', text: String(err.message || err) })
    } finally {
      setBusy(false)
    }
  }

  async function saveEnrichment() {
    if (!current) {
      return
    }

    setBusy(true)
    try {
      const res = await api.enrichEntry({ entryId: current.id, payload: enrichForm })
      setEntries((prev) => {
        const copy = [...prev]
        copy[currentIndex] = res.entry
        return copy
      })
      setMessage({ kind: 'info', text: `Enrichment saved (${(res.contacted_history || []).length} prior contacts).` })
    } catch (err) {
      setMessage({ kind: 'error', text: String(err.message || err) })
    } finally {
      setBusy(false)
    }
  }

  async function saveCrunchbaseAction(action, { advance = false } = {}) {
    if (!current) {
      return
    }
    setBusy(true)
    try {
      const res = await api.crunchbaseAction({
        entryId: current.id,
        payload: {
          action,
          ...cbForm,
        },
      })

      if (action === 'eliminate') {
        setEntries((prev) => prev.filter((_, idx) => idx !== currentIndex))
        setCurrentIndex((idx) => Math.max(0, Math.min(idx, entries.length - 2)))
        setMessage({ kind: 'info', text: 'Entry eliminated and skipped.' })
      } else {
        setEntries((prev) => {
          const copy = [...prev]
          copy[currentIndex] = res.entry
          return copy
        })
        if (advance) {
          setCurrentIndex((idx) => Math.min(entries.length - 1, idx + 1))
        }
        const label = action === 'save_next' ? 'Saved + moved next.' : 'Saved.'
        setMessage({ kind: 'info', text: label })
      }
    } catch (err) {
      setMessage({ kind: 'error', text: String(err.message || err) })
    } finally {
      setBusy(false)
    }
  }

  async function finishLabeling() {
    setBusy(true)
    try {
      const res = await api.finishLabeling(batchId)
      setMessage({ kind: 'info', text: `Finish Labeling complete. Labeled: ${res.total_labeled}, written: ${res.written}.` })
    } catch (err) {
      setMessage({ kind: 'error', text: String(err.message || err) })
    } finally {
      setBusy(false)
    }
  }

  async function runSearch() {
    if (!searchQuery.trim()) {
      setSearchResult(null)
      return
    }

    try {
      const res = await api.searchMaster(searchQuery.trim())
      setSearchResult(res)
    } catch (err) {
      setMessage({ kind: 'error', text: String(err.message || err) })
    }
  }

  useEffect(() => {
    function onKey(e) {
      const tag = document.activeElement?.tagName?.toLowerCase()
      const inInput = tag === 'input' || tag === 'textarea' || tag === 'select'
      if (inInput) {
        return
      }

      if (e.key === 'ArrowRight') {
        e.preventDefault()
        goNext()
      }
      if (e.key === 'ArrowLeft') {
        e.preventDefault()
        goPrev()
      }

      if (isCrunchbase) {
        if (e.key.toLowerCase() === 'y') {
          e.preventDefault()
          saveCrunchbaseAction('yes')
        }
        if (e.key.toLowerCase() === 'n') {
          e.preventDefault()
          saveCrunchbaseAction('eliminate', { advance: true })
        }
        return
      }

      if (e.key.toLowerCase() === 'y') {
        e.preventDefault()
        applyLabel('yes')
      }
      if (e.key.toLowerCase() === 'n') {
        e.preventDefault()
        applyLabel('no')
      }
      if (e.key.toLowerCase() === 'c' && current?.pipeline_type === 'lp_czech') {
        e.preventDefault()
        applyLabel('cc')
      }
    }

    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [goNext, goPrev, current?.pipeline_type, isCrunchbase, cbForm, currentIndex, entries.length])

  if (loading) {
    return <MessageBox kind="info" text="Loading analysis data..." />
  }

  if (!current) {
    return (
      <section className="panel" style={{ display: 'grid', gap: '0.6rem' }}>
        <h1 style={{ margin: 0 }}>Analysis</h1>
        <MessageBox
          kind="warn"
          text={aiFilterEnabled ? 'No entries available after AI classifier filter.' : 'No entries available in this batch.'}
        />
        <Link className="btn" to="/">
          Back to Dashboard
        </Link>
      </section>
    )
  }

  const cbSeries = firstPresent(meta, ['series', 'Series', 'Funding Stage'])
  const cbFunding = firstPresent(meta, ['last_funding_amount_usd', 'Last Funding Amount', 'Funding'])
  const cbIndustries = firstPresent(meta, ['industries', 'Industries', 'Industry']) || company.industry || 'n/a'
  const cbHq = firstPresent(meta, ['hq_location', 'Headquarters Location', 'HQ']) || company.hq_location || 'n/a'
  const cbDescription = firstPresent(meta, ['description', 'Description']) || signal.content_text || company.description || 'No description.'
  const cbStatus = firstPresent(meta, ['status', 'Status']) || 'n/a'
  const cbEntryStatus = String(current.status || meta.entry_workflow_status || 'new')
  const cbCrunchbaseUrl =
    company.crunchbase_profile_url ||
    firstPresent(meta, ['Crunchbase URL', 'Crunchbase profile URL']) ||
    signal.content_url

  return (
    <>
      <section className="panel" style={{ display: 'grid', gap: '0.65rem' }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: '0.8rem', flexWrap: 'wrap' }}>
          <div>
            <h1 style={{ margin: 0, fontSize: '1.25rem' }}>Analysis · Batch {batchId.slice(0, 8)}...</h1>
            <div style={{ fontSize: '0.76rem', color: 'var(--ink-soft)' }}>
              Hotkeys: <span className="kbd">Y</span> YES · <span className="kbd">N</span> {isCrunchbase ? 'Eliminate' : 'NO'} · <span className="kbd">←/→</span> nav
            </div>
            {aiFilterEnabled ? (
              <div style={{ fontSize: '0.74rem', color: 'var(--ink-soft)' }}>Filter enabled: AI classifier NO rows hidden.</div>
            ) : null}
          </div>
          <div style={{ display: 'flex', gap: '0.45rem', flexWrap: 'wrap' }}>
            <Link className="btn" to={`/pipeline/${current.pipeline_type}`}>
              Pipeline
            </Link>
            <button className="btn warn" disabled={busy} onClick={finishLabeling}>
              Finish Labeling
            </button>
            {!isCrunchbase ? (
              <Link className="btn" to={`/draft/${batchId}`}>
                Go to Drafting
              </Link>
            ) : null}
          </div>
        </div>

        <MessageBox kind={message.kind} text={message.text} />

        <div style={{ display: 'grid', gap: '0.4rem' }}>
          <div style={{ fontSize: '0.78rem', color: 'var(--ink-soft)' }}>
            Entry {currentIndex + 1}/{entries.length} · YES {progress.yes} · NO {progress.no} · CC {progress.cc}
            {isCrunchbase ? ` · pushed-ready ${progress.pushedReady}` : ''}
          </div>
          <ProgressBar value={progress.labeled} total={Math.max(progress.total, 1)} />
        </div>
      </section>

      {isCrunchbase ? (
        <section className="grid-2" style={{ alignItems: 'start' }}>
          <article className="panel" style={{ display: 'grid', gap: '0.7rem' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'start', gap: '0.7rem' }}>
              <div>
                <div style={{ fontWeight: 700, fontSize: '1rem' }}>{company.name_raw || firstPresent(meta, ['Name']) || 'Unknown company'}</div>
                <div style={{ fontSize: '0.8rem', color: 'var(--ink-soft)' }}>
                  {company.website ? (
                    <a href={company.website} target="_blank" rel="noreferrer">
                      {company.website}
                    </a>
                  ) : (
                    'No company website'
                  )}
                </div>
              </div>

              <span className={`badge ${current.ai_pre_score > 0.7 ? 'green' : current.ai_pre_score > 0.4 ? 'amber' : 'gray'}`}>
                AI score {Number(current.ai_pre_score || 0).toFixed(2)}
              </span>
            </div>

            <div style={{ display: 'flex', gap: '0.5rem', flexWrap: 'wrap' }}>
              {isBlacklisted ? <span className="badge red">BLACKLISTED</span> : null}
              {isAlreadyContacted ? <span className="badge amber">ALREADY CONTACTED</span> : null}
              <span className="badge gray">CB status: {String(cbStatus)}</span>
              <span className={`badge ${String(cbEntryStatus).toLowerCase() === 'pushed-ready' ? 'green' : 'amber'}`}>
                Entry status: {cbEntryStatus}
              </span>
              {current.relevant ? <span className="badge green">Label {current.relevant.toUpperCase()}</span> : null}
            </div>

            <div style={{ display: 'grid', gap: '0.35rem', fontSize: '0.84rem' }}>
              <div><strong>Series:</strong> {String(cbSeries || 'n/a')}</div>
              <div><strong>Funding:</strong> {String(cbFunding || 'n/a')}</div>
              <div><strong>Industries:</strong> {String(cbIndustries || 'n/a')}</div>
              <div><strong>HQ:</strong> {String(cbHq || 'n/a')}</div>
            </div>

            <div className="panel" style={{ margin: 0, padding: '0.8rem', background: '#fff' }}>
              <div style={{ fontSize: '0.76rem', color: 'var(--ink-soft)', marginBottom: '0.4rem' }}>Description</div>
              <div style={{ whiteSpace: 'pre-wrap', fontSize: '0.82rem' }}>{String(cbDescription)}</div>
            </div>

            <div className="panel" style={{ margin: 0, padding: '0.8rem', background: '#fff' }}>
              <div style={{ fontSize: '0.76rem', color: 'var(--ink-soft)', marginBottom: '0.4rem' }}>Summary</div>
              <div style={{ whiteSpace: 'pre-wrap', fontSize: '0.82rem' }}>{signal.content_summary || 'No summary.'}</div>
            </div>

            <div style={{ display: 'flex', gap: '0.45rem', flexWrap: 'wrap' }}>
              {company.linkedin_url ? (
                <a className="btn" href={company.linkedin_url} target="_blank" rel="noreferrer">
                  Company LinkedIn
                </a>
              ) : null}
              {cbCrunchbaseUrl ? (
                <a className="btn" href={cbCrunchbaseUrl} target="_blank" rel="noreferrer">
                  Crunchbase Link
                </a>
              ) : null}
              <CrunchbaseDiscover companyName={company.name_raw || firstPresent(meta, ['Name']) || ''} />
            </div>
          </article>

          <aside style={{ display: 'grid', gap: '0.7rem' }}>
            <section className="panel" style={{ display: 'grid', gap: '0.55rem' }}>
              <h2 style={{ margin: 0, fontSize: '0.95rem' }}>CB Analysis Actions</h2>
              <textarea
                value={cbForm.message_fin}
                placeholder="Message fin"
                onChange={(e) => setCbForm((f) => ({ ...f, message_fin: e.target.value }))}
                style={{ minHeight: 120 }}
              />
              <input
                value={cbForm.main_contact}
                placeholder="Main Contact (LinkedIn URL)"
                onChange={(e) => setCbForm((f) => ({ ...f, main_contact: e.target.value }))}
              />
              <input
                value={cbForm.secondary_contact_1}
                placeholder="Secondary Contact #1"
                onChange={(e) => setCbForm((f) => ({ ...f, secondary_contact_1: e.target.value }))}
              />
              <input
                value={cbForm.secondary_contact_2}
                placeholder="Secondary Contact #2"
                onChange={(e) => setCbForm((f) => ({ ...f, secondary_contact_2: e.target.value }))}
              />
              <input
                value={cbForm.secondary_contact_3}
                placeholder="Secondary Contact #3"
                onChange={(e) => setCbForm((f) => ({ ...f, secondary_contact_3: e.target.value }))}
              />

              <div style={{ display: 'flex', gap: '0.45rem', flexWrap: 'wrap' }}>
                <button className="btn primary" disabled={busy} onClick={() => saveCrunchbaseAction('yes')}>
                  YES <span className="kbd">Y</span>
                </button>
                <button className="btn danger" disabled={busy} onClick={() => saveCrunchbaseAction('eliminate', { advance: true })}>
                  Eliminate
                </button>
                <button className="btn" disabled={busy} onClick={() => saveCrunchbaseAction('uneliminate')}>
                  Un-eliminate
                </button>
                <button className="btn warn" disabled={busy} onClick={() => saveCrunchbaseAction('save_next', { advance: true })}>
                  Save & Next
                </button>
                <button className="btn" disabled={busy} onClick={() => saveCrunchbaseAction('save_stay')}>
                  Save & Stay
                </button>
              </div>
              <div style={{ display: 'flex', gap: '0.45rem', flexWrap: 'wrap' }}>
                <button className="btn" onClick={goPrev}>
                  Back
                </button>
                <button className="btn" onClick={goNext}>
                  Skip
                </button>
              </div>
            </section>

            <section className="panel" style={{ display: 'grid', gap: '0.55rem' }}>
              <h2 style={{ margin: 0, fontSize: '0.95rem' }}>Master Search</h2>
              <div style={{ display: 'flex', gap: '0.45rem' }}>
                <input
                  placeholder="Search company"
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                  onKeyDown={(e) => {
                    if (e.key === 'Enter') {
                      runSearch()
                    }
                  }}
                />
                <button className="btn" onClick={runSearch}>
                  Search
                </button>
              </div>

              {searchResult ? (
                <div style={{ display: 'grid', gap: '0.45rem', fontSize: '0.8rem' }}>
                  <div><strong>Exact:</strong> {(searchResult.exact || []).length}</div>
                  {(searchResult.exact || []).slice(0, 4).map((row) => (
                    <div key={row.id} style={{ borderBottom: '1px dashed var(--line)', paddingBottom: '0.35rem' }}>
                      {row.name_raw} · {row.domain_normalized || 'no-domain'}
                    </div>
                  ))}
                  <div><strong>Partial:</strong> {(searchResult.partial || []).length}</div>
                  {(searchResult.partial || []).slice(0, 4).map((row) => (
                    <div key={row.id} style={{ borderBottom: '1px dashed var(--line)', paddingBottom: '0.35rem' }}>
                      {row.name_raw} · {row.domain_normalized || 'no-domain'}
                    </div>
                  ))}
                </div>
              ) : null}
            </section>
          </aside>
        </section>
      ) : (
        <section className="grid-2" style={{ alignItems: 'start' }}>
          <article className="panel" style={{ display: 'grid', gap: '0.7rem' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'start', gap: '0.7rem' }}>
              <div>
                <div style={{ fontWeight: 700 }}>
                  {isNews
                    ? signal.author_name || 'Unknown author'
                    : contact.full_name || `${contact.first_name || ''} ${contact.last_name || ''}`.trim() || 'Unknown author'}
                </div>
                <div style={{ fontSize: '0.8rem', color: 'var(--ink-soft)' }}>
                  {!isNews && contact.linkedin_url ? (
                    <a href={contact.linkedin_url} target="_blank" rel="noreferrer">
                      {contact.linkedin_url}
                    </a>
                  ) : isNews && signal.source_metadata?.source_name ? (
                    `Source: ${signal.source_metadata.source_name}`
                  ) : (
                    isNews ? 'No source metadata' : 'No author profile'
                  )}
                </div>
              </div>

              <span className={`badge ${current.ai_pre_score > 0.7 ? 'green' : current.ai_pre_score > 0.4 ? 'amber' : 'gray'}`}>
                AI score {Number(current.ai_pre_score || 0).toFixed(2)}
              </span>
            </div>

            {isNews ? (
              <div style={{ display: 'grid', gap: '0.35rem', fontSize: '0.84rem' }}>
                <div>
                  <strong>Headline:</strong> {signal.content_title || 'Untitled'}
                </div>
                <div>
                  <strong>Published:</strong>{' '}
                  {signal.published_at ? new Date(signal.published_at).toLocaleString() : 'n/a'}
                </div>
                <div>
                  <strong>Article URL:</strong>{' '}
                  {signal.content_url ? (
                    <a href={signal.content_url} target="_blank" rel="noreferrer" style={{ textDecoration: 'underline' }}>
                      {signal.content_url}
                    </a>
                  ) : (
                    'n/a'
                  )}
                </div>
                <div>
                  <strong>Company (enrichment):</strong>{' '}
                  {company.name_raw || enrichForm.company_name || 'Not identified yet'}
                </div>
              </div>
            ) : (
              <>
                <div style={{ fontSize: '0.86rem' }}>
                  <strong>Company:</strong>{' '}
                  {company.linkedin_url ? (
                    <a href={company.linkedin_url} target="_blank" rel="noreferrer" style={{ textDecoration: 'underline' }}>
                      {company.name_raw || 'Unknown'}
                    </a>
                  ) : company.website ? (
                    <a href={company.website} target="_blank" rel="noreferrer" style={{ textDecoration: 'underline' }}>
                      {company.name_raw || company.website}
                    </a>
                  ) : (
                    company.name_raw || 'Unknown'
                  )}
                </div>

                <div style={{ fontSize: '0.82rem', color: 'var(--ink-soft)' }}>
                  <strong>Position:</strong> {contact.relation_to_company || 'n/a'}
                </div>
              </>
            )}

            {isNews && signal.source_metadata?.url_to_image ? (
              <img
                src={signal.source_metadata.url_to_image}
                alt="Article"
                style={{ width: '100%', maxHeight: 220, objectFit: 'cover', borderRadius: 12, border: '1px solid var(--line)' }}
              />
            ) : null}

            <div style={{ display: 'flex', gap: '0.5rem', flexWrap: 'wrap' }}>
              {isBlacklisted ? <span className="badge red">BLACKLISTED</span> : null}
              {isAlreadyContacted ? <span className="badge amber">ALREADY CONTACTED</span> : null}
              {aiClassifier ? (
                <span className={`badge ${aiClassifierLower === 'yes' ? 'green' : aiClassifierLower === 'no' ? 'red' : 'gray'}`}>
                  AI classifier: {aiClassifier}
                </span>
              ) : (
                <span className="badge gray">AI classifier: n/a</span>
              )}
              {current.learning_data ? <span className="badge gray">Learning Data</span> : null}
              {current.relevant ? <span className="badge green">Label {current.relevant.toUpperCase()}</span> : null}
            </div>

            <div className="panel" style={{ margin: 0, padding: '0.8rem', background: '#fff' }}>
              <div style={{ fontSize: '0.76rem', color: 'var(--ink-soft)', marginBottom: '0.45rem' }}>
                {isNews ? 'Article content' : 'Post text'}
              </div>
              <div style={{ whiteSpace: 'pre-wrap', lineHeight: 1.45, fontSize: '0.84rem' }}>
                {highlightText(signal.content_text || '').map((part, idx) =>
                  part.hit ? (
                    <mark className="marked-hit" key={idx}>
                      {part.value}
                    </mark>
                  ) : (
                    <span key={idx}>{part.value}</span>
                  ),
                )}
              </div>
            </div>

            <div className="panel" style={{ margin: 0, padding: '0.8rem', background: '#fff' }}>
              <div style={{ fontSize: '0.76rem', color: 'var(--ink-soft)', marginBottom: '0.4rem' }}>
                {isNews ? 'Article summary' : 'Summary'}
              </div>
              <div style={{ whiteSpace: 'pre-wrap', fontSize: '0.82rem' }}>{signal.content_summary || 'No summary.'}</div>
            </div>

            <div style={{ display: 'flex', gap: '0.45rem', flexWrap: 'wrap' }}>
              {isLinkedinLeadspicker && signal.content_url ? (
                <a className="btn" href={signal.content_url} target="_blank" rel="noreferrer">
                  Open LinkedIn post
                </a>
              ) : null}
              <CrunchbaseDiscover companyName={enrichForm.company_name || company.name_raw || ''} />
            </div>

            <div style={{ display: 'flex', gap: '0.45rem', flexWrap: 'wrap' }}>
              <button className="btn primary" disabled={busy} onClick={() => applyLabel('yes')}>
                YES <span className="kbd">Y</span>
              </button>
              <button className="btn" disabled={busy} onClick={() => applyLabel('no')}>
                NO <span className="kbd">N</span>
              </button>
              <button className="btn" disabled={busy} onClick={() => applyLabel('no', true)}>
                NO + Learning
              </button>
              {current.pipeline_type === 'lp_czech' && !isNews ? (
                <button className="btn cc" disabled={busy} onClick={() => applyLabel('cc')}>
                  CC <span className="kbd">C</span>
                </button>
              ) : null}
              <button className="btn" onClick={goPrev}>
                Back
              </button>
              <button className="btn" onClick={goNext}>
                Skip
              </button>
            </div>
          </article>

          <aside style={{ display: 'grid', gap: '0.7rem' }}>
            <section className="panel" style={{ display: 'grid', gap: '0.55rem' }}>
              <h2 style={{ margin: 0, fontSize: '0.95rem' }}>Master Search</h2>
              <div style={{ display: 'flex', gap: '0.45rem' }}>
                <input
                  placeholder="Search company"
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                  onKeyDown={(e) => {
                    if (e.key === 'Enter') {
                      runSearch()
                    }
                  }}
                />
                <button className="btn" onClick={runSearch}>
                  Search
                </button>
              </div>

              {searchResult ? (
                <div style={{ display: 'grid', gap: '0.45rem', fontSize: '0.8rem' }}>
                  <div>
                    <strong>Exact:</strong> {(searchResult.exact || []).length}
                  </div>
                  {(searchResult.exact || []).slice(0, 4).map((row) => (
                    <div key={row.id} style={{ borderBottom: '1px dashed var(--line)', paddingBottom: '0.35rem' }}>
                      {row.name_raw} · {row.domain_normalized || 'no-domain'}
                    </div>
                  ))}
                  <div>
                    <strong>Partial:</strong> {(searchResult.partial || []).length}
                  </div>
                  {(searchResult.partial || []).slice(0, 4).map((row) => (
                    <div key={row.id} style={{ borderBottom: '1px dashed var(--line)', paddingBottom: '0.35rem' }}>
                      {row.name_raw} · {row.domain_normalized || 'no-domain'}
                    </div>
                  ))}
                </div>
              ) : null}
            </section>

            <section className="panel" style={{ display: 'grid', gap: '0.55rem' }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <h2 style={{ margin: 0, fontSize: '0.95rem' }}>Enrichment</h2>
                <button className="btn" onClick={() => setEnrichOpen((v) => !v)}>
                  {enrichOpen ? 'Collapse' : 'Expand'}
                </button>
              </div>
              {isNews ? (
                <div style={{ fontSize: '0.78rem', color: 'var(--ink-soft)' }}>
                  News flow: identify company first, then contact. This creates company/contact from NULL import values.
                </div>
              ) : null}

              {enrichOpen ? (
                <>
                  <input
                    value={enrichForm.full_name}
                    placeholder="Full name"
                    onChange={(e) => setEnrichForm((f) => ({ ...f, full_name: e.target.value }))}
                  />
                  <input
                    value={enrichForm.linkedin_url}
                    placeholder="LinkedIn URL"
                    onChange={(e) => setEnrichForm((f) => ({ ...f, linkedin_url: e.target.value }))}
                  />
                  <input
                    value={enrichForm.relation_to_company}
                    placeholder="Relation / Position"
                    onChange={(e) => setEnrichForm((f) => ({ ...f, relation_to_company: e.target.value }))}
                  />
                  <input
                    value={enrichForm.company_name}
                    placeholder="Company name"
                    onChange={(e) => setEnrichForm((f) => ({ ...f, company_name: e.target.value }))}
                  />
                  {isNews && isAlreadyContacted ? (
                    <span className="badge amber">ALREADY CONTACTED (typed company)</span>
                  ) : null}
                  <input
                    value={enrichForm.company_website}
                    placeholder="Company website"
                    onChange={(e) => setEnrichForm((f) => ({ ...f, company_website: e.target.value }))}
                  />
                  <input
                    value={enrichForm.company_linkedin}
                    placeholder="Company LinkedIn"
                    onChange={(e) => setEnrichForm((f) => ({ ...f, company_linkedin: e.target.value }))}
                  />

                  <div style={{ display: 'flex', gap: '0.45rem', flexWrap: 'wrap' }}>
                    <button className="btn primary" disabled={busy} onClick={saveEnrichment}>
                      Save Enrichment
                    </button>
                    <CrunchbaseDiscover
                      withInput
                      companyName={enrichForm.company_name || company.name_raw || ''}
                      buttonLabel="Open on Crunchbase"
                    />
                  </div>

                  <div style={{ display: 'flex', flexWrap: 'wrap', gap: '0.38rem' }}>
                    {LINKEDIN_KEYWORDS.map((k) => (
                      <button
                        key={k}
                        className="btn"
                        onClick={() =>
                          window.open(
                            `https://www.linkedin.com/search/results/people/?keywords=${encodeURIComponent(`${enrichForm.company_name || company.name_raw || ''} ${k}`)}`,
                            '_blank',
                          )
                        }
                      >
                        {k}
                      </button>
                    ))}
                  </div>
                </>
              ) : (
                <div style={{ fontSize: '0.8rem', color: 'var(--ink-soft)' }}>
                  Opens automatically on YES label; keeps writes explicit.
                </div>
              )}
            </section>
          </aside>
        </section>
      )}
    </>
  )
}
