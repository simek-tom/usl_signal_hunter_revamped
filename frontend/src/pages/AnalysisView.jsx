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

function isLpPipeline(sourceType) {
  return sourceType === 'leadspicker'
}

export default function AnalysisView() {
  const { pipelineKey, batchId } = useParams()
  const [searchParams] = useSearchParams()

  const [loading, setLoading] = useState(true)
  const [entries, setEntries] = useState([])
  const [currentIndex, setCurrentIndex] = useState(0)

  const [searchQuery, setSearchQuery] = useState('')
  const [searchResult, setSearchResult] = useState(null)

  const [enrichOpen, setEnrichOpen] = useState(false)
  const [enrichForm, setEnrichForm] = useState({
    enriched_contact_name: '',
    enriched_contact_linkedin: '',
    enriched_contact_position: '',
    enriched_company_name: '',
    enriched_company_website: '',
    enriched_company_linkedin: '',
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
  const [contactedStatus, setContactedStatus] = useState({ is_contacted: false, matches: [] })

  const aiFilterEnabled = useMemo(() => {
    const raw = String(searchParams.get('ai_filter') || '').trim().toLowerCase()
    return raw === 'yes' || raw === 'true' || raw === '1'
  }, [searchParams])

  useEffect(() => {
    let alive = true
    async function load() {
      setLoading(true)
      setMessage({ kind: 'info', text: '' })
      try {
        const data = await api.getStagingEntries(pipelineKey, batchId)
        if (!alive) return
        const filtered = aiFilterEnabled
          ? data.filter((row) => String(row?.ai_classifier || '').trim().toLowerCase() !== 'no')
          : data
        setEntries(filtered)
        setCurrentIndex(0)
        if (aiFilterEnabled && filtered.length !== data.length) {
          setMessage({ kind: 'info', text: `AI filter active: ${data.length - filtered.length} NO rows hidden.` })
        }
      } catch (err) {
        if (alive) setMessage({ kind: 'error', text: String(err.message || err) })
      } finally {
        if (alive) setLoading(false)
      }
    }
    load()
    return () => { alive = false }
  }, [pipelineKey, batchId, aiFilterEnabled])

  const current = entries[currentIndex] || null

  // Determine source type from pipeline_key prefix
  const isCrunchbase = pipelineKey?.startsWith('crunchbase') || current?.pipeline_key?.startsWith('crunchbase')
  const isNews = pipelineKey?.startsWith('news') || current?.pipeline_key?.startsWith('news')
  const isLp = !isCrunchbase && !isNews

  // Flat field access with enrichment overrides
  const companyName = current?.enriched_company_name || current?.company_name || ''
  const companyWebsite = current?.enriched_company_website || current?.company_website || ''
  const companyLinkedin = current?.enriched_company_linkedin || current?.company_linkedin || ''
  const contactName = current?.enriched_contact_name || current?.author_full_name || ''
  const contactLinkedin = current?.enriched_contact_linkedin || current?.author_linkedin || ''
  const contactPosition = current?.enriched_contact_position || current?.author_position || ''
  const aiClassifier = String(current?.ai_classifier || '').trim()
  const aiClassifierLower = aiClassifier.toLowerCase()

  // CB-specific fields
  const fundingSeries = current?.funding_series || 'n/a'
  const lastFunding = current?.last_funding_amount || 'n/a'
  const cbIndustry = current?.company_industry || 'n/a'
  const cbHq = current?.company_hq_location || 'n/a'
  const cbDescription = current?.company_description || current?.content_text || 'No description.'
  const cbStatus = current?.airtable_status || 'n/a'
  const cbWorkflowStatus = current?.workflow_status || 'new'
  const cbCrunchbaseUrl = current?.crunchbase_profile_url || current?.content_url || ''

  // News-specific
  const headline = current?.content_title || 'Untitled'
  const articleAuthor = current?.article_author || ''

  // Check contacted when entry changes
  useEffect(() => {
    if (!current) return
    const name = companyName
    const website = companyWebsite
    const linkedin = companyLinkedin
    if (!name && !website && !linkedin) {
      setContactedStatus({ is_contacted: false, matches: [] })
      return
    }
    api.checkContacted({ company_name: name, company_website: website, company_linkedin: linkedin })
      .then(setContactedStatus)
      .catch(() => setContactedStatus({ is_contacted: false, matches: [] }))
  }, [currentIndex, companyName, companyWebsite, companyLinkedin])

  // Sync forms when entry changes
  useEffect(() => {
    if (!current) return
    if (isCrunchbase) {
      setCbForm({
        message_fin: current?.message_fin || '',
        main_contact: current?.main_contact_linkedin || '',
        secondary_contact_1: current?.secondary_contact_1 || '',
        secondary_contact_2: current?.secondary_contact_2 || '',
        secondary_contact_3: current?.secondary_contact_3 || '',
      })
    } else {
      setEnrichForm({
        enriched_contact_name: contactName,
        enriched_contact_linkedin: contactLinkedin,
        enriched_contact_position: contactPosition,
        enriched_company_name: companyName,
        enriched_company_website: companyWebsite,
        enriched_company_linkedin: companyLinkedin,
      })
      if (current.label !== 'yes') setEnrichOpen(false)
    }
  }, [currentIndex, current?.id])

  const progress = useMemo(() => {
    const total = entries.length
    let labeled = 0, yes = 0, no = 0, cc = 0, pushedReady = 0
    for (const e of entries) {
      if (e.label) labeled++
      if (e.label === 'yes') yes++
      if (e.label === 'no') no++
      if (e.label === 'cc') cc++
      if (e.workflow_status === 'pushed-ready') pushedReady++
    }
    return { total, labeled, yes, no, cc, pushedReady }
  }, [entries])

  const goNext = useCallback(() => setCurrentIndex((i) => Math.min(entries.length - 1, i + 1)), [entries.length])
  const goPrev = useCallback(() => setCurrentIndex((i) => Math.max(0, i - 1)), [])

  async function applyLabel(label, learningData = null) {
    if (!current) return
    setBusy(true)
    try {
      const res = await api.labelStagingEntry({ pipelineKey, stagingId: current.id, label, learningData })
      setEntries((prev) => {
        const copy = [...prev]
        copy[currentIndex] = res.entry
        return copy
      })
      if (label === 'yes') setEnrichOpen(true)
      setMessage({ kind: 'info', text: `Saved label ${label.toUpperCase()}.` })
    } catch (err) {
      setMessage({ kind: 'error', text: String(err.message || err) })
    } finally {
      setBusy(false)
    }
  }

  async function saveEnrichment() {
    if (!current) return
    setBusy(true)
    try {
      const res = await api.enrichStagingEntry({ pipelineKey, stagingId: current.id, payload: enrichForm })
      setEntries((prev) => { const c = [...prev]; c[currentIndex] = res.entry; return c })
      setMessage({ kind: 'info', text: 'Enrichment saved.' })
    } catch (err) {
      setMessage({ kind: 'error', text: String(err.message || err) })
    } finally {
      setBusy(false)
    }
  }

  async function saveCrunchbaseAction(action, { advance = false } = {}) {
    if (!current) return
    setBusy(true)
    try {
      const res = await api.cbStagingAction({ pipelineKey, stagingId: current.id, payload: { action, ...cbForm } })
      if (action === 'eliminate') {
        setEntries((prev) => prev.filter((_, idx) => idx !== currentIndex))
        setCurrentIndex((idx) => Math.max(0, Math.min(idx, entries.length - 2)))
        setMessage({ kind: 'info', text: 'Entry eliminated.' })
      } else {
        setEntries((prev) => { const c = [...prev]; c[currentIndex] = res.entry; return c })
        if (advance) setCurrentIndex((idx) => Math.min(entries.length - 1, idx + 1))
        setMessage({ kind: 'info', text: action === 'save_next' ? 'Saved + moved next.' : 'Saved.' })
      }
    } catch (err) {
      setMessage({ kind: 'error', text: String(err.message || err) })
    } finally {
      setBusy(false)
    }
  }

  async function finishAnalysis() {
    setBusy(true)
    try {
      const res = await api.finishAnalysis({ pipelineKey, batchId })
      setMessage({ kind: 'info', text: `Finish Analysis: promoted ${res.promoted}, skipped ${res.skipped}.` })
    } catch (err) {
      setMessage({ kind: 'error', text: String(err.message || err) })
    } finally {
      setBusy(false)
    }
  }

  async function runSearch() {
    if (!searchQuery.trim()) { setSearchResult(null); return }
    try {
      setSearchResult(await api.searchMaster(searchQuery.trim()))
    } catch (err) {
      setMessage({ kind: 'error', text: String(err.message || err) })
    }
  }

  // Keyboard shortcuts
  useEffect(() => {
    function onKey(e) {
      const tag = document.activeElement?.tagName?.toLowerCase()
      if (tag === 'input' || tag === 'textarea' || tag === 'select') return
      if (e.key === 'ArrowRight') { e.preventDefault(); goNext() }
      if (e.key === 'ArrowLeft') { e.preventDefault(); goPrev() }
      if (isCrunchbase) {
        if (e.key.toLowerCase() === 'y') { e.preventDefault(); saveCrunchbaseAction('yes') }
        if (e.key.toLowerCase() === 'n') { e.preventDefault(); saveCrunchbaseAction('eliminate', { advance: true }) }
        return
      }
      if (e.key.toLowerCase() === 'y') { e.preventDefault(); applyLabel('yes') }
      if (e.key.toLowerCase() === 'n') { e.preventDefault(); applyLabel('no') }
      if (e.key.toLowerCase() === 'c' && pipelineKey === 'lp_czech') { e.preventDefault(); applyLabel('cc') }
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [goNext, goPrev, pipelineKey, isCrunchbase, cbForm, currentIndex, entries.length])

  if (loading) return <MessageBox kind="info" text="Loading analysis data..." />

  if (!current) {
    return (
      <section className="panel" style={{ display: 'grid', gap: '0.6rem' }}>
        <h1 style={{ margin: 0 }}>Analysis</h1>
        <MessageBox kind="warn" text={aiFilterEnabled ? 'No entries after AI filter.' : 'No entries in this batch.'} />
        <Link className="btn" to="/">Back to Dashboard</Link>
      </section>
    )
  }

  // -- RENDER --
  return (
    <>
      {/* Header */}
      <section className="panel" style={{ display: 'grid', gap: '0.65rem' }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: '0.8rem', flexWrap: 'wrap' }}>
          <div>
            <h1 style={{ margin: 0, fontSize: '1.25rem' }}>Analysis · Batch {batchId.slice(0, 8)}...</h1>
            <div style={{ fontSize: '0.76rem', color: 'var(--ink-soft)' }}>
              Hotkeys: <span className="kbd">Y</span> YES · <span className="kbd">N</span> {isCrunchbase ? 'Eliminate' : 'NO'} · <span className="kbd">←/→</span> nav
            </div>
          </div>
          <div style={{ display: 'flex', gap: '0.45rem', flexWrap: 'wrap' }}>
            <Link className="btn" to={`/pipeline/${pipelineKey}`}>Pipeline</Link>
            <button className="btn warn" disabled={busy} onClick={finishAnalysis}>Finish Analysis</button>
            {!isCrunchbase ? <Link className="btn" to={`/draft/${batchId}`}>Go to Drafting</Link> : null}
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
        /* ---- CRUNCHBASE LAYOUT ---- */
        <section className="grid-2" style={{ alignItems: 'start' }}>
          <article className="panel" style={{ display: 'grid', gap: '0.7rem' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'start', gap: '0.7rem' }}>
              <div>
                <div style={{ fontWeight: 700, fontSize: '1rem' }}>{companyName || 'Unknown company'}</div>
                <div style={{ fontSize: '0.8rem', color: 'var(--ink-soft)' }}>
                  {companyWebsite ? <a href={companyWebsite} target="_blank" rel="noreferrer">{companyWebsite}</a> : 'No website'}
                </div>
              </div>
              <span className={`badge ${(current.ai_pre_score || 0) > 0.7 ? 'green' : (current.ai_pre_score || 0) > 0.4 ? 'amber' : 'gray'}`}>
                AI score {Number(current.ai_pre_score || 0).toFixed(2)}
              </span>
            </div>
            <div style={{ display: 'flex', gap: '0.5rem', flexWrap: 'wrap' }}>
              {contactedStatus.is_contacted ? <span className="badge amber">ALREADY CONTACTED</span> : null}
              <span className="badge gray">CB status: {cbStatus}</span>
              <span className={`badge ${cbWorkflowStatus === 'pushed-ready' ? 'green' : 'amber'}`}>Entry: {cbWorkflowStatus}</span>
              {current.label ? <span className="badge green">Label {current.label.toUpperCase()}</span> : null}
            </div>
            <div style={{ display: 'grid', gap: '0.35rem', fontSize: '0.84rem' }}>
              <div><strong>Series:</strong> {fundingSeries}</div>
              <div><strong>Funding:</strong> {lastFunding}</div>
              <div><strong>Industries:</strong> {cbIndustry}</div>
              <div><strong>HQ:</strong> {cbHq}</div>
            </div>
            <div className="panel" style={{ margin: 0, padding: '0.8rem', background: '#fff' }}>
              <div style={{ fontSize: '0.76rem', color: 'var(--ink-soft)', marginBottom: '0.4rem' }}>Description</div>
              <div style={{ whiteSpace: 'pre-wrap', fontSize: '0.82rem' }}>{cbDescription}</div>
            </div>
            <div className="panel" style={{ margin: 0, padding: '0.8rem', background: '#fff' }}>
              <div style={{ fontSize: '0.76rem', color: 'var(--ink-soft)', marginBottom: '0.4rem' }}>Summary</div>
              <div style={{ whiteSpace: 'pre-wrap', fontSize: '0.82rem' }}>{current?.content_summary || 'No summary.'}</div>
            </div>
            <div style={{ display: 'flex', gap: '0.45rem', flexWrap: 'wrap' }}>
              {companyLinkedin ? <a className="btn" href={companyLinkedin} target="_blank" rel="noreferrer">Company LinkedIn</a> : null}
              {cbCrunchbaseUrl ? <a className="btn" href={cbCrunchbaseUrl} target="_blank" rel="noreferrer">Crunchbase Link</a> : null}
              <CrunchbaseDiscover companyName={companyName} />
            </div>
          </article>
          <aside style={{ display: 'grid', gap: '0.7rem' }}>
            <section className="panel" style={{ display: 'grid', gap: '0.55rem' }}>
              <h2 style={{ margin: 0, fontSize: '0.95rem' }}>CB Analysis Actions</h2>
              <textarea value={cbForm.message_fin} placeholder="Message fin" onChange={(e) => setCbForm((f) => ({ ...f, message_fin: e.target.value }))} style={{ minHeight: 120 }} />
              <input value={cbForm.main_contact} placeholder="Main Contact (LinkedIn)" onChange={(e) => setCbForm((f) => ({ ...f, main_contact: e.target.value }))} />
              <input value={cbForm.secondary_contact_1} placeholder="Secondary #1" onChange={(e) => setCbForm((f) => ({ ...f, secondary_contact_1: e.target.value }))} />
              <input value={cbForm.secondary_contact_2} placeholder="Secondary #2" onChange={(e) => setCbForm((f) => ({ ...f, secondary_contact_2: e.target.value }))} />
              <input value={cbForm.secondary_contact_3} placeholder="Secondary #3" onChange={(e) => setCbForm((f) => ({ ...f, secondary_contact_3: e.target.value }))} />
              <div style={{ display: 'flex', gap: '0.45rem', flexWrap: 'wrap' }}>
                <button className="btn primary" disabled={busy} onClick={() => saveCrunchbaseAction('yes')}>YES <span className="kbd">Y</span></button>
                <button className="btn danger" disabled={busy} onClick={() => saveCrunchbaseAction('eliminate', { advance: true })}>Eliminate</button>
                <button className="btn" disabled={busy} onClick={() => saveCrunchbaseAction('uneliminate')}>Un-eliminate</button>
                <button className="btn warn" disabled={busy} onClick={() => saveCrunchbaseAction('save_next', { advance: true })}>Save & Next</button>
                <button className="btn" disabled={busy} onClick={() => saveCrunchbaseAction('save_stay')}>Save & Stay</button>
              </div>
              <div style={{ display: 'flex', gap: '0.45rem' }}>
                <button className="btn" onClick={goPrev}>Back</button>
                <button className="btn" onClick={goNext}>Skip</button>
              </div>
            </section>
            <section className="panel" style={{ display: 'grid', gap: '0.55rem' }}>
              <h2 style={{ margin: 0, fontSize: '0.95rem' }}>Master Search</h2>
              <div style={{ display: 'flex', gap: '0.45rem' }}>
                <input placeholder="Search company" value={searchQuery} onChange={(e) => setSearchQuery(e.target.value)} onKeyDown={(e) => e.key === 'Enter' && runSearch()} />
                <button className="btn" onClick={runSearch}>Search</button>
              </div>
              {searchResult ? (
                <div style={{ display: 'grid', gap: '0.45rem', fontSize: '0.8rem' }}>
                  <div><strong>Exact:</strong> {(searchResult.exact || []).length}</div>
                  {(searchResult.exact || []).slice(0, 4).map((r) => <div key={r.id} style={{ borderBottom: '1px dashed var(--line)', paddingBottom: '0.35rem' }}>{r.name_raw} · {r.domain_normalized || 'no-domain'}</div>)}
                  <div><strong>Partial:</strong> {(searchResult.partial || []).length}</div>
                  {(searchResult.partial || []).slice(0, 4).map((r) => <div key={r.id} style={{ borderBottom: '1px dashed var(--line)', paddingBottom: '0.35rem' }}>{r.name_raw} · {r.domain_normalized || 'no-domain'}</div>)}
                </div>
              ) : null}
            </section>
          </aside>
        </section>
      ) : (
        /* ---- LP / NEWS LAYOUT ---- */
        <section className="grid-2" style={{ alignItems: 'start' }}>
          <article className="panel" style={{ display: 'grid', gap: '0.7rem' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'start', gap: '0.7rem' }}>
              <div>
                <div style={{ fontWeight: 700 }}>{isNews ? articleAuthor || 'Unknown author' : contactName || 'Unknown author'}</div>
                <div style={{ fontSize: '0.8rem', color: 'var(--ink-soft)' }}>
                  {!isNews && contactLinkedin ? <a href={contactLinkedin} target="_blank" rel="noreferrer">{contactLinkedin}</a> : isNews ? `Source: ${current?.source_name || 'n/a'}` : 'No profile'}
                </div>
              </div>
              <span className={`badge ${(current.ai_pre_score || 0) > 0.7 ? 'green' : (current.ai_pre_score || 0) > 0.4 ? 'amber' : 'gray'}`}>
                AI score {Number(current.ai_pre_score || 0).toFixed(2)}
              </span>
            </div>

            {isNews ? (
              <div style={{ display: 'grid', gap: '0.35rem', fontSize: '0.84rem' }}>
                <div><strong>Headline:</strong> {headline}</div>
                <div><strong>Published:</strong> {current?.published_at ? new Date(current.published_at).toLocaleString() : 'n/a'}</div>
                <div><strong>URL:</strong> {current?.content_url ? <a href={current.content_url} target="_blank" rel="noreferrer" style={{ textDecoration: 'underline' }}>{current.content_url}</a> : 'n/a'}</div>
                <div><strong>Company:</strong> {companyName || 'Not identified yet'}</div>
              </div>
            ) : (
              <>
                <div style={{ fontSize: '0.86rem' }}>
                  <strong>Company:</strong>{' '}
                  {companyLinkedin ? <a href={companyLinkedin} target="_blank" rel="noreferrer" style={{ textDecoration: 'underline' }}>{companyName || 'Unknown'}</a>
                    : companyWebsite ? <a href={companyWebsite} target="_blank" rel="noreferrer" style={{ textDecoration: 'underline' }}>{companyName || companyWebsite}</a>
                    : companyName || 'Unknown'}
                </div>
                <div style={{ fontSize: '0.82rem', color: 'var(--ink-soft)' }}><strong>Position:</strong> {contactPosition || 'n/a'}</div>
              </>
            )}

            <div style={{ display: 'flex', gap: '0.5rem', flexWrap: 'wrap' }}>
              {contactedStatus.is_contacted ? <span className="badge amber">ALREADY CONTACTED</span> : null}
              {aiClassifier ? <span className={`badge ${aiClassifierLower === 'yes' ? 'green' : aiClassifierLower === 'no' ? 'red' : 'gray'}`}>AI classifier: {aiClassifier}</span> : <span className="badge gray">AI classifier: n/a</span>}
              {current.learning_data ? <span className="badge gray">Learning Data</span> : null}
              {current.label ? <span className="badge green">Label {current.label.toUpperCase()}</span> : null}
            </div>

            <div className="panel" style={{ margin: 0, padding: '0.8rem', background: '#fff' }}>
              <div style={{ fontSize: '0.76rem', color: 'var(--ink-soft)', marginBottom: '0.45rem' }}>{isNews ? 'Article content' : 'Post text'}</div>
              <div style={{ whiteSpace: 'pre-wrap', lineHeight: 1.45, fontSize: '0.84rem' }}>
                {highlightText(current?.content_text || '').map((part, idx) => part.hit ? <mark className="marked-hit" key={idx}>{part.value}</mark> : <span key={idx}>{part.value}</span>)}
              </div>
            </div>

            <div className="panel" style={{ margin: 0, padding: '0.8rem', background: '#fff' }}>
              <div style={{ fontSize: '0.76rem', color: 'var(--ink-soft)', marginBottom: '0.4rem' }}>{isNews ? 'Article summary' : 'Summary'}</div>
              <div style={{ whiteSpace: 'pre-wrap', fontSize: '0.82rem' }}>{current?.content_summary || 'No summary.'}</div>
            </div>

            <div style={{ display: 'flex', gap: '0.45rem', flexWrap: 'wrap' }}>
              {isLp && current?.content_url ? <a className="btn" href={current.content_url} target="_blank" rel="noreferrer">Open LinkedIn post</a> : null}
              <CrunchbaseDiscover companyName={enrichForm.enriched_company_name || companyName} />
            </div>

            <div style={{ display: 'flex', gap: '0.45rem', flexWrap: 'wrap' }}>
              <button className="btn primary" disabled={busy} onClick={() => applyLabel('yes')}>YES <span className="kbd">Y</span></button>
              <button className="btn" disabled={busy} onClick={() => applyLabel('no')}>NO <span className="kbd">N</span></button>
              <button className="btn" disabled={busy} onClick={() => applyLabel('no', true)}>NO + Learning</button>
              {pipelineKey === 'lp_czech' ? <button className="btn cc" disabled={busy} onClick={() => applyLabel('cc')}>CC <span className="kbd">C</span></button> : null}
              <button className="btn" onClick={goPrev}>Back</button>
              <button className="btn" onClick={goNext}>Skip</button>
            </div>
          </article>

          <aside style={{ display: 'grid', gap: '0.7rem' }}>
            <section className="panel" style={{ display: 'grid', gap: '0.55rem' }}>
              <h2 style={{ margin: 0, fontSize: '0.95rem' }}>Master Search</h2>
              <div style={{ display: 'flex', gap: '0.45rem' }}>
                <input placeholder="Search company" value={searchQuery} onChange={(e) => setSearchQuery(e.target.value)} onKeyDown={(e) => e.key === 'Enter' && runSearch()} />
                <button className="btn" onClick={runSearch}>Search</button>
              </div>
              {searchResult ? (
                <div style={{ display: 'grid', gap: '0.45rem', fontSize: '0.8rem' }}>
                  <div><strong>Exact:</strong> {(searchResult.exact || []).length}</div>
                  {(searchResult.exact || []).slice(0, 4).map((r) => <div key={r.id} style={{ borderBottom: '1px dashed var(--line)', paddingBottom: '0.35rem' }}>{r.name_raw} · {r.domain_normalized || 'no-domain'}</div>)}
                  <div><strong>Partial:</strong> {(searchResult.partial || []).length}</div>
                  {(searchResult.partial || []).slice(0, 4).map((r) => <div key={r.id} style={{ borderBottom: '1px dashed var(--line)', paddingBottom: '0.35rem' }}>{r.name_raw} · {r.domain_normalized || 'no-domain'}</div>)}
                </div>
              ) : null}
            </section>

            <section className="panel" style={{ display: 'grid', gap: '0.55rem' }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <h2 style={{ margin: 0, fontSize: '0.95rem' }}>Enrichment</h2>
                <button className="btn" onClick={() => setEnrichOpen((v) => !v)}>{enrichOpen ? 'Collapse' : 'Expand'}</button>
              </div>
              {enrichOpen ? (
                <>
                  <input value={enrichForm.enriched_contact_name} placeholder="Full name" onChange={(e) => setEnrichForm((f) => ({ ...f, enriched_contact_name: e.target.value }))} />
                  <input value={enrichForm.enriched_contact_linkedin} placeholder="LinkedIn URL" onChange={(e) => setEnrichForm((f) => ({ ...f, enriched_contact_linkedin: e.target.value }))} />
                  <input value={enrichForm.enriched_contact_position} placeholder="Position" onChange={(e) => setEnrichForm((f) => ({ ...f, enriched_contact_position: e.target.value }))} />
                  <input value={enrichForm.enriched_company_name} placeholder="Company name" onChange={(e) => setEnrichForm((f) => ({ ...f, enriched_company_name: e.target.value }))} />
                  <input value={enrichForm.enriched_company_website} placeholder="Company website" onChange={(e) => setEnrichForm((f) => ({ ...f, enriched_company_website: e.target.value }))} />
                  <input value={enrichForm.enriched_company_linkedin} placeholder="Company LinkedIn" onChange={(e) => setEnrichForm((f) => ({ ...f, enriched_company_linkedin: e.target.value }))} />
                  <div style={{ display: 'flex', gap: '0.45rem', flexWrap: 'wrap' }}>
                    <button className="btn primary" disabled={busy} onClick={saveEnrichment}>Save Enrichment</button>
                    <CrunchbaseDiscover withInput companyName={enrichForm.enriched_company_name || companyName} buttonLabel="Open on Crunchbase" />
                  </div>
                  <div style={{ display: 'flex', flexWrap: 'wrap', gap: '0.38rem' }}>
                    {LINKEDIN_KEYWORDS.map((k) => (
                      <button key={k} className="btn" onClick={() => window.open(`https://www.linkedin.com/search/results/people/?keywords=${encodeURIComponent(`${enrichForm.enriched_company_name || companyName} ${k}`)}`, '_blank')}>{k}</button>
                    ))}
                  </div>
                </>
              ) : <div style={{ fontSize: '0.8rem', color: 'var(--ink-soft)' }}>Opens on YES label.</div>}
            </section>
          </aside>
        </section>
      )}
    </>
  )
}