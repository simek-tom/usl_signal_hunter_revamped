import { useState, useEffect, useCallback } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { api } from '../lib/api'
import { usePipelineConfigs } from '../context/PipelineConfigContext'
import MessageBox from '../components/MessageBox'

// ── Stat chip ──────────────────────────────────────────────────────────────────
function Stat({ label, value, highlight }) {
  return (
    <span style={{ display: 'inline-flex', flexDirection: 'column', alignItems: 'center', gap: 1 }}>
      <span style={{ fontSize: '1.35rem', fontWeight: 700, color: highlight ? 'var(--teal)' : 'inherit' }}>
        {value ?? '—'}
      </span>
      <span style={{ fontSize: '0.68rem', color: 'var(--ink-soft)', textTransform: 'uppercase', letterSpacing: '0.04em' }}>
        {label}
      </span>
    </span>
  )
}

function StatsRow({ children }) {
  return (
    <div style={{ display: 'flex', gap: '1.5rem', flexWrap: 'wrap', margin: '0.5rem 0' }}>
      {children}
    </div>
  )
}

function Card({ title, children }) {
  return (
    <div style={{
      background: 'var(--card)',
      border: '1px solid var(--line)',
      borderRadius: 14,
      padding: '1.1rem 1.25rem',
      display: 'grid',
      gap: '0.75rem',
    }}>
      <div style={{ fontSize: '0.75rem', fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.06em', color: 'var(--ink-soft)' }}>
        {title}
      </div>
      {children}
    </div>
  )
}

function ProgressBar({ value, total }) {
  const pct = total > 0 ? Math.round((value / total) * 100) : 0
  return (
    <div style={{ display: 'grid', gap: '0.2rem' }}>
      <div style={{ height: 6, background: 'var(--line)', borderRadius: 99, overflow: 'hidden' }}>
        <div style={{ height: '100%', width: `${pct}%`, background: 'var(--teal)', borderRadius: 99, transition: 'width 0.3s' }} />
      </div>
      <div style={{ fontSize: '0.72rem', color: 'var(--ink-soft)' }}>{value} / {total} ({pct}%)</div>
    </div>
  )
}

export default function PipelineDashboard() {
  const { type } = useParams()
  const navigate = useNavigate()
  const { getConfig } = usePipelineConfigs()
  const pipelineCfg = getConfig(type)

  const sourceType = pipelineCfg?.source_type || 'leadspicker'
  const isLp = sourceType === 'leadspicker'
  const isCrunchbase = sourceType === 'crunchbase'
  const isNews = sourceType === 'news'

  const [stats, setStats] = useState(null)
  const [statsLoading, setStatsLoading] = useState(true)
  const [message, setMessage] = useState(null)
  const [busy, setBusy] = useState('')

  // Import state
  const [csvFile, setCsvFile] = useState(null)
  const [cbStatus, setCbStatus] = useState('')
  const [cbContactEnriched, setCbContactEnriched] = useState('all')
  const [cbView, setCbView] = useState('')
  const [cbMaxRecords, setCbMaxRecords] = useState(200)
  const [cbTableName, setCbTableName] = useState('')
  const [newsQuery, setNewsQuery] = useState('')
  const [newsDomains, setNewsDomains] = useState('')
  const [newsLanguage, setNewsLanguage] = useState('')
  const [newsFromDate, setNewsFromDate] = useState('')
  const [newsToDate, setNewsToDate] = useState('')
  const [newsPageSize, setNewsPageSize] = useState(100)
  const [newsMaxPages, setNewsMaxPages] = useState(3)

  // Push state
  const [pushExpanded, setPushExpanded] = useState(false)
  const [pushEntries, setPushEntries] = useState([])
  const [pushLoading, setPushLoading] = useState(false)
  const [omitted, setOmitted] = useState(new Set())

  function notify(kind, text) {
    setMessage({ kind, text })
    setTimeout(() => setMessage(null), 6000)
  }

  const loadStats = useCallback(async () => {
    if (!type) return
    setStatsLoading(true)
    try {
      const data = await api.getPipelineStats(type)
      setStats(data)
    } catch (err) {
      console.error('Stats load failed:', err)
    } finally {
      setStatsLoading(false)
    }
  }, [type])

  useEffect(() => { loadStats() }, [loadStats])

  async function loadPushCandidates() {
    if (pushLoading) return
    setPushLoading(true)
    setOmitted(new Set())
    try {
      const all = await api.getPipelineDraftEntries(type)
      setPushEntries(all.filter(e =>
        e.status !== 'pushed' &&
        e.message && (e.message.final_text || e.message.draft_text)
      ))
    } catch (err) {
      notify('error', err.message)
    } finally {
      setPushLoading(false)
    }
  }

  function togglePushExpand() {
    if (!pushExpanded && pushEntries.length === 0) loadPushCandidates()
    setPushExpanded(v => !v)
  }

  function toggleOmit(id) {
    setOmitted(prev => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id); else next.add(id)
      return next
    })
  }

  // Import handlers
  async function runLpImport() {
    const ids = pipelineCfg?.lp_project_ids || []
    if (!ids.length) { notify('warn', 'No LP project IDs configured. Edit this sub-branch to add them.'); return }
    setBusy('import')
    try {
      const r = await api.importFromLp({ projectIds: ids, pipelineType: type })
      notify('info', `Imported ${r.record_count} records.`)
      await loadStats()
    } catch (err) { notify('error', err.message) } finally { setBusy('') }
  }

  async function runCrunchbaseImport() {
    setBusy('import')
    try {
      const enriched = cbContactEnriched === 'yes' ? true : cbContactEnriched === 'no' ? false : null
      const r = await api.importCrunchbase({ status: cbStatus || null, contactEnriched: enriched, view: cbView || null, maxRecords: cbMaxRecords, tableName: cbTableName || null })
      notify('info', `Imported ${r.record_count} records.`)
      await loadStats()
    } catch (err) { notify('error', err.message) } finally { setBusy('') }
  }

  async function runNewsImport() {
    setBusy('import')
    try {
      const r = await api.importNews({ query: newsQuery || null, domains: newsDomains || null, language: newsLanguage || null, fromDate: newsFromDate || null, toDate: newsToDate || null, sortBy: 'publishedAt', pageSize: newsPageSize || null, maxPages: newsMaxPages || null })
      notify('info', `Imported ${r.record_count} articles.`)
      await loadStats()
    } catch (err) { notify('error', err.message) } finally { setBusy('') }
  }

  async function runCsvImport() {
    if (!csvFile) return
    setBusy('import')
    try {
      const r = isCrunchbase
        ? await api.uploadCrunchbaseCsv({ file: csvFile })
        : await api.uploadCsv({ file: csvFile, pipelineType: type })
      notify('info', `Imported ${r.record_count} records from CSV.`)
      setCsvFile(null)
      await loadStats()
    } catch (err) { notify('error', err.message) } finally { setBusy('') }
  }

  async function runPromote() {
    setBusy('promote')
    try {
      const r = await api.finishAnalysis(type)
      const n = r.promoted ?? r.total ?? 0
      notify('info', `Promoted ${n} entries to drafting.`)
      await loadStats()
    } catch (err) { notify('error', err.message) } finally { setBusy('') }
  }

  async function runLabelAiNo() {
    setBusy('label-ai-no')
    try {
      const r = await api.labelAiClassifierNo(type)
      notify('info', `Labeled ${r.labeled} AI-classifier-NO entries as not relevant.`)
      await loadStats()
    } catch (err) { notify('error', err.message) } finally { setBusy('') }
  }

  async function runStartDrafting() {
    setBusy('start-draft')
    try {
      const r = await api.startPipelineDrafting(type)
      notify('info', `Created ${r.created} message stubs (${r.total_relevant} total relevant).`)
      await loadStats()
    } catch (err) { notify('error', err.message) } finally { setBusy('') }
  }

  async function runPushLp() {
    const ids = pushEntries.filter(e => !omitted.has(e.id)).map(e => e.id)
    if (!ids.length) { notify('warn', 'No entries selected for push.'); return }
    const projectId = pipelineCfg?.lp_project_ids?.[0]
    if (!projectId) { notify('warn', 'No LP project ID configured. Edit this sub-branch.'); return }
    setBusy('push-lp')
    try {
      const r = await api.pushLeadspicker({ entryIds: ids, projectId, pipelineKey: type })
      notify('info', `LP push: ${r.pushed} pushed, ${r.failed} failed, ${r.skipped} skipped.`)
      await loadStats()
      await loadPushCandidates()
    } catch (err) { notify('error', err.message) } finally { setBusy('') }
  }

  async function runPushAirtable() {
    const ids = pushEntries.filter(e => !omitted.has(e.id)).map(e => e.id)
    if (!ids.length) { notify('warn', 'No entries selected for push.'); return }
    setBusy('push-at')
    try {
      const r = await api.pushAirtable({ entryIds: ids, pipelineKey: type })
      notify('info', `Airtable push: ${r.created} created, ${r.failed} failed, ${r.skipped} skipped.`)
      await loadStats()
      await loadPushCandidates()
    } catch (err) { notify('error', err.message) } finally { setBusy('') }
  }

  // Derived
  const staging = stats?.staging || {}
  const ent = stats?.entries || {}
  const relevantTotal = ent.total || 0
  const drafted = ent.drafted || 0
  const pushed = ent.pushed || 0
  const waitingToDraft = Math.max(0, relevantTotal - drafted)
  const readyToPush = pushEntries.filter(e => !omitted.has(e.id)).length

  const lbl = { fontSize: '0.78rem', fontWeight: 600, display: 'grid', gap: '0.3rem' }
  const inp = { fontSize: '0.82rem' }

  return (
    <div style={{ padding: '1.5rem', maxWidth: 1100, margin: '0 auto' }}>
      <div style={{ marginBottom: '1.1rem' }}>
        <h2 style={{ margin: 0, fontSize: '1.2rem', fontWeight: 700 }}>{pipelineCfg?.label || type}</h2>
        <p style={{ margin: '0.2rem 0 0', fontSize: '0.8rem', color: 'var(--ink-soft)' }}>
          {pipelineCfg?.pipeline_key}
          {pipelineCfg?.airtable_table_name && (
            <span style={{ marginLeft: '0.65rem', background: 'var(--mint-soft)', borderRadius: 6, padding: '0.1rem 0.4rem' }}>
              {pipelineCfg.airtable_table_name}
            </span>
          )}
        </p>
      </div>

      {message && <div style={{ marginBottom: '0.75rem' }}><MessageBox kind={message.kind} text={message.text} /></div>}

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '1rem' }}>

        {/* 1. Import */}
        <Card title="Import">
          {isLp && (
            <div style={{ display: 'grid', gap: '0.5rem' }}>
              <div style={{ fontSize: '0.8rem', color: pipelineCfg?.lp_project_ids?.length ? 'var(--ink-soft)' : 'var(--rose)' }}>
                {pipelineCfg?.lp_project_ids?.length
                  ? <>LP projects: <strong>{pipelineCfg.lp_project_ids.join(', ')}</strong></>
                  : 'No LP project IDs configured — edit this sub-branch.'}
              </div>
              <button className="btn primary" disabled={busy === 'import'} onClick={runLpImport}>
                {busy === 'import' ? 'Importing…' : 'Fetch from LP (auto dedup)'}
              </button>
            </div>
          )}

          {isCrunchbase && (
            <div style={{ display: 'grid', gap: '0.5rem' }}>
              <label style={lbl}>Airtable Table<input style={inp} value={cbTableName} onChange={e => setCbTableName(e.target.value)} placeholder="optional override" /></label>
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '0.5rem' }}>
                <label style={lbl}>Status<input style={inp} value={cbStatus} onChange={e => setCbStatus(e.target.value)} placeholder="e.g. Prospect" /></label>
                <label style={lbl}>Contact Enriched
                  <select style={inp} value={cbContactEnriched} onChange={e => setCbContactEnriched(e.target.value)}>
                    <option value="all">All</option><option value="yes">Yes</option><option value="no">No</option>
                  </select>
                </label>
              </div>
              <label style={lbl}>View<input style={inp} value={cbView} onChange={e => setCbView(e.target.value)} placeholder="optional" /></label>
              <label style={lbl}>Max Records<input style={inp} type="number" value={cbMaxRecords} onChange={e => setCbMaxRecords(Number(e.target.value))} /></label>
              <button className="btn primary" disabled={busy === 'import'} onClick={runCrunchbaseImport}>
                {busy === 'import' ? 'Importing…' : 'Fetch from Airtable (auto dedup)'}
              </button>
            </div>
          )}

          {isNews && (
            <div style={{ display: 'grid', gap: '0.5rem' }}>
              <label style={lbl}>Query<input style={inp} value={newsQuery} onChange={e => setNewsQuery(e.target.value)} placeholder="uses default from settings" /></label>
              <label style={lbl}>Domains<input style={inp} value={newsDomains} onChange={e => setNewsDomains(e.target.value)} placeholder="uses default from settings" /></label>
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '0.5rem' }}>
                <label style={lbl}>Language<input style={inp} value={newsLanguage} onChange={e => setNewsLanguage(e.target.value)} placeholder="en" /></label>
                <label style={lbl}>Page size<input style={inp} type="number" value={newsPageSize} onChange={e => setNewsPageSize(Number(e.target.value))} /></label>
                <label style={lbl}>From date<input style={inp} type="date" value={newsFromDate} onChange={e => setNewsFromDate(e.target.value)} /></label>
                <label style={lbl}>To date<input style={inp} type="date" value={newsToDate} onChange={e => setNewsToDate(e.target.value)} /></label>
              </div>
              <label style={lbl}>Max pages<input style={inp} type="number" value={newsMaxPages} onChange={e => setNewsMaxPages(Number(e.target.value))} /></label>
              <button className="btn primary" disabled={busy === 'import'} onClick={runNewsImport}>
                {busy === 'import' ? 'Importing…' : 'Fetch NewsAPI (auto dedup)'}
              </button>
            </div>
          )}

          {!isNews && (
            <div style={{ borderTop: '1px solid var(--line)', paddingTop: '0.6rem', display: 'grid', gap: '0.4rem' }}>
              <span style={{ fontSize: '0.73rem', color: 'var(--ink-soft)', fontWeight: 600 }}>CSV Upload</span>
              <input type="file" accept=".csv" style={{ fontSize: '0.78rem' }} onChange={e => setCsvFile(e.target.files[0] || null)} />
              <button className="btn" disabled={!csvFile || busy === 'import'} onClick={runCsvImport}>
                {busy === 'import' ? 'Importing…' : 'Upload CSV + Import'}
              </button>
            </div>
          )}
        </Card>

        {/* 2. Analysis */}
        <Card title="Analysis">
          <StatsRow>
            <Stat label="Total" value={statsLoading ? '…' : staging.total} />
            <Stat label="Unlabeled" value={statsLoading ? '…' : staging.unlabeled} highlight />
            <Stat label="YES" value={statsLoading ? '…' : staging.yes} />
            <Stat label="NO" value={statsLoading ? '…' : staging.no} />
            <Stat label="CC" value={statsLoading ? '…' : staging.cc} />
          </StatsRow>
          <div style={{ display: 'flex', gap: '0.5rem', flexWrap: 'wrap' }}>
            <button
              className="btn primary"
              disabled={busy === 'promote' || !((staging.yes || 0) + (staging.cc || 0))}
              onClick={runPromote}
            >
              {busy === 'promote' ? 'Promoting…' : `Promote to Drafting (${(staging.yes || 0) + (staging.cc || 0)})`}
            </button>
            <button className="btn" onClick={() => navigate(`/analyze/${type}`)}>
              Open Analysis →
            </button>
            <button
              className="btn warn"
              disabled={busy === 'label-ai-no' || !(staging.unlabeled)}
              onClick={runLabelAiNo}
              title="Label all unlabeled entries where AI classifier = NO as not relevant"
            >
              {busy === 'label-ai-no' ? 'Labeling…' : 'Label AI-NO as irrelevant'}
            </button>
          </div>
        </Card>

        {/* 3. Drafting */}
        <Card title="Drafting">
          <StatsRow>
            <Stat label="In pipeline" value={statsLoading ? '…' : relevantTotal} />
            <Stat label="Drafted" value={statsLoading ? '…' : drafted} highlight />
            <Stat label="Waiting" value={statsLoading ? '…' : waitingToDraft} />
            <Stat label="Pushed" value={statsLoading ? '…' : pushed} />
          </StatsRow>
          {relevantTotal > 0 && <ProgressBar value={drafted} total={relevantTotal} />}
          <div style={{ display: 'flex', gap: '0.5rem', flexWrap: 'wrap' }}>
            <button className="btn" disabled={busy === 'start-draft'} onClick={runStartDrafting}>
              {busy === 'start-draft' ? 'Starting…' : 'Start Drafting'}
            </button>
            <button className="btn primary" disabled={relevantTotal === 0} onClick={() => navigate(`/draft/${type}`)}>
              Open Drafting →
            </button>
          </div>
        </Card>

        {/* 4. Push */}
        <Card title="Push">
          <StatsRow>
            <Stat label="Pushed" value={statsLoading ? '…' : pushed} />
            <Stat label="Remaining" value={statsLoading ? '…' : Math.max(0, relevantTotal - pushed)} highlight />
          </StatsRow>

          <button className="btn" style={{ fontSize: '0.78rem', padding: '0.3rem 0.6rem' }} onClick={togglePushExpand}>
            {pushExpanded
              ? '▲ Hide candidates'
              : `▼ Show push candidates${pushEntries.length > 0 ? ` (${pushEntries.length})` : ''}`}
          </button>

          {pushExpanded && (
            <div style={{ display: 'grid', gap: '0.4rem' }}>
              {pushLoading ? (
                <span style={{ fontSize: '0.8rem', color: 'var(--ink-soft)' }}>Loading…</span>
              ) : pushEntries.length === 0 ? (
                <span style={{ fontSize: '0.8rem', color: 'var(--ink-soft)' }}>No push candidates. Start Drafting first.</span>
              ) : (
                <>
                  <div style={{ border: '1px solid var(--line)', borderRadius: 8, maxHeight: 220, overflowY: 'auto' }}>
                    {pushEntries.map(e => {
                      const co = e.signals?.companies || {}
                      const ct = e.contacts || {}
                      const checked = !omitted.has(e.id)
                      return (
                        <label key={e.id} style={{
                          display: 'flex', alignItems: 'center', gap: '0.5rem',
                          padding: '0.4rem 0.65rem', fontSize: '0.78rem',
                          borderBottom: '1px solid var(--line)', cursor: 'pointer',
                          background: checked ? 'white' : '#fafaf8', opacity: checked ? 1 : 0.5,
                        }}>
                          <input type="checkbox" checked={checked} onChange={() => toggleOmit(e.id)} />
                          <span style={{ fontWeight: 600 }}>{ct.full_name || ct.first_name || '—'}</span>
                          <span style={{ color: 'var(--ink-soft)' }}>· {co.name_raw || '—'}</span>
                        </label>
                      )
                    })}
                  </div>
                  <div style={{ fontSize: '0.73rem', color: 'var(--ink-soft)' }}>
                    {readyToPush} of {pushEntries.length} selected
                  </div>
                </>
              )}
            </div>
          )}

          <div style={{ display: 'flex', gap: '0.5rem', flexWrap: 'wrap' }}>
            {isLp && (
              <button className="btn primary" disabled={busy === 'push-lp'} onClick={runPushLp}>
                {busy === 'push-lp' ? 'Pushing…' : 'Push to LP'}
              </button>
            )}
            <button className="btn" disabled={busy === 'push-at'} onClick={runPushAirtable}>
              {busy === 'push-at' ? 'Pushing…' : isCrunchbase ? 'Batch Update Airtable' : 'Push to Airtable'}
            </button>
            {pushExpanded && (
              <button className="btn" style={{ fontSize: '0.75rem', padding: '0.25rem 0.5rem', marginLeft: 'auto' }} onClick={loadPushCandidates} disabled={pushLoading}>
                ↻ Refresh
              </button>
            )}
          </div>
        </Card>

      </div>
    </div>
  )
}
