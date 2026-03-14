import { useEffect, useMemo, useState } from 'react'
import { Link, useParams } from 'react-router-dom'
import { api } from '../lib/api'
import { usePipelineConfigs } from '../context/PipelineConfigContext'
import StatusBadge from '../components/StatusBadge'
import ProgressBar from '../components/ProgressBar'
import MessageBox from '../components/MessageBox'

function cbMeta(entry) {
  const meta = entry?.signals?.source_metadata
  return meta && typeof meta === 'object' ? meta : {}
}

function cbMessage(entry) {
  const meta = cbMeta(entry)
  return String(meta.message_fin || meta.message_draft || '').trim()
}

function cbWorkflowStatus(entry) {
  const meta = cbMeta(entry)
  return String(entry?.status || meta.entry_workflow_status || '').trim().toLowerCase()
}

export default function PipelineDashboard() {
  const { type = 'lp_general' } = useParams()
  const { getConfig, getLabel, configs } = usePipelineConfigs()
  const pipelineCfg = useMemo(() => {
    const cfg = getConfig(type)
    return cfg || { pipeline_key: type, label: type, airtable_table_name: '', source_type: 'leadspicker' }
  }, [type, getConfig])
  const isLpCzech = type === 'lp_czech'
  const isCrunchbase = pipelineCfg.source_type === 'crunchbase'
  const isNews = pipelineCfg.source_type === 'news'

  const [projects, setProjects] = useState([])
  const [batches, setBatches] = useState([])
  const [loading, setLoading] = useState(true)

  const [selectedProjectId, setSelectedProjectId] = useState('')
  const [selectedBatchId, setSelectedBatchId] = useState('')
  const [pushEntries, setPushEntries] = useState([])
  const [pushLoading, setPushLoading] = useState(false)
  const [airtableTable, setAirtableTable] = useState(pipelineCfg.airtable_table_name || '')
  const [analysisYesOnly, setAnalysisYesOnly] = useState(false)

  const [cbStatus, setCbStatus] = useState('')
  const [cbContactEnriched, setCbContactEnriched] = useState('all')
  const [cbView, setCbView] = useState('')
  const [cbMaxRecords, setCbMaxRecords] = useState(200)
  const [newsQuery, setNewsQuery] = useState('')
  const [newsDomains, setNewsDomains] = useState('')
  const [newsLanguage, setNewsLanguage] = useState('')
  const [newsFromDate, setNewsFromDate] = useState('')
  const [newsToDate, setNewsToDate] = useState('')
  const [newsPageSize, setNewsPageSize] = useState(100)
  const [newsMaxPages, setNewsMaxPages] = useState(3)

  const [csvFile, setCsvFile] = useState(null)
  const [busy, setBusy] = useState(false)
  const [message, setMessage] = useState({ kind: 'info', text: '' })

  async function loadBatches() {
    const data = await api.getBatches()
    setBatches(data)
    const relevant = data.filter((b) => b.pipeline_type === type)
    if (!selectedBatchId && relevant.length > 0) {
      setSelectedBatchId(relevant[0].id)
    }
  }

  useEffect(() => {
    let alive = true
    async function boot() {
      setLoading(true)
      setMessage({ kind: 'info', text: '' })
      try {
        const [projectsData, batchesData] = await Promise.all([api.listProjects(), api.getBatches()])
        if (!alive) {
          return
        }
        setProjects(projectsData)
        setBatches(batchesData)
        setSelectedProjectId(projectsData[0] ? String(projectsData[0].lp_project_id) : '')
        const relevant = batchesData.filter((b) => b.pipeline_type === type)
        setSelectedBatchId(relevant[0] ? relevant[0].id : '')
        setAirtableTable(pipelineCfg.airtable_table_name || '')
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
    boot()
    return () => {
      alive = false
    }
  }, [type, pipelineCfg.airtable_table_name])

  useEffect(() => {
    if (!selectedBatchId) {
      setPushEntries([])
      return
    }

    let alive = true
    async function loadPushEntries() {
      setPushLoading(true)
      try {
        const entries = await api.getBatchEntries(selectedBatchId)
        if (!alive) {
          return
        }
        setPushEntries(entries)
      } catch (err) {
        if (alive) {
          setMessage({ kind: 'error', text: String(err.message || err) })
        }
      } finally {
        if (alive) {
          setPushLoading(false)
        }
      }
    }
    loadPushEntries()
    return () => {
      alive = false
    }
  }, [selectedBatchId])

  const pipelineBatches = useMemo(
    () => batches.filter((b) => b.pipeline_type === type),
    [batches, type],
  )

  const pushCandidates = useMemo(() => {
    if (isCrunchbase) {
      return pushEntries
        .filter((e) => cbWorkflowStatus(e) === 'pushed-ready' && cbMessage(e))
        .map((e) => e.id)
    }
    if (isNews) {
      return pushEntries
        .filter((e) => e.relevant === 'yes' && e.status !== 'eliminated')
        .map((e) => e.id)
    }
    return pushEntries
      .filter((e) => ['yes', 'cc'].includes(e.relevant) && e.status !== 'eliminated')
      .map((e) => e.id)
  }, [pushEntries, isCrunchbase, isNews])

  const analysisEntries = useMemo(() => {
    if (!analysisYesOnly) {
      return pushEntries
    }
    return pushEntries.filter((e) => String(e?.signals?.ai_classifier || '').trim().toLowerCase() !== 'no')
  }, [pushEntries, analysisYesOnly])

  const analysisUrl = (batchId) => `/analyze/${type}/${batchId}${analysisYesOnly ? '?ai_filter=yes' : ''}`

  async function refreshProjects() {
    setBusy(true)
    setMessage({ kind: 'info', text: '' })
    try {
      const data = await api.refreshProjects()
      setProjects(data)
      if (data[0]) {
        setSelectedProjectId(String(data[0].lp_project_id))
      }
      setMessage({ kind: 'info', text: `Project cache refreshed (${data.length})` })
    } catch (err) {
      setMessage({ kind: 'error', text: String(err.message || err) })
    } finally {
      setBusy(false)
    }
  }

  async function runLpImport() {
    if (!selectedProjectId) {
      setMessage({ kind: 'warn', text: 'Select a Leadspicker project first.' })
      return
    }

    setBusy(true)
    setMessage({ kind: 'info', text: '' })
    try {
      const res = await api.importFromLp({
        projectIds: [Number(selectedProjectId)],
        pipelineType: type,
      })
      setMessage({ kind: 'info', text: `Imported ${res.record_count} entries into batch ${String(res.batch_id).slice(0, 8)}...` })
      await loadBatches()
    } catch (err) {
      setMessage({ kind: 'error', text: String(err.message || err) })
    } finally {
      setBusy(false)
    }
  }

  async function runCrunchbaseImport() {
    setBusy(true)
    setMessage({ kind: 'info', text: '' })
    try {
      const contactEnriched =
        cbContactEnriched === 'all'
          ? null
          : cbContactEnriched === 'yes'
            ? true
            : false
      const res = await api.importCrunchbase({
        status: cbStatus.trim() || undefined,
        contactEnriched,
        view: cbView.trim() || undefined,
        maxRecords: Number(cbMaxRecords) || 200,
        tableName: airtableTable.trim() || undefined,
      })
      setMessage({ kind: 'info', text: `Crunchbase import complete: ${res.record_count} entries.` })
      await loadBatches()
    } catch (err) {
      setMessage({ kind: 'error', text: String(err.message || err) })
    } finally {
      setBusy(false)
    }
  }

  async function runNewsImport() {
    setBusy(true)
    setMessage({ kind: 'info', text: '' })
    try {
      const res = await api.importNews({
        query: newsQuery.trim() || undefined,
        domains: newsDomains.trim() || undefined,
        language: newsLanguage.trim() || undefined,
        fromDate: newsFromDate || undefined,
        toDate: newsToDate || undefined,
        sortBy: 'publishedAt',
        pageSize: Number(newsPageSize) || undefined,
        maxPages: Number(newsMaxPages) || undefined,
      })
      setMessage({ kind: 'info', text: `News import complete: ${res.record_count} entries.` })
      await loadBatches()
    } catch (err) {
      setMessage({ kind: 'error', text: String(err.message || err) })
    } finally {
      setBusy(false)
    }
  }

  async function runCsvImport() {
    if (!csvFile) {
      setMessage({ kind: 'warn', text: 'Attach a CSV file first.' })
      return
    }

    setBusy(true)
    setMessage({ kind: 'info', text: '' })
    try {
      const res = isCrunchbase
        ? await api.uploadCrunchbaseCsv({ file: csvFile })
        : await api.uploadCsv({ file: csvFile, pipelineType: type })
      setMessage({ kind: 'info', text: `CSV imported: ${res.record_count} entries.` })
      await loadBatches()
    } catch (err) {
      setMessage({ kind: 'error', text: String(err.message || err) })
    } finally {
      setBusy(false)
    }
  }

  async function startDraftingForBatch() {
    if (!selectedBatchId) {
      return
    }
    setBusy(true)
    try {
      const res = await api.startDrafting(selectedBatchId)
      setMessage({ kind: 'info', text: `Drafting started. Created ${res.created} message rows.` })
      await loadBatches()
    } catch (err) {
      setMessage({ kind: 'error', text: String(err.message || err) })
    } finally {
      setBusy(false)
    }
  }

  async function pushToLeadspicker() {
    if (!selectedProjectId || pushCandidates.length === 0) {
      setMessage({ kind: 'warn', text: 'Need pushable entries and a project selected.' })
      return
    }
    setBusy(true)
    try {
      const res = await api.pushLeadspicker({
        entryIds: pushCandidates,
        projectId: Number(selectedProjectId),
      })
      setMessage({
        kind: 'info',
        text: `${isCrunchbase ? 'Cross-push' : 'LP push'} done: pushed ${res.pushed}, failed ${res.failed}, skipped ${res.skipped}.`,
      })
      await loadBatches()
      const entries = await api.getBatchEntries(selectedBatchId)
      setPushEntries(entries)
    } catch (err) {
      setMessage({ kind: 'error', text: String(err.message || err) })
    } finally {
      setBusy(false)
    }
  }

  async function pushToAirtable() {
    if (pushCandidates.length === 0) {
      setMessage({ kind: 'warn', text: 'Need pushable entries first.' })
      return
    }
    const manualTableRequired = !isLpCzech && !isCrunchbase
    if (manualTableRequired && !airtableTable.trim()) {
      setMessage({ kind: 'warn', text: 'Need pushable entries and Airtable table name.' })
      return
    }
    setBusy(true)
    try {
      const res = await api.pushAirtable({
        entryIds: pushCandidates,
        tableName: isLpCzech ? undefined : airtableTable.trim(),
      })
      setMessage({ kind: 'info', text: `Airtable push done: created ${res.created}, failed ${res.failed}, skipped ${res.skipped}.` })
      await loadBatches()
      const entries = await api.getBatchEntries(selectedBatchId)
      setPushEntries(entries)
    } catch (err) {
      setMessage({ kind: 'error', text: String(err.message || err) })
    } finally {
      setBusy(false)
    }
  }

  return (
    <>
      <section className="panel" style={{ display: 'grid', gap: '0.7rem' }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', flexWrap: 'wrap', gap: '0.7rem' }}>
          <div>
            <h1 style={{ margin: 0, fontSize: '1.35rem' }}>{pipelineCfg.label}</h1>
            <p style={{ margin: '0.2rem 0 0', fontSize: '0.82rem', color: 'var(--ink-soft)' }}>
              Import, dedup, and push controls for this pipeline.
            </p>
          </div>
          <div style={{ display: 'flex', gap: '0.4rem', alignItems: 'center' }}>
            <button className="btn" disabled={busy} onClick={refreshProjects}>
              Refresh Projects
            </button>
            <button className="btn" disabled={busy} onClick={loadBatches}>
              Refresh Batches
            </button>
          </div>
        </div>
        <MessageBox kind={message.kind} text={message.text} />
        {loading ? <MessageBox kind="info" text="Loading dashboard..." /> : null}
      </section>

      <section className="grid-2">
        <div className="panel" style={{ display: 'grid', gap: '0.65rem' }}>
          <h2 style={{ margin: 0, fontSize: '1rem' }}>Import Controls</h2>

          {isCrunchbase ? (
            <>
              <label style={{ fontSize: '0.78rem', fontWeight: 700 }}>Airtable Table</label>
              <input value={airtableTable} onChange={(e) => setAirtableTable(e.target.value)} placeholder="Crunchbase Source" />

              <label style={{ fontSize: '0.78rem', fontWeight: 700 }}>Status Filter</label>
              <input value={cbStatus} onChange={(e) => setCbStatus(e.target.value)} placeholder="Quality B - Contacted" />

              <label style={{ fontSize: '0.78rem', fontWeight: 700 }}>Contact Enriched</label>
              <select value={cbContactEnriched} onChange={(e) => setCbContactEnriched(e.target.value)}>
                <option value="all">All</option>
                <option value="yes">Yes</option>
                <option value="no">No</option>
              </select>

              <label style={{ fontSize: '0.78rem', fontWeight: 700 }}>Airtable View</label>
              <input value={cbView} onChange={(e) => setCbView(e.target.value)} placeholder="Optional view name" />

              <label style={{ fontSize: '0.78rem', fontWeight: 700 }}>Max Records</label>
              <input
                type="number"
                min={1}
                max={5000}
                value={cbMaxRecords}
                onChange={(e) => setCbMaxRecords(Math.max(1, Number(e.target.value) || 1))}
              />

              <button className="btn primary" disabled={busy} onClick={runCrunchbaseImport}>
                Fetch from Airtable (auto dedup)
              </button>
            </>
          ) : isNews ? (
            <>
              <label style={{ fontSize: '0.78rem', fontWeight: 700 }}>Query</label>
              <input
                value={newsQuery}
                onChange={(e) => setNewsQuery(e.target.value)}
                placeholder="Uses backend settings default when empty"
              />

              <label style={{ fontSize: '0.78rem', fontWeight: 700 }}>Domains (comma-separated)</label>
              <input
                value={newsDomains}
                onChange={(e) => setNewsDomains(e.target.value)}
                placeholder="techcrunch.com,venturebeat.com"
              />

              <div className="grid-2" style={{ gap: '0.6rem' }}>
                <div>
                  <label style={{ fontSize: '0.78rem', fontWeight: 700 }}>Language</label>
                  <input value={newsLanguage} onChange={(e) => setNewsLanguage(e.target.value)} placeholder="en" />
                </div>
                <div>
                  <label style={{ fontSize: '0.78rem', fontWeight: 700 }}>Page Size</label>
                  <input
                    type="number"
                    min={1}
                    max={100}
                    value={newsPageSize}
                    onChange={(e) => setNewsPageSize(Math.max(1, Math.min(100, Number(e.target.value) || 1)))}
                  />
                </div>
              </div>

              <div className="grid-2" style={{ gap: '0.6rem' }}>
                <div>
                  <label style={{ fontSize: '0.78rem', fontWeight: 700 }}>From Date</label>
                  <input type="date" value={newsFromDate} onChange={(e) => setNewsFromDate(e.target.value)} />
                </div>
                <div>
                  <label style={{ fontSize: '0.78rem', fontWeight: 700 }}>To Date</label>
                  <input type="date" value={newsToDate} onChange={(e) => setNewsToDate(e.target.value)} />
                </div>
              </div>

              <label style={{ fontSize: '0.78rem', fontWeight: 700 }}>Max Pages</label>
              <input
                type="number"
                min={1}
                max={50}
                value={newsMaxPages}
                onChange={(e) => setNewsMaxPages(Math.max(1, Math.min(50, Number(e.target.value) || 1)))}
              />

              <button className="btn primary" disabled={busy} onClick={runNewsImport}>
                Fetch NewsAPI (auto dedup)
              </button>
              <div style={{ fontSize: '0.76rem', color: 'var(--ink-soft)' }}>
                Multi-page pagination is enabled. Empty fields use server settings defaults.
              </div>
            </>
          ) : (
            <>
              <label style={{ fontSize: '0.78rem', fontWeight: 700 }}>Leadspicker Project</label>
              <select value={selectedProjectId} onChange={(e) => setSelectedProjectId(e.target.value)}>
                <option value="">Select project...</option>
                {projects.map((p) => (
                  <option key={p.id} value={p.lp_project_id}>
                    {p.lp_project_id} · {p.name}
                  </option>
                ))}
              </select>

              <button className="btn primary" disabled={busy || !selectedProjectId} onClick={runLpImport}>
                Fetch from LP (auto dedup)
              </button>
            </>
          )}

          {!isNews ? (
            <>
              <hr style={{ border: 0, borderTop: '1px solid var(--line)' }} />
              <label style={{ fontSize: '0.78rem', fontWeight: 700 }}>
                CSV Upload ({isCrunchbase ? 'Airtable export accepted' : 'semicolon-delimited LP format'})
              </label>
              <input type="file" accept=".csv" onChange={(e) => setCsvFile(e.target.files?.[0] || null)} />
              <button className="btn" disabled={busy || !csvFile} onClick={runCsvImport}>
                Upload CSV + Import
              </button>
            </>
          ) : null}
        </div>

        <div className="panel" style={{ display: 'grid', gap: '0.65rem' }}>
          <h2 style={{ margin: 0, fontSize: '1rem' }}>Push Controls</h2>
          <label style={{ fontSize: '0.78rem', fontWeight: 700 }}>Batch</label>
          <select value={selectedBatchId} onChange={(e) => setSelectedBatchId(e.target.value)}>
            <option value="">Select batch...</option>
            {pipelineBatches.map((b) => (
              <option key={b.id} value={b.id}>
                {String(b.id).slice(0, 8)}... · entries {b.progress?.total || 0}
              </option>
            ))}
          </select>

          <div style={{ fontSize: '0.79rem', color: 'var(--ink-soft)' }}>
            Push candidates ({isCrunchbase ? 'pushed-ready + non-empty message' : isNews ? 'YES only' : 'YES/CC'}): {pushLoading ? 'loading...' : pushCandidates.length}
          </div>

          <label style={{ display: 'flex', alignItems: 'center', gap: '0.45rem', fontSize: '0.79rem', color: 'var(--ink-soft)' }}>
            <input
              type="checkbox"
              checked={analysisYesOnly}
              onChange={(e) => setAnalysisYesOnly(e.target.checked)}
              style={{ width: 16, height: 16 }}
            />
            Before analysis: hide rows where AI classifier == NO
          </label>

          <div style={{ fontSize: '0.79rem', color: 'var(--ink-soft)' }}>
            Analysis row count with filter: {pushLoading ? 'loading...' : analysisEntries.length}
          </div>

          {selectedBatchId ? (
            <Link className="btn" to={analysisUrl(selectedBatchId)}>
              Open Analysis ({analysisYesOnly ? 'AI YES only' : 'all rows'})
            </Link>
          ) : null}

          <label style={{ fontSize: '0.78rem', fontWeight: 700 }}>Leadspicker Project (for LP push / CB cross-push)</label>
          <select value={selectedProjectId} onChange={(e) => setSelectedProjectId(e.target.value)}>
            <option value="">Select project...</option>
            {projects.map((p) => (
              <option key={p.id} value={p.lp_project_id}>
                {p.lp_project_id} · {p.name}
              </option>
            ))}
          </select>

          <div style={{ display: 'flex', gap: '0.45rem', flexWrap: 'wrap' }}>
            {!isCrunchbase ? (
              <button className="btn" disabled={!selectedBatchId || busy} onClick={startDraftingForBatch}>
                Start Drafting
              </button>
            ) : null}
            <button className="btn primary" disabled={!selectedProjectId || busy || pushCandidates.length === 0} onClick={pushToLeadspicker}>
              {isCrunchbase ? 'Cross-push to Leadspicker' : 'Push to Leadspicker'}
            </button>
          </div>

          {isLpCzech ? (
            <div style={{ fontSize: '0.79rem', color: 'var(--ink-soft)' }}>
              Czech Airtable target is configured on backend env (`AIRTABLE_LP_CZECH_TABLE`).
            </div>
          ) : (
            <>
              <label style={{ fontSize: '0.78rem', fontWeight: 700 }}>Airtable Table</label>
              <input value={airtableTable} onChange={(e) => setAirtableTable(e.target.value)} />
            </>
          )}
          <button
            className="btn warn"
            disabled={busy || pushCandidates.length === 0 || (!isLpCzech && !isCrunchbase && !airtableTable.trim())}
            onClick={pushToAirtable}
          >
            {isCrunchbase ? 'Batch Update Airtable' : 'Push to Airtable'}
          </button>
        </div>
      </section>

      {selectedBatchId ? (
        <section className="panel" style={{ display: 'grid', gap: '0.65rem' }}>
          <h2 style={{ margin: 0, fontSize: '1rem' }}>
            Entry Preview ({analysisEntries.length})
          </h2>
          <div style={{ maxHeight: 300, overflow: 'auto', border: '1px solid var(--line)', borderRadius: 12 }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.8rem' }}>
              <thead style={{ position: 'sticky', top: 0, background: '#f8f6ef' }}>
                <tr>
                  {isCrunchbase ? (
                    <>
                      <th style={{ textAlign: 'left', padding: '0.45rem 0.5rem' }}>Company</th>
                      <th style={{ textAlign: 'left', padding: '0.45rem 0.5rem' }}>Entry status</th>
                      <th style={{ textAlign: 'left', padding: '0.45rem 0.5rem' }}>Series</th>
                      <th style={{ textAlign: 'left', padding: '0.45rem 0.5rem' }}>Funding</th>
                      <th style={{ textAlign: 'left', padding: '0.45rem 0.5rem' }}>Message fin</th>
                    </>
                  ) : isNews ? (
                    <>
                      <th style={{ textAlign: 'left', padding: '0.45rem 0.5rem' }}>Headline</th>
                      <th style={{ textAlign: 'left', padding: '0.45rem 0.5rem' }}>Source</th>
                      <th style={{ textAlign: 'left', padding: '0.45rem 0.5rem' }}>Published</th>
                      <th style={{ textAlign: 'left', padding: '0.45rem 0.5rem' }}>URL</th>
                    </>
                  ) : (
                    <>
                      <th style={{ textAlign: 'left', padding: '0.45rem 0.5rem' }}>Contact</th>
                      <th style={{ textAlign: 'left', padding: '0.45rem 0.5rem' }}>Company</th>
                      <th style={{ textAlign: 'left', padding: '0.45rem 0.5rem' }}>AI classifier</th>
                      <th style={{ textAlign: 'left', padding: '0.45rem 0.5rem' }}>Summary</th>
                    </>
                  )}
                </tr>
              </thead>
              <tbody>
                {analysisEntries.slice(0, 60).map((row) => {
                  const meta = cbMeta(row)
                  return (
                    <tr key={row.id}>
                      {isCrunchbase ? (
                        <>
                          <td style={{ padding: '0.45rem 0.5rem', borderTop: '1px solid #efeee8' }}>
                            {row.signals?.companies?.name_raw || meta.Name || 'Unknown'}
                          </td>
                          <td style={{ padding: '0.45rem 0.5rem', borderTop: '1px solid #efeee8' }}>
                            {cbWorkflowStatus(row) || 'new'}
                          </td>
                          <td style={{ padding: '0.45rem 0.5rem', borderTop: '1px solid #efeee8' }}>
                            {String(meta.series || meta.Series || 'n/a')}
                          </td>
                          <td style={{ padding: '0.45rem 0.5rem', borderTop: '1px solid #efeee8' }}>
                            {String(meta.last_funding_amount_usd || meta['Last Funding Amount'] || 'n/a')}
                          </td>
                          <td style={{ padding: '0.45rem 0.5rem', borderTop: '1px solid #efeee8' }}>
                            {String(cbMessage(row) || 'empty').slice(0, 90)}
                          </td>
                        </>
                      ) : isNews ? (
                        <>
                          <td style={{ padding: '0.45rem 0.5rem', borderTop: '1px solid #efeee8' }}>
                            {String(row.signals?.content_title || 'Untitled').slice(0, 90)}
                          </td>
                          <td style={{ padding: '0.45rem 0.5rem', borderTop: '1px solid #efeee8' }}>
                            {String((row.signals?.source_metadata || {}).source_name || 'n/a')}
                          </td>
                          <td style={{ padding: '0.45rem 0.5rem', borderTop: '1px solid #efeee8' }}>
                            {row.signals?.published_at ? new Date(row.signals.published_at).toLocaleDateString() : 'n/a'}
                          </td>
                          <td style={{ padding: '0.45rem 0.5rem', borderTop: '1px solid #efeee8' }}>
                            {row.signals?.content_url ? (
                              <a href={row.signals.content_url} target="_blank" rel="noreferrer" style={{ textDecoration: 'underline' }}>
                                open
                              </a>
                            ) : (
                              'n/a'
                            )}
                          </td>
                        </>
                      ) : (
                        <>
                          <td style={{ padding: '0.45rem 0.5rem', borderTop: '1px solid #efeee8' }}>
                            {row.contacts?.full_name || `${row.contacts?.first_name || ''} ${row.contacts?.last_name || ''}`.trim() || 'Unknown'}
                          </td>
                          <td style={{ padding: '0.45rem 0.5rem', borderTop: '1px solid #efeee8' }}>
                            {row.signals?.companies?.name_raw || 'Unknown'}
                          </td>
                          <td style={{ padding: '0.45rem 0.5rem', borderTop: '1px solid #efeee8' }}>
                            {String(row.signals?.ai_classifier || 'n/a')}
                          </td>
                          <td style={{ padding: '0.45rem 0.5rem', borderTop: '1px solid #efeee8' }}>
                            {String(row.signals?.content_summary || 'No summary').slice(0, 130)}
                          </td>
                        </>
                      )}
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        </section>
      ) : null}

      <section className="panel" style={{ display: 'grid', gap: '0.65rem' }}>
        <h2 style={{ margin: 0, fontSize: '1rem' }}>Batches ({getLabel(type)})</h2>

        {pipelineBatches.length === 0 ? (
          <div style={{ color: 'var(--ink-soft)' }}>No batches for this pipeline yet.</div>
        ) : (
          <div style={{ display: 'grid', gap: '0.55rem' }}>
            {pipelineBatches.map((batch) => {
              const progress = batch.progress || { total: 0, yes: 0, no: 0, cc: 0, unlabeled: 0 }
              const labeled = progress.yes + progress.no + progress.cc
              return (
                <div
                  key={batch.id}
                  className="panel"
                  style={{ margin: 0, padding: '0.75rem', display: 'grid', gap: '0.45rem' }}
                >
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: '0.5rem' }}>
                    <div>
                      <div style={{ fontWeight: 700, fontSize: '0.88rem' }}>{String(batch.id).slice(0, 8)}...</div>
                      <div style={{ fontSize: '0.74rem', color: 'var(--ink-soft)' }}>
                        imported {new Date(batch.imported_at).toLocaleString()}
                      </div>
                    </div>
                    <StatusBadge status={batch.status || 'new'} />
                  </div>
                  <ProgressBar value={labeled} total={Math.max(progress.total, 1)} />
                  <div style={{ fontSize: '0.76rem', display: 'flex', gap: '0.55rem', flexWrap: 'wrap' }}>
                    <span>YES {progress.yes}</span>
                    <span>NO {progress.no}</span>
                    <span>CC {progress.cc}</span>
                    <span>Unlabeled {progress.unlabeled}</span>
                    <span>Total {progress.total}</span>
                  </div>
                  <div style={{ display: 'flex', gap: '0.45rem', flexWrap: 'wrap' }}>
                    <Link className="btn" to={analysisUrl(batch.id)}>
                      Open Analysis
                    </Link>
                    {!isCrunchbase ? (
                      <Link className="btn warn" to={`/draft/${batch.id}`}>
                        Open Drafting
                      </Link>
                    ) : null}
                  </div>
                </div>
              )
            })}
          </div>
        )}
      </section>
    </>
  )
}
