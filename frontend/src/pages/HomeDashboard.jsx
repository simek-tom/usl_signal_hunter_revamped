import { useEffect, useMemo, useState } from 'react'
import { Link } from 'react-router-dom'
import { api } from '../lib/api'
import { PIPELINES, pipelineLabel } from '../lib/pipeline'
import ProgressBar from '../components/ProgressBar'
import StatusBadge from '../components/StatusBadge'
import MessageBox from '../components/MessageBox'

function BatchRow({ batch }) {
  const progress = batch.progress || { total: 0, yes: 0, no: 0, cc: 0, unlabeled: 0 }
  const labeled = progress.yes + progress.no + progress.cc

  return (
    <div className="panel" style={{ padding: '0.8rem', display: 'grid', gap: '0.5rem' }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', gap: '0.5rem', alignItems: 'center' }}>
        <div>
          <div style={{ fontWeight: 700, fontSize: '0.9rem' }}>{pipelineLabel(batch.pipeline_type)}</div>
          <div style={{ fontSize: '0.75rem', color: 'var(--ink-soft)' }}>Batch {batch.id.slice(0, 8)}...</div>
        </div>
        <StatusBadge status={batch.status || 'new'} />
      </div>

      <ProgressBar value={labeled} total={Math.max(progress.total, 1)} />

      <div style={{ display: 'flex', gap: '0.6rem', flexWrap: 'wrap', fontSize: '0.74rem' }}>
        <span>YES {progress.yes}</span>
        <span>NO {progress.no}</span>
        <span>CC {progress.cc}</span>
        <span>Unlabeled {progress.unlabeled}</span>
      </div>

      <div style={{ display: 'flex', gap: '0.45rem', flexWrap: 'wrap' }}>
        <Link className="btn" to={`/analyze/${batch.id}`}>
          Analyze
        </Link>
        {batch.pipeline_type !== 'crunchbase' ? (
          <Link className="btn warn" to={`/draft/${batch.id}`}>
            Draft
          </Link>
        ) : null}
        <Link className="btn" to={`/pipeline/${batch.pipeline_type}`}>
          Pipeline
        </Link>
      </div>
    </div>
  )
}

export default function HomeDashboard() {
  const [batches, setBatches] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')

  useEffect(() => {
    let alive = true
    async function load() {
      setLoading(true)
      setError('')
      try {
        const data = await api.getBatches()
        if (alive) {
          setBatches(data)
        }
      } catch (err) {
        if (alive) {
          setError(String(err.message || err))
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

  const quickStats = useMemo(() => {
    const byPipeline = PIPELINES.map((p) => {
      const rel = batches.filter((b) => b.pipeline_type === p.key)
      const entries = rel.reduce((acc, b) => acc + (b.progress?.total || 0), 0)
      return {
        key: p.key,
        label: p.label,
        batches: rel.length,
        entries,
      }
    })

    return {
      totalBatches: batches.length,
      totalEntries: batches.reduce((acc, b) => acc + (b.progress?.total || 0), 0),
      byPipeline,
    }
  }, [batches])

  const activeBatches = useMemo(
    () => batches.filter((b) => (b.progress?.unlabeled || 0) > 0 || (b.progress?.total || 0) > 0).slice(0, 12),
    [batches],
  )

  return (
    <>
      <section className="panel" style={{ display: 'grid', gap: '0.9rem' }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', gap: '0.8rem', flexWrap: 'wrap' }}>
          <div>
            <h1 style={{ margin: 0, fontSize: '1.4rem' }}>Dashboard</h1>
            <p style={{ margin: '0.2rem 0 0', color: 'var(--ink-soft)', fontSize: '0.85rem' }}>
              Pipeline overview, active batches, and quick navigation.
            </p>
          </div>
          <div style={{ display: 'flex', gap: '0.45rem', flexWrap: 'wrap' }}>
            <Link className="btn" to="/settings">
              Open Settings
            </Link>
            <button className="btn" onClick={() => location.reload()}>
              Refresh
            </button>
          </div>
        </div>

        <MessageBox kind="error" text={error} />
        {loading ? <MessageBox kind="info" text="Loading batches..." /> : null}

        <div className="grid-3">
          <div className="panel metric" style={{ margin: 0 }}>
            <span className="metric-label">Total Batches</span>
            <span className="metric-value">{quickStats.totalBatches}</span>
          </div>
          <div className="panel metric" style={{ margin: 0 }}>
            <span className="metric-label">Tracked Entries</span>
            <span className="metric-value">{quickStats.totalEntries}</span>
          </div>
          <div className="panel metric" style={{ margin: 0 }}>
            <span className="metric-label">Pipelines Live</span>
            <span className="metric-value">{quickStats.byPipeline.filter((p) => p.batches > 0).length}</span>
          </div>
        </div>
      </section>

      <section className="grid-2">
        {quickStats.byPipeline.map((p) => (
          <Link key={p.key} className="panel" to={`/pipeline/${p.key}`} style={{ display: 'grid', gap: '0.4rem' }}>
            <div style={{ fontWeight: 700 }}>{p.label}</div>
            <div style={{ color: 'var(--ink-soft)', fontSize: '0.8rem' }}>{p.batches} batches</div>
            <div style={{ fontSize: '1.15rem', fontWeight: 700 }}>{p.entries} entries</div>
          </Link>
        ))}
      </section>

      <section style={{ display: 'grid', gap: '0.75rem' }}>
        <h2 style={{ margin: 0, fontSize: '1.05rem' }}>Active Batches</h2>
        {activeBatches.length === 0 ? (
          <div className="panel" style={{ color: 'var(--ink-soft)' }}>
            No batches yet.
          </div>
        ) : (
          <div className="grid-3">
            {activeBatches.map((batch) => (
              <BatchRow key={batch.id} batch={batch} />
            ))}
          </div>
        )}
      </section>
    </>
  )
}
