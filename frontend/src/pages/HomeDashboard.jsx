import { useEffect, useState } from 'react'
import { Link } from 'react-router-dom'
import { api } from '../lib/api'
import { usePipelineConfigs } from '../context/PipelineConfigContext'
import MessageBox from '../components/MessageBox'

export default function HomeDashboard() {
  const { configs, getLabel } = usePipelineConfigs()
  const [stats, setStats] = useState({})
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')

  useEffect(() => {
    let alive = true
    async function load() {
      setLoading(true)
      setError('')
      try {
        const results = await Promise.all(
          configs.map(async (p) => {
            try {
              const s = await api.getPipelineStats(p.pipeline_key)
              return [p.pipeline_key, s]
            } catch {
              return [p.pipeline_key, null]
            }
          }),
        )
        if (alive) setStats(Object.fromEntries(results))
      } catch (err) {
        if (alive) setError(String(err.message || err))
      } finally {
        if (alive) setLoading(false)
      }
    }
    if (configs.length > 0) load()
    else setLoading(false)
    return () => { alive = false }
  }, [configs])

  const totalImported = Object.values(stats).reduce((a, s) => a + (s?.staging?.total || 0), 0)
  const totalUnlabeled = Object.values(stats).reduce((a, s) => a + (s?.staging?.unlabeled || 0), 0)
  const totalDrafted = Object.values(stats).reduce((a, s) => a + (s?.entries?.drafted || 0), 0)

  return (
    <>
      <section className="panel" style={{ display: 'grid', gap: '0.9rem' }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', gap: '0.8rem', flexWrap: 'wrap' }}>
          <div>
            <h1 style={{ margin: 0, fontSize: '1.4rem' }}>Dashboard</h1>
            <p style={{ margin: '0.2rem 0 0', color: 'var(--ink-soft)', fontSize: '0.85rem' }}>
              Pipeline overview and quick navigation.
            </p>
          </div>
          <div style={{ display: 'flex', gap: '0.45rem', flexWrap: 'wrap' }}>
            <Link className="btn" to="/settings">Open Settings</Link>
            <button className="btn" onClick={() => location.reload()}>Refresh</button>
          </div>
        </div>

        <MessageBox kind="error" text={error} />
        {loading ? <MessageBox kind="info" text="Loading pipeline stats..." /> : null}

        <div className="grid-3">
          <div className="panel metric" style={{ margin: 0 }}>
            <span className="metric-label">Total Imported</span>
            <span className="metric-value">{totalImported}</span>
          </div>
          <div className="panel metric" style={{ margin: 0 }}>
            <span className="metric-label">Awaiting Analysis</span>
            <span className="metric-value">{totalUnlabeled}</span>
          </div>
          <div className="panel metric" style={{ margin: 0 }}>
            <span className="metric-label">Drafted</span>
            <span className="metric-value">{totalDrafted}</span>
          </div>
        </div>
      </section>

      <section className="grid-2">
        {configs.map((p) => {
          const s = stats[p.pipeline_key]
          const staging = s?.staging || {}
          const entries = s?.entries || {}
          return (
            <Link key={p.pipeline_key} className="panel" to={`/pipeline/${p.pipeline_key}`} style={{ display: 'grid', gap: '0.4rem', textDecoration: 'none' }}>
              <div style={{ fontWeight: 700 }}>{p.label}</div>
              <div style={{ display: 'flex', gap: '0.8rem', fontSize: '0.8rem', color: 'var(--ink-soft)', flexWrap: 'wrap' }}>
                <span>{staging.total || 0} imported</span>
                <span>{staging.unlabeled || 0} unlabeled</span>
                <span>{staging.yes || 0} yes</span>
              </div>
              <div style={{ display: 'flex', gap: '0.8rem', fontSize: '0.8rem', color: 'var(--ink-soft)', flexWrap: 'wrap' }}>
                <span>{entries.total || 0} entries</span>
                <span>{entries.drafted || 0} drafted</span>
                <span>{entries.pushed || 0} pushed</span>
              </div>
            </Link>
          )
        })}
      </section>
    </>
  )
}
