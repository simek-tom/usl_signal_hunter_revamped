export default function ProgressBar({ value, total }) {
  const pct = total > 0 ? Math.round((value / total) * 100) : 0
  return (
    <div>
      <div className="progress-track" role="progressbar" aria-valuemin={0} aria-valuemax={100} aria-valuenow={pct}>
        <div className="progress-bar" style={{ width: `${pct}%` }} />
      </div>
      <div style={{ marginTop: '0.35rem', fontSize: '0.76rem', color: 'var(--ink-soft)' }}>{pct}%</div>
    </div>
  )
}
