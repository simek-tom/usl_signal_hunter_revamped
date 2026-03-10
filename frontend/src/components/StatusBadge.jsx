const STATUS_STYLE = {
  new: 'gray',
  analyzed: 'amber',
  enriched: 'amber',
  drafted: 'amber',
  'pushed-ready': 'amber',
  pushed: 'green',
  eliminated: 'red',
}

export default function StatusBadge({ status }) {
  const cls = STATUS_STYLE[status] || 'gray'
  return <span className={`badge ${cls}`}>{status || 'unknown'}</span>
}
