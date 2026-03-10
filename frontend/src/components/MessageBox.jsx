export default function MessageBox({ kind = 'info', text }) {
  if (!text) {
    return null
  }

  const byKind = {
    info: { bg: 'var(--mint-soft)', border: '#9edecf', color: '#10584a' },
    warn: { bg: 'var(--amber-soft)', border: '#efcb9f', color: '#7b4a1a' },
    error: { bg: '#fde7e5', border: '#e7b0aa', color: '#8f2f27' },
  }

  const style = byKind[kind] || byKind.info

  return (
    <div
      style={{
        border: `1px solid ${style.border}`,
        background: style.bg,
        color: style.color,
        borderRadius: '10px',
        padding: '0.55rem 0.7rem',
        fontSize: '0.8rem',
        fontWeight: 600,
      }}
    >
      {text}
    </div>
  )
}
