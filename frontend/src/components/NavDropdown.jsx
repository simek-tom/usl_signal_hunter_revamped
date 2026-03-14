import { useState, useRef, useEffect } from 'react'
import { NavLink, useParams } from 'react-router-dom'
import { usePipelineConfigs } from '../context/PipelineConfigContext'

export default function NavDropdown({ sourceType, label }) {
  const { getConfigsBySource } = usePipelineConfigs()
  const [open, setOpen] = useState(false)
  const ref = useRef(null)
  const { type } = useParams()

  const configs = getConfigsBySource(sourceType)
  const isActive = configs.some((c) => c.pipeline_key === type)

  useEffect(() => {
    function handleClick(e) {
      if (ref.current && !ref.current.contains(e.target)) {
        setOpen(false)
      }
    }
    document.addEventListener('mousedown', handleClick)
    return () => document.removeEventListener('mousedown', handleClick)
  }, [])

  return (
    <div ref={ref} style={{ position: 'relative' }}>
      <button
        className={`nav-link${isActive ? ' active' : ''}`}
        onClick={() => setOpen((v) => !v)}
      >
        {label} ▾
      </button>
      {open ? (
        <div
          style={{
            position: 'absolute',
            top: '100%',
            left: 0,
            marginTop: '0.3rem',
            background: 'var(--card)',
            border: '1px solid var(--line)',
            borderRadius: 10,
            padding: '0.3rem 0',
            minWidth: 200,
            zIndex: 30,
            boxShadow: '0 8px 24px rgba(17,34,31,0.12)',
          }}
        >
          {configs.map((cfg) => (
            <NavLink
              key={cfg.pipeline_key}
              to={`/pipeline/${cfg.pipeline_key}`}
              onClick={() => setOpen(false)}
              style={{
                display: 'block',
                padding: '0.45rem 0.75rem',
                fontSize: '0.82rem',
                fontWeight: 600,
              }}
            >
              {cfg.label}
            </NavLink>
          ))}
          <div style={{ borderTop: '1px solid var(--line)', margin: '0.2rem 0' }} />
          <NavLink
            to={`/pipeline/new?source=${sourceType}`}
            onClick={() => setOpen(false)}
            style={{
              display: 'block',
              padding: '0.45rem 0.75rem',
              fontSize: '0.82rem',
              fontWeight: 600,
              color: 'var(--mint)',
            }}
          >
            + Add New Sub-branch
          </NavLink>
        </div>
      ) : null}
    </div>
  )
}