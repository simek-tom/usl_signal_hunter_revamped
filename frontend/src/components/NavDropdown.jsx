import { useState, useRef, useEffect } from 'react'
import { NavLink, useParams } from 'react-router-dom'
import { usePipelineConfigs } from '../context/PipelineConfigContext'
import PipelineConfigModal from './PipelineConfigModal'

export default function NavDropdown({ sourceType, label }) {
  const { getConfigsBySource } = usePipelineConfigs()
  const [open, setOpen] = useState(false)
  const [modal, setModal] = useState(null) // null | { mode: 'create'|'edit', config: null|obj }
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

  function openCreate() {
    setOpen(false)
    setModal({ mode: 'create', config: null })
  }

  function openEdit(cfg, e) {
    e.preventDefault()
    e.stopPropagation()
    setOpen(false)
    setModal({ mode: 'edit', config: cfg })
  }

  return (
    <>
      <div ref={ref} style={{ position: 'relative' }}>
        <button
          className={`nav-link${isActive ? ' active' : ''}`}
          onClick={() => setOpen((v) => !v)}
        >
          {label} ▾
        </button>

        {open && (
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
              minWidth: 220,
              zIndex: 30,
              boxShadow: '0 8px 24px rgba(17,34,31,0.12)',
            }}
          >
            {configs.map((cfg) => (
              <div
                key={cfg.pipeline_key}
                style={{ display: 'flex', alignItems: 'center' }}
              >
                <NavLink
                  to={`/pipeline/${cfg.pipeline_key}`}
                  onClick={() => setOpen(false)}
                  style={{
                    flex: 1,
                    display: 'block',
                    padding: '0.45rem 0.75rem',
                    fontSize: '0.82rem',
                    fontWeight: 600,
                  }}
                >
                  {cfg.label}
                </NavLink>
                <button
                  onClick={(e) => openEdit(cfg, e)}
                  title="Edit sub-branch"
                  style={{
                    border: 'none',
                    background: 'none',
                    color: 'var(--ink-soft)',
                    padding: '0.45rem 0.65rem',
                    fontSize: '0.82rem',
                    cursor: 'pointer',
                    lineHeight: 1,
                  }}
                >
                  ✎
                </button>
              </div>
            ))}

            <div style={{ borderTop: '1px solid var(--line)', margin: '0.2rem 0' }} />

            <button
              onClick={openCreate}
              style={{
                display: 'block',
                width: '100%',
                textAlign: 'left',
                border: 'none',
                background: 'none',
                padding: '0.45rem 0.75rem',
                fontSize: '0.82rem',
                fontWeight: 600,
                color: 'var(--mint)',
                cursor: 'pointer',
              }}
            >
              + Add New Sub-branch
            </button>
          </div>
        )}
      </div>

      {modal && (
        <PipelineConfigModal
          mode={modal.mode}
          sourceType={sourceType}
          config={modal.config}
          onClose={() => setModal(null)}
          onSaved={() => setModal(null)}
        />
      )}
    </>
  )
}
