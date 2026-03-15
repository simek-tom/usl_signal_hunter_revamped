import { useState, useMemo } from 'react'
import { createPortal } from 'react-dom'
import { api } from '../lib/api'
import { usePipelineConfigs } from '../context/PipelineConfigContext'

// All 15 internal fields with their labels and default LP API keys
const FIELD_CONFIG = [
  { key: 'first_name',            label: 'First Name',               defaultLpKey: 'first_name' },
  { key: 'last_name',             label: 'Last Name',                defaultLpKey: 'last_name' },
  { key: 'email',                 label: 'Email',                    defaultLpKey: 'email' },
  { key: 'contact_linkedin',      label: 'Contact LinkedIn',         defaultLpKey: 'linkedin' },
  { key: 'position',              label: 'Position / Role',          defaultLpKey: 'position' },
  { key: 'company_name',          label: 'Company Name',             defaultLpKey: 'company_name' },
  { key: 'company_website',       label: 'Company Website',          defaultLpKey: 'company_website' },
  { key: 'company_linkedin',      label: 'Company LinkedIn',         defaultLpKey: 'company_linkedin' },
  { key: 'company_country',       label: 'Company Country',          defaultLpKey: 'country' },
  { key: 'company_employee_count',label: 'Employee Count',           defaultLpKey: 'company_employees' },
  { key: 'content_url',           label: 'Content URL (dedup key)',  defaultLpKey: 'post_url' },
  { key: 'content_text',          label: 'Content Text',             defaultLpKey: 'post_content' },
  { key: 'content_summary',       label: 'Content Summary',          defaultLpKey: 'summary' },
  { key: 'ai_classifier',         label: 'AI Classifier',            defaultLpKey: 'ai_classifier' },
  { key: 'source_robot',          label: 'Source Robot',             defaultLpKey: 'source_robot' },
]

const INTERNAL_FIELDS = FIELD_CONFIG.map((f) => f.key)
const PUSH_SOURCE_FIELDS = [...INTERNAL_FIELDS, 'message']

// ── LP project picker ─────────────────────────────────────────────────────────

function LpProjectPicker({ selectedIds, onChange }) {
  const [projects, setProjects] = useState([])
  const [loading, setLoading] = useState(false)
  const [open, setOpen] = useState(false)
  const [error, setError] = useState('')

  async function load(refresh = false) {
    setLoading(true)
    setError('')
    try {
      const data = refresh ? await api.refreshProjects() : await api.listProjects()
      setProjects(data)
      setOpen(true)
    } catch (err) {
      setError(err.message)
    } finally {
      setLoading(false)
    }
  }

  function toggle(lpId) {
    onChange(
      selectedIds.includes(lpId)
        ? selectedIds.filter((id) => id !== lpId)
        : [...selectedIds, lpId]
    )
  }

  return (
    <div style={{ display: 'grid', gap: '0.4rem' }}>
      <div style={{ display: 'flex', gap: '0.4rem' }}>
        <button
          type="button"
          className="btn"
          style={{ fontSize: '0.78rem', padding: '0.3rem 0.6rem' }}
          disabled={loading}
          onClick={() => (open ? setOpen(false) : load(false))}
        >
          {loading ? 'Loading…' : open ? 'Hide projects' : 'Browse LP projects'}
        </button>
        {open && (
          <button
            type="button"
            className="btn"
            style={{ fontSize: '0.78rem', padding: '0.3rem 0.6rem' }}
            disabled={loading}
            onClick={() => load(true)}
          >
            Refresh
          </button>
        )}
      </div>
      {error && <span style={{ fontSize: '0.78rem', color: 'var(--rose)' }}>{error}</span>}
      {open && projects.length > 0 && (
        <div
          style={{
            border: '1px solid var(--line)',
            borderRadius: 8,
            maxHeight: 180,
            overflowY: 'auto',
            background: 'white',
          }}
        >
          {projects.map((p) => {
            const checked = selectedIds.includes(p.lp_project_id)
            return (
              <label
                key={p.lp_project_id}
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  gap: '0.5rem',
                  padding: '0.4rem 0.65rem',
                  cursor: 'pointer',
                  borderBottom: '1px solid var(--line)',
                  fontSize: '0.8rem',
                  background: checked ? 'var(--mint-soft)' : 'white',
                }}
              >
                <input
                  type="checkbox"
                  checked={checked}
                  onChange={() => toggle(p.lp_project_id)}
                  style={{ width: 14, height: 14, flexShrink: 0 }}
                />
                <span style={{ fontWeight: 600 }}>{p.lp_project_id}</span>
                <span style={{ color: 'var(--ink-soft)' }}>· {p.name}</span>
              </label>
            )
          })}
        </div>
      )}
    </div>
  )
}

// ── API field mapping (LP API columns → internal fields) ──────────────────────

function ApiFieldMapping({ projectIds, apiFieldMap, onChange }) {
  const [selectedProjectId, setSelectedProjectId] = useState(
    projectIds.length > 0 ? String(projectIds[0]) : ''
  )
  const [preview, setPreview] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')

  async function loadColumns() {
    const id = Number(selectedProjectId)
    if (!id) return
    setLoading(true)
    setError('')
    try {
      const data = await api.getLpProjectPreview(id)
      setPreview(data)
    } catch (err) {
      setError(err.message)
    } finally {
      setLoading(false)
    }
  }

  // First preview row for example values
  const exampleRow = preview?.rows?.[0] || {}

  // Live parsed preview: apply current mapping to each preview row
  const parsedRows = useMemo(() => {
    if (!preview?.rows?.length) return []
    return preview.rows.map((row) => {
      const parsed = {}
      for (const field of FIELD_CONFIG) {
        const lpKey = apiFieldMap[field.key] || field.defaultLpKey
        parsed[field.key] = row[lpKey] || ''
      }
      return parsed
    })
  }, [preview, apiFieldMap])

  function setMapping(fieldKey, lpKey) {
    const next = { ...apiFieldMap }
    if (lpKey) {
      next[fieldKey] = lpKey
    } else {
      delete next[fieldKey]
    }
    onChange(next)
  }

  return (
    <div style={{ display: 'grid', gap: '0.75rem' }}>
      {/* Load columns controls */}
      <div style={{ display: 'flex', gap: '0.4rem', alignItems: 'center' }}>
        {projectIds.length > 0 ? (
          <>
            <select
              value={selectedProjectId}
              onChange={(e) => setSelectedProjectId(e.target.value)}
              style={{ flex: 1, fontSize: '0.8rem' }}
            >
              <option value="">Select project to load columns…</option>
              {projectIds.map((id) => (
                <option key={id} value={id}>{id}</option>
              ))}
            </select>
            <button
              type="button"
              className="btn"
              style={{ fontSize: '0.78rem', padding: '0.3rem 0.6rem', whiteSpace: 'nowrap' }}
              disabled={!selectedProjectId || loading}
              onClick={loadColumns}
            >
              {loading ? 'Loading…' : 'Load columns'}
            </button>
          </>
        ) : (
          <span style={{ fontSize: '0.78rem', color: 'var(--ink-soft)', fontStyle: 'italic' }}>
            Add LP project IDs above to load columns for mapping.
          </span>
        )}
      </div>
      {error && <span style={{ fontSize: '0.78rem', color: 'var(--rose)' }}>{error}</span>}
      {preview && (
        <div style={{ fontSize: '0.75rem', color: 'var(--ink-soft)' }}>
          {preview.total_count} people in project · {preview.columns.length} columns · showing {preview.rows.length} example row(s)
        </div>
      )}

      {/* Mapping table */}
      <div style={{ border: '1px solid var(--line)', borderRadius: 10, overflow: 'hidden' }}>
        {/* Header */}
        <div
          style={{
            display: 'grid',
            gridTemplateColumns: '1fr 1fr',
            gap: '0.5rem',
            padding: '0.4rem 0.75rem',
            background: '#f5f4ef',
            borderBottom: '1px solid var(--line)',
          }}
        >
          <span style={{ fontSize: '0.72rem', fontWeight: 700, color: 'var(--ink-soft)' }}>Internal field (DB)</span>
          <span style={{ fontSize: '0.72rem', fontWeight: 700, color: 'var(--ink-soft)' }}>LP API column</span>
        </div>
        {/* Rows */}
        {FIELD_CONFIG.map((field, i) => {
          const selected = apiFieldMap[field.key] || ''
          const isLastRow = i === FIELD_CONFIG.length - 1
          return (
            <div
              key={field.key}
              style={{
                display: 'grid',
                gridTemplateColumns: '1fr 1fr',
                gap: '0.5rem',
                padding: '0.4rem 0.75rem',
                alignItems: 'center',
                borderBottom: isLastRow ? 'none' : '1px solid var(--line)',
                background: selected ? 'var(--mint-soft)' : 'white',
              }}
            >
              <div>
                <span style={{ fontSize: '0.82rem', fontWeight: 600 }}>{field.label}</span>
                <span style={{ fontSize: '0.7rem', color: 'var(--ink-soft)', marginLeft: '0.35rem' }}>
                  default: {field.defaultLpKey}
                </span>
              </div>
              <select
                value={selected}
                onChange={(e) => setMapping(field.key, e.target.value)}
                disabled={!preview}
                style={{
                  fontSize: '0.78rem',
                  padding: '0.3rem 0.4rem',
                  opacity: preview ? 1 : 0.5,
                }}
              >
                <option value="">
                  {preview ? `— use default (${field.defaultLpKey}) —` : '← load columns first'}
                </option>
                {(preview?.columns || []).map((col) => {
                  const example = exampleRow[col]
                  const label = example
                    ? `${col}  (${String(example).slice(0, 45)}${String(example).length > 45 ? '…' : ''})`
                    : col
                  return (
                    <option key={col} value={col}>{label}</option>
                  )
                })}
              </select>
            </div>
          )
        })}
      </div>

      {/* Live parsed preview */}
      {parsedRows.length > 0 && (
        <div style={{ display: 'grid', gap: '0.35rem' }}>
          <div style={{ fontSize: '0.75rem', fontWeight: 700, color: 'var(--ink-soft)' }}>
            Live preview — how LP rows parse into the analysis table:
          </div>
          <div style={{ overflowX: 'auto', border: '1px solid var(--line)', borderRadius: 8 }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.72rem' }}>
              <thead style={{ background: '#f5f4ef' }}>
                <tr>
                  {FIELD_CONFIG.map((f) => (
                    <th
                      key={f.key}
                      style={{
                        padding: '0.3rem 0.55rem',
                        textAlign: 'left',
                        whiteSpace: 'nowrap',
                        borderBottom: '1px solid var(--line)',
                        fontWeight: 600,
                      }}
                    >
                      {f.label}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {parsedRows.map((row, i) => (
                  <tr key={i}>
                    {FIELD_CONFIG.map((f) => (
                      <td
                        key={f.key}
                        style={{
                          padding: '0.3rem 0.55rem',
                          borderTop: '1px solid var(--line)',
                          maxWidth: 180,
                          overflow: 'hidden',
                          textOverflow: 'ellipsis',
                          whiteSpace: 'nowrap',
                          color: row[f.key] ? 'inherit' : 'var(--ink-soft)',
                        }}
                      >
                        {row[f.key] || '—'}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  )
}

// ── Generic key→value mapping rows (push + CSV) ───────────────────────────────

function KeyValueMapping({ rows, onChange, leftLabel, rightLabel, leftPlaceholder, rightOptions }) {
  function setRow(i, key, val) {
    onChange(rows.map((r, idx) => (idx === i ? { ...r, [key]: val } : r)))
  }
  function addRow() { onChange([...rows, { left: '', right: '' }]) }
  function removeRow(i) { onChange(rows.filter((_, idx) => idx !== i)) }

  return (
    <div style={{ display: 'grid', gap: '0.35rem' }}>
      {rows.length > 0 && (
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr auto', gap: '0.35rem' }}>
          <span style={{ fontSize: '0.72rem', color: 'var(--ink-soft)', fontWeight: 700 }}>{leftLabel}</span>
          <span style={{ fontSize: '0.72rem', color: 'var(--ink-soft)', fontWeight: 700 }}>{rightLabel}</span>
          <span />
        </div>
      )}
      {rows.map((row, i) => (
        <div
          key={i}
          style={{ display: 'grid', gridTemplateColumns: '1fr 1fr auto', gap: '0.35rem', alignItems: 'center' }}
        >
          <input
            value={row.left}
            onChange={(e) => setRow(i, 'left', e.target.value)}
            placeholder={leftPlaceholder}
            style={{ fontSize: '0.8rem', padding: '0.35rem 0.5rem' }}
          />
          <select
            value={row.right}
            onChange={(e) => setRow(i, 'right', e.target.value)}
            style={{ fontSize: '0.8rem', padding: '0.35rem 0.5rem' }}
          >
            <option value="">— pick field —</option>
            {rightOptions.map((f) => (
              <option key={f} value={f}>{f}</option>
            ))}
          </select>
          <button
            type="button"
            onClick={() => removeRow(i)}
            style={{
              border: 'none',
              background: 'none',
              color: 'var(--rose)',
              fontSize: '1rem',
              cursor: 'pointer',
              lineHeight: 1,
              padding: '0 0.25rem',
            }}
          >
            ×
          </button>
        </div>
      ))}
      <button
        type="button"
        className="btn"
        style={{ fontSize: '0.75rem', padding: '0.25rem 0.5rem', justifySelf: 'start' }}
        onClick={addRow}
      >
        + Add row
      </button>
    </div>
  )
}

// ── Accordion ─────────────────────────────────────────────────────────────────

function Accordion({ title, children, defaultOpen = false }) {
  const [open, setOpen] = useState(defaultOpen)
  return (
    <div style={{ border: '1px solid var(--line)', borderRadius: 10 }}>
      <button
        type="button"
        onClick={() => setOpen((v) => !v)}
        style={{
          width: '100%',
          textAlign: 'left',
          border: 'none',
          background: open ? 'var(--mint-soft)' : 'transparent',
          padding: '0.55rem 0.75rem',
          fontSize: '0.82rem',
          fontWeight: 700,
          cursor: 'pointer',
          borderRadius: open ? '10px 10px 0 0' : 10,
          display: 'flex',
          justifyContent: 'space-between',
        }}
      >
        {title}
        <span style={{ fontWeight: 400, fontSize: '0.75rem', color: 'var(--ink-soft)' }}>
          {open ? '▲ hide' : '▼ show'}
        </span>
      </button>
      {open && (
        <div
          style={{
            padding: '0.75rem',
            borderTop: '1px solid var(--line)',
            display: 'grid',
            gap: '0.75rem',
          }}
        >
          {children}
        </div>
      )}
    </div>
  )
}

// ── Helpers ───────────────────────────────────────────────────────────────────

function rowsToMap(rows) {
  return Object.fromEntries(
    rows.filter((r) => r.left.trim() && r.right).map((r) => [r.left.trim(), r.right])
  )
}

function mapToRows(map) {
  if (!map || typeof map !== 'object') return []
  return Object.entries(map).map(([left, right]) => ({ left, right }))
}

// ── Main modal ────────────────────────────────────────────────────────────────

export default function PipelineConfigModal({ mode, sourceType, config, onClose, onSaved }) {
  const { refresh } = usePipelineConfigs()

  const existingParams = config?.default_import_params || {}

  const [form, setForm] = useState({
    pipeline_key: config?.pipeline_key ?? '',
    label: config?.label ?? '',
    airtable_table_name: config?.airtable_table_name ?? '',
    lp_project_ids: config?.lp_project_ids ?? [],
    // LP API field mapping: { internal_field: lp_api_column }
    api_field_map: existingParams.api_field_map || {},
    // CSV import mapping rows: [{ left: csv_header, right: internal_field }]
    import_map_rows: mapToRows(existingParams.import_column_map),
    // Push custom fields mapping rows: [{ left: lp_custom_key, right: internal_field }]
    push_map_rows: mapToRows(existingParams.push_column_map),
    // Airtable push mapping rows: [{ left: airtable_column, right: internal_field }]
    airtable_push_map_rows: mapToRows(existingParams.airtable_push_column_map),
  })
  const [saving, setSaving] = useState(false)
  const [deleting, setDeleting] = useState(false)
  const [confirmDelete, setConfirmDelete] = useState(false)
  const [error, setError] = useState('')

  function set(key, val) {
    setForm((prev) => ({ ...prev, [key]: val }))
  }

  async function handleSave() {
    setError('')
    if (!form.label.trim()) { setError('Label is required'); return }
    if (mode === 'create' && !form.pipeline_key.trim()) { setError('Pipeline key is required'); return }

    const lpIds = form.lp_project_ids.length > 0 ? form.lp_project_ids : undefined
    const api_field_map = Object.keys(form.api_field_map).length ? form.api_field_map : undefined
    const import_column_map = rowsToMap(form.import_map_rows)
    const push_column_map = rowsToMap(form.push_map_rows)
    const airtable_push_column_map = rowsToMap(form.airtable_push_map_rows)

    const params = {
      ...(api_field_map ? { api_field_map } : {}),
      ...(Object.keys(import_column_map).length ? { import_column_map } : {}),
      ...(Object.keys(push_column_map).length ? { push_column_map } : {}),
      ...(Object.keys(airtable_push_column_map).length ? { airtable_push_column_map } : {}),
    }
    const default_import_params = Object.keys(params).length ? params : undefined

    setSaving(true)
    try {
      if (mode === 'create') {
        await api.createPipelineConfig({
          source_type: sourceType,
          pipeline_key: form.pipeline_key.trim(),
          label: form.label.trim(),
          airtable_table_name: form.airtable_table_name.trim() || null,
          lp_project_ids: lpIds,
          default_import_params,
        })
      } else {
        await api.updatePipelineConfig(config.id, {
          label: form.label.trim(),
          airtable_table_name: form.airtable_table_name.trim() || null,
          lp_project_ids: lpIds,
          default_import_params,
        })
      }
      await refresh()
      onSaved()
    } catch (err) {
      setError(err.message)
    } finally {
      setSaving(false)
    }
  }

  async function handleDelete() {
    setDeleting(true)
    try {
      await api.deletePipelineConfig(config.id)
      await refresh()
      onSaved()
    } catch (err) {
      setError(err.message)
      setDeleting(false)
    }
  }

  const SOURCE_LABELS = { leadspicker: 'Leadspicker', crunchbase: 'Crunchbase', news: 'News' }
  const isLp = sourceType === 'leadspicker'

  return createPortal(
    <div
      style={{
        position: 'fixed',
        inset: 0,
        zIndex: 9999,
        background: 'rgba(17,34,31,0.35)',
        overflowY: 'auto',
        display: 'flex',
        alignItems: 'flex-start',
        justifyContent: 'center',
        padding: '5rem 1rem 2rem',
      }}
      onMouseDown={onClose}
    >
      <div
        style={{
          background: 'var(--card)',
          border: '1px solid var(--line)',
          borderRadius: 16,
          padding: '1.5rem',
          width: 'min(680px, 96vw)',
          boxShadow: '0 24px 64px rgba(17,34,31,0.18)',
          flexShrink: 0,
        }}
        onMouseDown={(e) => e.stopPropagation()}
      >
        {/* Header */}
        <div
          style={{
            display: 'flex',
            justifyContent: 'space-between',
            alignItems: 'center',
            marginBottom: '1.25rem',
          }}
        >
          <h3 style={{ margin: 0, fontSize: '1rem', fontWeight: 700 }}>
            {mode === 'create'
              ? `New ${SOURCE_LABELS[sourceType]} sub-branch`
              : `Edit: ${config.label}`}
          </h3>
          <button
            className="btn"
            style={{ padding: '0.2rem 0.5rem', fontSize: '0.9rem', lineHeight: 1 }}
            onClick={onClose}
          >
            ✕
          </button>
        </div>

        {/* Basic fields */}
        <div style={{ display: 'grid', gap: '0.85rem', marginBottom: '1rem' }}>
          {mode === 'create' && (
            <label style={{ display: 'grid', gap: '0.3rem', fontSize: '0.8rem', fontWeight: 600 }}>
              <span>
                Pipeline Key{' '}
                <span style={{ fontWeight: 400, color: 'var(--ink-soft)' }}>(snake_case, unique)</span>
              </span>
              <input
                value={form.pipeline_key}
                onChange={(e) => set('pipeline_key', e.target.value)}
                placeholder="e.g. lp_czech_live"
              />
            </label>
          )}

          <label style={{ display: 'grid', gap: '0.3rem', fontSize: '0.8rem', fontWeight: 600 }}>
            Label
            <input
              value={form.label}
              onChange={(e) => set('label', e.target.value)}
              placeholder="e.g. Leadspicker Czech Live"
            />
          </label>

          <label style={{ display: 'grid', gap: '0.3rem', fontSize: '0.8rem', fontWeight: 600 }}>
            <span>
              Airtable Table Name{' '}
              <span style={{ fontWeight: 400, color: 'var(--ink-soft)' }}>(optional)</span>
            </span>
            <input
              value={form.airtable_table_name}
              onChange={(e) => set('airtable_table_name', e.target.value)}
              placeholder="e.g. Leadspicker - Czech"
            />
          </label>

          {isLp && (
            <div style={{ display: 'grid', gap: '0.3rem', fontSize: '0.8rem', fontWeight: 600 }}>
              <span>
                LP Project IDs{' '}
                {form.lp_project_ids.length > 0 && (
                  <span style={{ fontWeight: 400, color: 'var(--ink-soft)' }}>
                    — selected: {form.lp_project_ids.join(', ')}
                  </span>
                )}
              </span>
              <LpProjectPicker
                selectedIds={form.lp_project_ids}
                onChange={(ids) => set('lp_project_ids', ids)}
              />
            </div>
          )}
        </div>

        {/* Mapping accordions (LP only) */}
        {isLp && (
          <div style={{ display: 'grid', gap: '0.5rem', marginBottom: '1rem' }}>
            {/* API field mapping */}
            <Accordion title="LP API Column Mapping" defaultOpen={Object.keys(form.api_field_map).length > 0}>
              <p style={{ margin: 0, fontSize: '0.78rem', color: 'var(--ink-soft)' }}>
                Map LP API contact_data columns to internal fields. Load columns from a project to see
                available keys with example values. Leave a field at default to use the built-in fallback chain.
              </p>
              <ApiFieldMapping
                projectIds={form.lp_project_ids}
                apiFieldMap={form.api_field_map}
                onChange={(map) => set('api_field_map', map)}
              />
            </Accordion>

            {/* Push custom fields */}
            <Accordion title="Push Custom Fields Mapping" defaultOpen={form.push_map_rows.length > 0}>
              <p style={{ margin: 0, fontSize: '0.78rem', color: 'var(--ink-soft)' }}>
                Map LP custom field keys ← internal fields when pushing entries to LP. Overrides the
                default <code style={{ background: '#f0efea', borderRadius: 4, padding: '0 3px' }}>base_post_url / message_text</code> behaviour.
              </p>
              <KeyValueMapping
                rows={form.push_map_rows}
                onChange={(rows) => set('push_map_rows', rows)}
                leftLabel="LP custom field key"
                rightLabel="Internal field"
                leftPlaceholder="e.g. base_post_url"
                rightOptions={PUSH_SOURCE_FIELDS}
              />
            </Accordion>

            {/* Airtable push column mapping */}
            <Accordion title="Airtable Push Column Mapping" defaultOpen={form.airtable_push_map_rows.length > 0}>
              <p style={{ margin: 0, fontSize: '0.78rem', color: 'var(--ink-soft)' }}>
                Map Airtable column names ← internal fields when pushing entries to Airtable.
                The message field includes the drafted message text.
              </p>
              <KeyValueMapping
                rows={form.airtable_push_map_rows}
                onChange={(rows) => set('airtable_push_map_rows', rows)}
                leftLabel="Airtable column name"
                rightLabel="Internal field"
                leftPlaceholder="e.g. General message"
                rightOptions={PUSH_SOURCE_FIELDS}
              />
            </Accordion>

            {/* CSV import mapping */}
            <Accordion title="CSV Import Column Mapping" defaultOpen={form.import_map_rows.length > 0}>
              <p style={{ margin: 0, fontSize: '0.78rem', color: 'var(--ink-soft)' }}>
                Map custom CSV column headers → internal fields. Overrides the built-in header aliases
                for CSV uploads of this sub-branch.
              </p>
              <KeyValueMapping
                rows={form.import_map_rows}
                onChange={(rows) => set('import_map_rows', rows)}
                leftLabel="CSV column header"
                rightLabel="Internal field"
                leftPlaceholder="e.g. Post URL"
                rightOptions={INTERNAL_FIELDS}
              />
            </Accordion>
          </div>
        )}

        {error && (
          <p style={{ margin: '0 0 0.75rem', color: 'var(--rose)', fontSize: '0.82rem', fontWeight: 600 }}>
            {error}
          </p>
        )}

        {/* Action buttons */}
        <div style={{ display: 'flex', gap: '0.5rem', justifyContent: 'flex-end' }}>
          {mode === 'edit' && !confirmDelete && (
            <button
              className="btn danger"
              style={{ marginRight: 'auto' }}
              onClick={() => setConfirmDelete(true)}
            >
              Delete
            </button>
          )}
          {mode === 'edit' && confirmDelete && (
            <>
              <button
                className="btn danger"
                style={{ marginRight: 'auto' }}
                disabled={deleting}
                onClick={handleDelete}
              >
                {deleting ? 'Deleting…' : 'Confirm delete'}
              </button>
              <button className="btn" onClick={() => setConfirmDelete(false)}>Cancel</button>
            </>
          )}
          {!confirmDelete && (
            <>
              <button className="btn" onClick={onClose}>Cancel</button>
              <button className="btn primary" disabled={saving} onClick={handleSave}>
                {saving ? 'Saving…' : mode === 'create' ? 'Create' : 'Save'}
              </button>
            </>
          )}
        </div>
      </div>
    </div>,
    document.body
  )
}
