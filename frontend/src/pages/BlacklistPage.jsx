import { useState, useEffect } from 'react'
import { api } from '../lib/api'
import MessageBox from '../components/MessageBox'

export default function BlacklistPage() {
  const [list, setList] = useState([])
  const [loading, setLoading] = useState(true)
  const [message, setMessage] = useState(null)
  const [busy, setBusy] = useState('')
  const [search, setSearch] = useState('')

  // CSV state
  const [csvFile, setCsvFile] = useState(null)
  const [csvHeaders, setCsvHeaders] = useState([])
  const [colMap, setColMap] = useState({ company_name: '', company_linkedin: '', company_website: '', reason: '', added_by: '', origin: '' })
  const [bulkReason, setBulkReason] = useState('')
  const [bulkAddedBy, setBulkAddedBy] = useState('')
  const [bulkOrigin, setBulkOrigin] = useState('')

  function notify(kind, text) {
    setMessage({ kind, text })
    setTimeout(() => setMessage(null), 6000)
  }

  async function loadList() {
    setLoading(true)
    try {
      const data = await api.getBlacklist()
      setList(data)
    } catch (err) { notify('error', err.message) }
    finally { setLoading(false) }
  }

  useEffect(() => { loadList() }, [])

  // CSV file handling
  function handleFile(file) {
    setCsvFile(file)
    if (!file) { setCsvHeaders([]); return }
    const reader = new FileReader()
    reader.onload = (e) => {
      const firstLine = e.target.result.split('\n')[0] || ''
      const delim = firstLine.includes(';') ? ';' : firstLine.includes('\t') ? '\t' : ','
      const headers = firstLine.split(delim).map(h => h.trim().replace(/^["']|["']$/g, ''))
      setCsvHeaders(headers)
      setColMap({ company_name: '', company_linkedin: '', company_website: '', reason: '', added_by: '', origin: '' })
    }
    reader.readAsText(file)
  }

  async function uploadCsv() {
    if (!csvFile) return
    setBusy('upload')
    try {
      const columnMap = {}
      for (const [target, csvCol] of Object.entries(colMap)) {
        if (csvCol) columnMap[csvCol] = target
      }
      const r = await api.uploadBlacklistCsv({
        file: csvFile,
        columnMap: Object.keys(columnMap).length ? columnMap : null,
        defaultReason: bulkReason.trim() || null,
        defaultAddedBy: bulkAddedBy.trim() || null,
        defaultContactedVia: bulkOrigin.trim() || null,
      })
      notify('info', `Imported ${r.imported}, skipped ${r.skipped_duplicates} duplicates (${r.total_in_file} in file).`)
      setCsvFile(null)
      setCsvHeaders([])
      loadList()
    } catch (err) { notify('error', err.message) }
    finally { setBusy('') }
  }

  async function deleteEntry(id) {
    try {
      await api.removeFromBlacklist(id)
      setList(prev => prev.filter(e => e.id !== id))
    } catch (err) { notify('error', err.message) }
  }

  const filtered = search
    ? list.filter(e => e.company_name.toLowerCase().includes(search.toLowerCase()))
    : list

  const lbl = { fontSize: '0.78rem', fontWeight: 600, display: 'grid', gap: '0.3rem' }

  return (
    <div style={{ padding: '1.5rem', maxWidth: 900, margin: '0 auto' }}>
      <h2 style={{ margin: '0 0 0.3rem', fontSize: '1.2rem', fontWeight: 700 }}>Blacklisted Companies</h2>
      <p style={{ margin: '0 0 1rem', fontSize: '0.8rem', color: 'var(--ink-soft)' }}>
        Global blacklist — applies across all pipelines. {list.length} companies blacklisted.
      </p>

      {message && <div style={{ marginBottom: '0.75rem' }}><MessageBox kind={message.kind} text={message.text} /></div>}

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '1rem' }}>

        {/* CSV Import */}
        <div style={{
          background: 'var(--card)', border: '1px solid var(--line)', borderRadius: 14,
          padding: '1.1rem 1.25rem', display: 'grid', gap: '0.75rem',
        }}>
          <div style={{ fontSize: '0.75rem', fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.06em', color: 'var(--ink-soft)' }}>
            Import from CSV
          </div>

          <input type="file" accept=".csv" style={{ fontSize: '0.78rem' }}
            onChange={e => handleFile(e.target.files[0] || null)} />

          {csvHeaders.length > 0 && (
            <div style={{ display: 'grid', gap: '0.35rem' }}>
              <span style={{ fontWeight: 600, fontSize: '0.73rem', color: 'var(--ink-soft)' }}>Column Mapping</span>
              {['company_name', 'company_linkedin', 'company_website', 'reason', 'added_by', 'origin'].map(field => (
                <label key={field} style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', fontSize: '0.78rem' }}>
                  <span style={{ minWidth: 110, fontWeight: 600 }}>{field}</span>
                  <select
                    value={colMap[field]}
                    onChange={e => setColMap(prev => ({ ...prev, [field]: e.target.value }))}
                    style={{ flex: 1, fontSize: '0.78rem' }}
                  >
                    <option value="">-- skip --</option>
                    {csvHeaders.map(h => <option key={h} value={h}>{h}</option>)}
                  </select>
                </label>
              ))}
            </div>
          )}

          <div style={{ display: 'grid', gap: '0.35rem' }}>
            <span style={{ fontWeight: 600, fontSize: '0.73rem', color: 'var(--ink-soft)' }}>Bulk Fill (applied to all rows)</span>
            <label style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', fontSize: '0.78rem' }}>
              <span style={{ minWidth: 70, fontWeight: 600 }}>Reason</span>
              <input value={bulkReason} onChange={e => setBulkReason(e.target.value)} placeholder="e.g. Competitor" style={{ flex: 1, fontSize: '0.78rem' }} />
            </label>
            <label style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', fontSize: '0.78rem' }}>
              <span style={{ minWidth: 70, fontWeight: 600 }}>Added By</span>
              <input value={bulkAddedBy} onChange={e => setBulkAddedBy(e.target.value)} placeholder="e.g. Tom" style={{ flex: 1, fontSize: '0.78rem' }} />
            </label>
            <label style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', fontSize: '0.78rem' }}>
              <span style={{ minWidth: 70, fontWeight: 600 }}>Origin</span>
              <input value={bulkOrigin} onChange={e => setBulkOrigin(e.target.value)} placeholder="e.g. leadspicker" style={{ flex: 1, fontSize: '0.78rem' }} />
            </label>
          </div>

          <button className="btn primary" disabled={!csvFile || !colMap.company_name || busy === 'upload'}
            onClick={uploadCsv}>
            {busy === 'upload' ? 'Uploading...' : 'Upload Blacklist CSV'}
          </button>
        </div>

        {/* Manual add */}
        <div style={{
          background: 'var(--card)', border: '1px solid var(--line)', borderRadius: 14,
          padding: '1.1rem 1.25rem', display: 'grid', gap: '0.75rem', alignContent: 'start',
        }}>
          <div style={{ fontSize: '0.75rem', fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.06em', color: 'var(--ink-soft)' }}>
            Add Manually
          </div>
          <form onSubmit={async (e) => {
            e.preventDefault()
            const fd = new FormData(e.target)
            const name = fd.get('name')?.trim()
            if (!name) return
            setBusy('add')
            try {
              const entry = await api.addToBlacklist({
                company_name: name,
                company_linkedin: fd.get('company_linkedin')?.trim() || null,
                company_website: fd.get('company_website')?.trim() || null,
                reason: fd.get('reason')?.trim() || null,
                added_by: fd.get('added_by')?.trim() || null,
                origin: fd.get('origin')?.trim() || null,
                pipeline_entry_id: 'manual',
              })
              setList(prev => [entry, ...prev])
              e.target.reset()
              notify('info', `Added "${name}" to blacklist.`)
            } catch (err) { notify('error', err.message) }
            finally { setBusy('') }
          }} style={{ display: 'grid', gap: '0.4rem' }}>
            <label style={lbl}>Company Name <input name="name" required style={{ fontSize: '0.82rem' }} /></label>
            <label style={lbl}>LinkedIn URL <input name="company_linkedin" style={{ fontSize: '0.82rem' }} placeholder="optional" /></label>
            <label style={lbl}>Website <input name="company_website" style={{ fontSize: '0.82rem' }} placeholder="optional" /></label>
            <label style={lbl}>Reason <input name="reason" style={{ fontSize: '0.82rem' }} placeholder="optional" /></label>
            <label style={lbl}>Added By <input name="added_by" style={{ fontSize: '0.82rem' }} placeholder="optional" /></label>
            <label style={lbl}>Origin <input name="origin" style={{ fontSize: '0.82rem' }} placeholder="optional" /></label>
            <button className="btn" type="submit" disabled={busy === 'add'}>
              {busy === 'add' ? 'Adding...' : 'Add to Blacklist'}
            </button>
          </form>
        </div>
      </div>

      {/* Company list */}
      <div style={{
        marginTop: '1rem', background: 'var(--card)', border: '1px solid var(--line)',
        borderRadius: 14, padding: '1.1rem 1.25rem', display: 'grid', gap: '0.5rem',
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
          <span style={{ fontSize: '0.75rem', fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.06em', color: 'var(--ink-soft)' }}>
            Blacklisted Companies ({filtered.length})
          </span>
          <input
            type="text" placeholder="Search..."
            value={search} onChange={e => setSearch(e.target.value)}
            style={{ fontSize: '0.78rem', padding: '0.25rem 0.5rem', flex: 1, maxWidth: 300 }}
          />
        </div>

        {loading ? (
          <span style={{ fontSize: '0.8rem', color: 'var(--ink-soft)' }}>Loading...</span>
        ) : filtered.length === 0 ? (
          <span style={{ fontSize: '0.8rem', color: 'var(--ink-soft)' }}>
            {search ? 'No matches.' : 'No blacklisted companies yet.'}
          </span>
        ) : (
          <div style={{ maxHeight: 400, overflowY: 'auto', border: '1px solid var(--line)', borderRadius: 8 }}>
            {filtered.map(e => (
              <div key={e.id} style={{
                display: 'flex', alignItems: 'center', padding: '0.4rem 0.65rem',
                fontSize: '0.78rem', borderBottom: '1px solid var(--line)', gap: '0.5rem',
              }}>
                <span style={{ flex: 1, fontWeight: 600 }}>{e.company_name}</span>
                {e.linkedin_slug && <span style={{ color: 'var(--ink-soft)', fontSize: '0.7rem' }}>/{e.linkedin_slug}</span>}
                {e.root_domain && <span style={{ color: 'var(--ink-soft)', fontSize: '0.7rem' }}>{e.root_domain}</span>}
                {e.reason && <span style={{ color: 'var(--ink-soft)', fontSize: '0.72rem' }}>{e.reason}</span>}
                {e.added_by && <span style={{ color: 'var(--ink-soft)', fontSize: '0.7rem' }}>by {e.added_by}</span>}
                <button
                  onClick={() => deleteEntry(e.id)}
                  title="Remove from blacklist"
                  style={{ border: 'none', background: 'none', color: 'var(--rose)', cursor: 'pointer', fontSize: '0.82rem', padding: '0.1rem 0.3rem' }}
                >x</button>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  )
}
