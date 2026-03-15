import { useCallback, useEffect, useMemo, useState } from 'react'
import { Link, useParams } from 'react-router-dom'
import { api } from '../lib/api'
import { highlightText } from '../lib/keywords'
import MessageBox from '../components/MessageBox'
import ProgressBar from '../components/ProgressBar'
import CrunchbaseDiscover from '../components/CrunchbaseDiscover'

function normalizeChatState(rawState) {
  if (!Array.isArray(rawState)) {
    return []
  }
  return rawState
    .filter((msg) => msg && typeof msg === 'object')
    .map((msg) => ({
      role: String(msg.role || '').trim().toLowerCase(),
      content: String(msg.content || '').trim(),
    }))
    .filter((msg) => ['user', 'assistant'].includes(msg.role) && msg.content)
}

function isLinkedinLeadspickerPipeline(pipelineType) {
  return ['lp_general', 'lp_czech', 'linkedin', 'leadspicker'].includes(String(pipelineType || ''))
}

export default function DraftingView() {
  const { pipelineKey } = useParams()

  const [loading, setLoading] = useState(true)
  const [entries, setEntries] = useState([])
  const [currentIndex, setCurrentIndex] = useState(0)
  const [pipelineType, setPipelineType] = useState('')
  const [draftText, setDraftText] = useState('')
  const [lastSavedText, setLastSavedText] = useState('')

  const [message, setMessage] = useState({ kind: 'info', text: '' })
  const [busy, setBusy] = useState(false)

  const [editTableOpen, setEditTableOpen] = useState(false)
  const [removeMap, setRemoveMap] = useState({})
  const [chatOpen, setChatOpen] = useState(false)
  const [chatMessages, setChatMessages] = useState([])
  const [chatInput, setChatInput] = useState('')
  const [chatBusy, setChatBusy] = useState(false)
  const [isContacted, setIsContacted] = useState(false)

  useEffect(() => {
    let alive = true

    async function load() {
      setLoading(true)
      try {
        const data = await api.getPipelineDraftEntries(pipelineKey)
        if (!alive) {
          return
        }
        setEntries(data)
        setCurrentIndex(0)
        if (data.length > 0) setPipelineType(data[0].pipeline_type || '')
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

    load()
    return () => {
      alive = false
    }
  }, [pipelineKey])

  const current = entries[currentIndex] || null
  const currentMessage = current?.message || null
  const currentSignal = current?.signals || {}
  const currentContact = current?.contacts || {}
  const currentCompany = currentSignal?.companies || {}
  const isLinkedinLeadspicker = isLinkedinLeadspickerPipeline(current?.pipeline_type)

  useEffect(() => {
    if (!currentMessage) {
      setDraftText('')
      setLastSavedText('')
      return
    }
    const initial = currentMessage.draft_text || currentMessage.final_text || ''
    setDraftText(initial)
    setLastSavedText(initial)
  }, [currentIndex, currentMessage])

  useEffect(() => {
    setChatMessages(normalizeChatState(current?.ai_chat_state))
    setChatInput('')
  }, [current?.id, current?.ai_chat_state])

  useEffect(() => {
    if (!current) {
      setIsContacted(false)
      return
    }
    const co = current?.signals?.companies || {}
    api.checkContacted({
      company_name: co.name_raw || null,
      company_website: co.website || co.domain_normalized || null,
      company_linkedin: co.linkedin_url || null,
    }).then((res) => setIsContacted(res?.is_contacted || false)).catch(() => setIsContacted(false))
  }, [current?.id])

  const saveCurrentDraft = useCallback(async () => {
    if (!currentMessage) {
      return true
    }
    if (draftText === lastSavedText) {
      return true
    }
    if (!draftText.trim()) {
      return true
    }

    try {
      await api.saveDraft({ messageId: currentMessage.id, text: draftText })
      setEntries((prev) => {
        const copy = [...prev]
        if (copy[currentIndex]?.message) {
          copy[currentIndex] = {
            ...copy[currentIndex],
            message: {
              ...copy[currentIndex].message,
              draft_text: draftText,
            },
          }
        }
        return copy
      })
      setLastSavedText(draftText)
      return true
    } catch (err) {
      setMessage({ kind: 'error', text: `Auto-save failed: ${String(err.message || err)}` })
      return false
    }
  }, [currentMessage, draftText, lastSavedText, currentIndex])

  const clearChatStateForEntry = useCallback(async (entryId) => {
    if (!entryId) {
      return true
    }
    try {
      await api.clearAiChat(entryId)
      setEntries((prev) =>
        prev.map((row) => (row.id === entryId ? { ...row, ai_chat_state: null } : row)),
      )
      return true
    } catch (err) {
      setMessage({
        kind: 'warn',
        text: `Could not clear AI chat state: ${String(err.message || err)} (non-blocking).`,
      })
      return false
    }
  }, [])

  const navigate = useCallback(async (delta) => {
    const target = Math.max(0, Math.min(entries.length, currentIndex + delta))
    if (target === currentIndex) {
      return
    }

    setBusy(true)
    const ok = await saveCurrentDraft()
    let chatCleared = true
    if (ok && current?.id) {
      chatCleared = await clearChatStateForEntry(current.id)
    }
    if (ok) {
      setCurrentIndex(target)
      setMessage({
        kind: chatCleared ? 'info' : 'warn',
        text: chatCleared
          ? 'Draft auto-saved on navigation.'
          : 'Draft auto-saved on navigation, but chat state clear failed (non-blocking).',
      })
    }
    setBusy(false)
  }, [entries.length, currentIndex, saveCurrentDraft, current?.id, clearChatStateForEntry])

  useEffect(() => {
    function onKey(e) {
      const tag = document.activeElement?.tagName?.toLowerCase()
      const inInput = tag === 'input' || tag === 'textarea' || tag === 'select'
      if (inInput) {
        return
      }

      if (e.key === 'ArrowRight') {
        e.preventDefault()
        navigate(1)
      }
      if (e.key === 'ArrowLeft') {
        e.preventDefault()
        navigate(-1)
      }
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [navigate])

  const removeIds = useMemo(
    () => Object.entries(removeMap).filter(([, v]) => v).map(([id]) => id),
    [removeMap],
  )

  const draftingProgress = useMemo(() => {
    const total = entries.length
    const withDraft = entries.filter((e) => (e.message?.draft_text || '').trim().length > 0).length
    return { total, withDraft }
  }, [entries])

  async function applyBatchRemoval() {
    if (removeIds.length === 0) {
      setMessage({ kind: 'warn', text: 'No rows selected for removal.' })
      return
    }

    setBusy(true)
    try {
      const res = await api.removeDraftingEntries(removeIds)
      setEntries((prev) => prev.filter((e) => !removeMap[e.id]))
      setRemoveMap({})
      setCurrentIndex(0)
      setMessage({ kind: 'info', text: `Removed ${res.removed} entries from drafting.` })
    } catch (err) {
      setMessage({ kind: 'error', text: String(err.message || err) })
    } finally {
      setBusy(false)
    }
  }

  async function finishDrafting() {
    setBusy(true)
    try {
      await saveCurrentDraft()
      const res = await api.finishPipelineDrafting(pipelineKey)
      setMessage({
        kind: 'info',
        text: `Finish Drafting done. Finalized ${res.finalized}/${res.total_drafted} (skipped ${res.skipped_empty}).`,
      })
    } catch (err) {
      setMessage({ kind: 'error', text: String(err.message || err) })
    } finally {
      setBusy(false)
    }
  }

  async function sendChatMessage() {
    if (!current) {
      return
    }
    const userText = chatInput.trim()
    if (!userText) {
      return
    }

    const optimistic = [...chatMessages, { role: 'user', content: userText }]
    setChatMessages(optimistic)
    setChatInput('')
    setChatBusy(true)

    try {
      const res = await api.aiChat({
        entryId: current.id,
        userMessage: userText,
      })

      const persisted = normalizeChatState(res.ai_chat_state)
      const nextMessages =
        persisted.length > 0
          ? persisted
          : [
              ...optimistic,
              {
                role: 'assistant',
                content: String(res.assistant_message || '').trim() || 'No assistant response.',
              },
            ]

      setChatMessages(nextMessages)
      setEntries((prev) =>
        prev.map((row) =>
          row.id === current.id
            ? {
                ...row,
                ai_chat_state: nextMessages,
              }
            : row,
        ),
      )
      if (res.degraded) {
        setMessage({
          kind: 'warn',
          text: `AI assistant degraded: ${res.error || 'Gemini unavailable. Continue drafting manually.'}`,
        })
      }
    } catch (err) {
      setMessage({
        kind: 'warn',
        text: `AI assistant unavailable: ${String(err.message || err)}. Continue drafting manually.`,
      })
    } finally {
      setChatBusy(false)
    }
  }

  function applyToDraft(text) {
    const snippet = String(text || '').trim()
    if (!snippet) {
      return
    }
    setDraftText((prev) => {
      const base = prev.trim()
      if (!base) {
        return snippet
      }
      return `${base}\n\n${snippet}`
    })
    setMessage({ kind: 'info', text: 'Assistant response applied to draft.' })
  }

  if (loading) {
    return <MessageBox kind="info" text="Loading drafting entries..." />
  }

  if (!current && entries.length === 0) {
    return (
      <section className="panel" style={{ display: 'grid', gap: '0.6rem' }}>
        <h1 style={{ margin: 0 }}>Drafting · {pipelineKey}</h1>
        <MessageBox kind="warn" text="No relevant entries for drafting." />
        <Link className="btn" to={`/pipeline/${pipelineKey}`}>Back to Pipeline</Link>
      </section>
    )
  }

  if (!current) {
    return (
      <section className="panel" style={{ display: 'grid', gap: '0.6rem' }}>
        <h1 style={{ margin: 0 }}>Drafting · {pipelineKey}</h1>
        <MessageBox kind="info" text="You've reached the end — all entries reviewed." />
        <div style={{ display: 'flex', gap: '0.5rem', flexWrap: 'wrap' }}>
          <button className="btn" onClick={() => setCurrentIndex(entries.length - 1)}>← Back to last entry</button>
          <button className="btn warn" disabled={busy} onClick={finishDrafting}>Finish Drafting</button>
          <Link className="btn" to={`/pipeline/${pipelineKey}`}>Back to Pipeline</Link>
        </div>
      </section>
    )
  }

  return (
    <>
      <section className="panel" style={{ display: 'grid', gap: '0.65rem' }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: '0.6rem', flexWrap: 'wrap' }}>
          <div>
            <h1 style={{ margin: 0, fontSize: '1.25rem' }}>Drafting · {pipelineKey}</h1>
            <div style={{ fontSize: '0.76rem', color: 'var(--ink-soft)' }}>
              Auto-save happens on navigation writes only.
            </div>
          </div>

          <div style={{ display: 'flex', gap: '0.45rem', flexWrap: 'wrap' }}>
            <Link className="btn" to={`/pipeline/${pipelineKey}`}>
              Back to Pipeline
            </Link>
            <button className="btn" onClick={() => setEditTableOpen((v) => !v)}>
              {editTableOpen ? 'Close Edit Entries' : 'Edit Entries'}
            </button>
            <button className="btn warn" disabled={busy} onClick={finishDrafting}>
              Finish Drafting
            </button>
          </div>
        </div>

        <MessageBox kind={message.kind} text={message.text} />

        <div style={{ display: 'grid', gap: '0.4rem' }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: '0.5rem' }}>
            <div style={{ fontSize: '0.78rem', color: 'var(--ink-soft)' }}>
              Entry {currentIndex + 1}/{entries.length} · Drafted {draftingProgress.withDraft}/{draftingProgress.total}
            </div>
            <div style={{ display: 'flex', gap: '0.38rem' }}>
              <button className="btn" disabled={busy || currentIndex === 0} onClick={() => navigate(-1)}>← Prev</button>
              <button className="btn primary" disabled={busy} onClick={() => navigate(1)}>Next →</button>
            </div>
          </div>
          <ProgressBar value={draftingProgress.withDraft} total={Math.max(draftingProgress.total, 1)} />
        </div>
      </section>

      {editTableOpen ? (
        <section className="panel" style={{ display: 'grid', gap: '0.55rem' }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: '0.5rem', flexWrap: 'wrap' }}>
            <strong style={{ fontSize: '0.92rem' }}>Edit Entries</strong>
            <button className="btn danger" disabled={busy || removeIds.length === 0} onClick={applyBatchRemoval}>
              Remove Selected ({removeIds.length})
            </button>
          </div>

          <div style={{ maxHeight: 260, overflow: 'auto', border: '1px solid var(--line)', borderRadius: 10 }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.8rem' }}>
              <thead style={{ position: 'sticky', top: 0, background: '#f8f6ef' }}>
                <tr>
                  <th style={{ textAlign: 'left', padding: '0.4rem 0.5rem' }}>Remove</th>
                  <th style={{ textAlign: 'left', padding: '0.4rem 0.5rem' }}>Name</th>
                  <th style={{ textAlign: 'left', padding: '0.4rem 0.5rem' }}>Company</th>
                  <th style={{ textAlign: 'left', padding: '0.4rem 0.5rem' }}>Has Draft</th>
                </tr>
              </thead>
              <tbody>
                {entries.map((row) => (
                  <tr key={row.id}>
                    <td style={{ padding: '0.3rem 0.5rem' }}>
                      <input
                        type="checkbox"
                        checked={Boolean(removeMap[row.id])}
                        onChange={(e) => setRemoveMap((m) => ({ ...m, [row.id]: e.target.checked }))}
                      />
                    </td>
                    <td style={{ padding: '0.3rem 0.5rem' }}>{row.contacts?.full_name || 'Unknown'}</td>
                    <td style={{ padding: '0.3rem 0.5rem' }}>{row.signals?.companies?.name_raw || 'Unknown'}</td>
                    <td style={{ padding: '0.3rem 0.5rem' }}>
                      {(row.message?.draft_text || '').trim() ? 'yes' : 'no'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      ) : null}

      <section className="grid-2" style={{ alignItems: 'start' }}>
        <article className="panel" style={{ display: 'grid', gap: '0.65rem' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', flexWrap: 'wrap' }}>
            <h2 style={{ margin: 0, fontSize: '1rem' }}>Context</h2>
            {isContacted ? (
              <span className="badge" style={{ background: '#e53e3e', color: '#fff', fontWeight: 700 }}>
                ALREADY CONTACTED
              </span>
            ) : null}
          </div>

          <div style={{ fontSize: '0.84rem', display: 'grid', gap: '0.3rem' }}>
            <div>
              <strong>Author:</strong> {currentContact.full_name || `${currentContact.first_name || ''} ${currentContact.last_name || ''}`.trim() || 'Unknown'}
            </div>
            <div>
              <strong>Company:</strong> {currentCompany.name_raw || 'Unknown'}
            </div>
            <div>
              <strong>Position:</strong> {currentContact.relation_to_company || 'n/a'}
            </div>
            <div>
              <strong>AI classifier:</strong> {currentSignal.ai_classifier || 'n/a'}
            </div>
            <div>
              <strong>Signal URL:</strong>{' '}
              {currentSignal.content_url ? (
                <a href={currentSignal.content_url} target="_blank" rel="noreferrer" style={{ textDecoration: 'underline' }}>
                  open
                </a>
              ) : (
                'n/a'
              )}
            </div>
          </div>

          {isLinkedinLeadspicker ? (
            <div style={{ display: 'flex', gap: '0.45rem', flexWrap: 'wrap' }}>
              {currentSignal.content_url ? (
                <a className="btn" href={currentSignal.content_url} target="_blank" rel="noreferrer">
                  Open LinkedIn post
                </a>
              ) : (
                <button className="btn" disabled>
                  Open LinkedIn post
                </button>
              )}
            </div>
          ) : null}

          <div className="panel" style={{ margin: 0, padding: '0.7rem', background: '#fff' }}>
            <div style={{ fontSize: '0.75rem', color: 'var(--ink-soft)' }}>Summary</div>
            <div style={{ whiteSpace: 'pre-wrap', fontSize: '0.82rem' }}>{currentSignal.content_summary || 'No summary'}</div>
          </div>

          <div className="panel" style={{ margin: 0, padding: '0.7rem', background: '#fff', display: 'grid', gap: '0.4rem' }}>
            <div style={{ fontSize: '0.75rem', color: 'var(--ink-soft)' }}>Crunchbase Discover</div>
            <CrunchbaseDiscover
              withInput
              companyName={currentCompany.name_raw || ''}
              buttonLabel="Open on Crunchbase"
            />
          </div>

          <div style={{ display: 'flex', gap: '0.4rem', flexWrap: 'wrap' }}>
            <span className={`badge ${current.is_post_author ? 'green' : 'gray'}`}>
              {current.is_post_author ? 'Author matches post' : 'Author mismatch'}
            </span>
            <span className={`badge ${current.is_from_company ? 'green' : 'gray'}`}>
              {current.is_from_company ? 'From company' : 'Outsider'}
            </span>
          </div>

          <div className="panel" style={{ margin: 0, padding: '0.7rem', background: '#fff' }}>
            <div style={{ fontSize: '0.75rem', color: 'var(--ink-soft)' }}>Post excerpt</div>
            <div style={{ whiteSpace: 'pre-wrap', fontSize: '0.82rem', maxHeight: 320, overflowY: 'auto' }}>
              {highlightText(currentSignal.content_text || '').map((part, idx) =>
                part.hit ? <mark className="marked-hit" key={idx}>{part.value}</mark> : <span key={idx}>{part.value}</span>
              )}
            </div>
          </div>
        </article>

        <article className="panel" style={{ display: 'grid', gap: '0.65rem' }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <h2 style={{ margin: 0, fontSize: '1rem' }}>Editor</h2>
            <div style={{ fontSize: '0.75rem', color: 'var(--ink-soft)' }}>{draftText.length} chars</div>
          </div>

          <textarea
            value={draftText}
            onChange={(e) => setDraftText(e.target.value)}
            placeholder="Draft message..."
            style={{ minHeight: 210 }}
          />

          <div style={{ display: 'flex', gap: '0.45rem', flexWrap: 'wrap' }}>
            <button className="btn" disabled={busy || currentIndex === 0} onClick={() => navigate(-1)}>
              Back (auto-save)
            </button>
            <button className="btn primary" disabled={busy} onClick={() => navigate(1)}>
              Next (auto-save)
            </button>
          </div>

          <section className="panel" style={{ margin: 0, padding: '0.7rem', background: '#fff', display: 'grid', gap: '0.45rem' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
              <strong style={{ fontSize: '0.84rem' }}>AI Draft Assistant</strong>
              <button className="btn" onClick={() => setChatOpen((v) => !v)}>
                {chatOpen ? 'Hide' : 'Show'}
              </button>
            </div>
            {chatOpen ? (
              <>
                <div
                  style={{
                    border: '1px solid var(--line)',
                    borderRadius: 10,
                    padding: '0.55rem',
                    maxHeight: 230,
                    overflow: 'auto',
                    display: 'grid',
                    gap: '0.45rem',
                    background: '#fffaf0',
                  }}
                >
                  {chatMessages.length === 0 ? (
                    <div style={{ fontSize: '0.8rem', color: 'var(--ink-soft)' }}>
                      No messages yet. Ask for tone, structure, or rewrite help.
                    </div>
                  ) : (
                    chatMessages.map((msg, idx) => (
                      <div
                        key={`${msg.role}-${idx}`}
                        style={{
                          border: '1px solid #ece6d5',
                          borderRadius: 8,
                          padding: '0.45rem 0.55rem',
                          background: msg.role === 'user' ? '#f6f0dd' : '#ffffff',
                          display: 'grid',
                          gap: '0.36rem',
                        }}
                      >
                        <div style={{ fontSize: '0.7rem', color: 'var(--ink-soft)', textTransform: 'uppercase' }}>{msg.role}</div>
                        <div style={{ whiteSpace: 'pre-wrap', fontSize: '0.81rem' }}>{msg.content}</div>
                        {msg.role === 'assistant' ? (
                          <div>
                            <button className="btn" onClick={() => applyToDraft(msg.content)}>
                              Apply to draft
                            </button>
                          </div>
                        ) : null}
                      </div>
                    ))
                  )}
                </div>

                <textarea
                  placeholder="Ask the assistant..."
                  value={chatInput}
                  onChange={(e) => setChatInput(e.target.value)}
                  style={{ minHeight: 78 }}
                  onKeyDown={(e) => {
                    if (e.key === 'Enter' && !e.shiftKey) {
                      e.preventDefault()
                      sendChatMessage()
                    }
                  }}
                />

                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: '0.4rem', flexWrap: 'wrap' }}>
                  <div style={{ fontSize: '0.75rem', color: 'var(--ink-soft)' }}>
                    If AI is down, drafting still works manually.
                  </div>
                  <button className="btn primary" disabled={chatBusy || !chatInput.trim()} onClick={sendChatMessage}>
                    {chatBusy ? 'Sending...' : 'Send'}
                  </button>
                </div>
              </>
            ) : null}
          </section>
        </article>
      </section>
    </>
  )
}
