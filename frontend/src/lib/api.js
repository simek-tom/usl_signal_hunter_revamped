const API_BASE = '/api'

async function request(path, { method = 'GET', body, isForm = false } = {}) {
  const headers = {}
  let payload = body

  if (body && !isForm) {
    headers['Content-Type'] = 'application/json'
    payload = JSON.stringify(body)
  }

  const res = await fetch(`${API_BASE}${path}`, {
    method,
    headers,
    body: payload,
  })

  if (!res.ok) {
    let detail = `${res.status} ${res.statusText}`
    try {
      const text = await res.text()
      try {
        const data = JSON.parse(text)
        detail = data.detail || JSON.stringify(data)
      } catch {
        if (text) detail = text
      }
    } catch { /* ignore */ }
    throw new Error(detail || 'Request failed')
  }

  if (res.status === 204) {
    return null
  }

  return res.json()
}

export const api = {
  getRuntimeSettings: () => request('/settings/runtime'),
  updateRuntimeSettings: (values) =>
    request('/settings/runtime', {
      method: 'PUT',
      body: { values },
    }),

  listProjects: () => request('/lp-projects'),
  refreshProjects: () => request('/lp-projects/refresh', { method: 'POST' }),

  importFromLp: ({ projectIds, pipelineType }) =>
    request('/import/lp', {
      method: 'POST',
      body: { project_ids: projectIds, pipeline_type: pipelineType },
    }),

  importCrunchbase: ({
    status,
    contactEnriched,
    view,
    maxRecords = 200,
    tableName,
  }) =>
    request('/import/crunchbase', {
      method: 'POST',
      body: {
        status: status || null,
        contact_enriched: typeof contactEnriched === 'boolean' ? contactEnriched : null,
        view: view || null,
        max_records: maxRecords,
        table_name: tableName || null,
      },
    }),

  importNews: ({
    query,
    domains,
    language,
    fromDate,
    toDate,
    sortBy,
    pageSize,
    maxPages,
  }) =>
    request('/import/news', {
      method: 'POST',
      body: {
        query: query || null,
        domains: domains || null,
        language: language || null,
        from_date: fromDate || null,
        to_date: toDate || null,
        sort_by: sortBy || null,
        page_size: pageSize || null,
        max_pages: maxPages || null,
      },
    }),

  uploadCsv: ({ file, pipelineType }) => {
    const form = new FormData()
    form.append('file', file)
    return request(
      `/import/lp/upload?pipeline_type=${pipelineType}`,
      {
        method: 'POST',
        body: form,
        isForm: true,
      },
    )
  },

  uploadCrunchbaseCsv: ({ file }) => {
    const form = new FormData()
    form.append('file', file)
    return request('/import/crunchbase/upload', {
      method: 'POST',
      body: form,
      isForm: true,
    })
  },

  labelEntry: ({ entryId, label, learningData }) =>
    request(`/entries/${entryId}/label`, {
      method: 'POST',
      body: { label, learning_data: learningData },
    }),

  enrichEntry: ({ entryId, payload }) =>
    request(`/entries/${entryId}/enrich`, {
      method: 'PUT',
      body: payload,
    }),

  crunchbaseAction: ({ entryId, payload }) =>
    request(`/entries/${entryId}/cb-action`, {
      method: 'POST',
      body: payload,
    }),

  saveDraft: ({ messageId, text }) =>
    request(`/drafting/${messageId}/save`, {
      method: 'PUT',
      body: { draft_text: text },
    }),

  removeDraftingEntries: (entryIds) =>
    request('/drafting/remove', {
      method: 'POST',
      body: { entry_ids: entryIds },
    }),

  pushLeadspicker: ({ entryIds, projectId, pipelineKey }) =>
    request('/push/leadspicker', {
      method: 'POST',
      body: { entry_ids: entryIds, project_id: projectId, pipeline_key: pipelineKey || null },
    }),

  pushAirtable: ({ entryIds, tableName, pipelineKey }) =>
    request('/push/airtable', {
      method: 'POST',
      body: {
        entry_ids: entryIds,
        ...(tableName ? { table_name: tableName } : {}),
        ...(pipelineKey ? { pipeline_key: pipelineKey } : {}),
      },
    }),

  getPushLog: (batchId) => request(`/push/log/${batchId}`),
  searchMaster: (query) => request(`/search/master?q=${encodeURIComponent(query)}`),

  aiChat: ({ entryId, userMessage }) =>
    request('/ai/chat', {
      method: 'POST',
      body: { entry_id: entryId, user_message: userMessage },
    }),

  clearAiChat: (entryId) =>
    request('/ai/clear', {
      method: 'POST',
      body: { entry_id: entryId },
    }),

  // Pipeline configs
  listPipelineConfigs: () => request('/pipeline-configs'),
  createPipelineConfig: (body) => request('/pipeline-configs', { method: 'POST', body }),
  updatePipelineConfig: (id, body) => request(`/pipeline-configs/${id}`, { method: 'PUT', body }),
  deletePipelineConfig: (id) => request(`/pipeline-configs/${id}`, { method: 'DELETE' }),
  getLpProjectPreview: (projectId) => request(`/lp-projects/${projectId}/preview`),

  // Pipeline-level (reservoir architecture)
  getPipelineStats: (pipelineKey) => request(`/pipeline/${pipelineKey}/stats`),
  getPipelineDraftEntries: (pipelineKey) => request(`/pipeline/${pipelineKey}/draft-entries-full`),
  startPipelineDrafting: (pipelineKey) =>
    request(`/pipeline/${pipelineKey}/start-drafting`, { method: 'POST' }),
  finishPipelineDrafting: (pipelineKey) =>
    request(`/pipeline/${pipelineKey}/finish-drafting`, { method: 'POST' }),
  labelAiClassifierNo: (pipelineKey) =>
    request(`/pipeline/${pipelineKey}/label-ai-classifier-no`, { method: 'POST' }),

  // Staging
  getStagingEntries: (pipelineKey, { unlabeledOnly = false, label = null } = {}) => {
    const params = new URLSearchParams()
    if (label) params.set('label', label)
    else if (unlabeledOnly) params.set('unlabeled_only', 'true')
    const qs = params.toString()
    return request(`/staging/${pipelineKey}/entries${qs ? '?' + qs : ''}`)
  },
  labelStagingEntry: ({ pipelineKey, stagingId, label, learningData }) =>
    request(`/staging/${pipelineKey}/${stagingId}/label`, {
      method: 'POST',
      body: { label, learning_data: learningData },
    }),
  enrichStagingEntry: ({ pipelineKey, stagingId, payload }) =>
    request(`/staging/${pipelineKey}/${stagingId}/enrich`, {
      method: 'PUT',
      body: payload,
    }),
  cbStagingAction: ({ pipelineKey, stagingId, payload }) =>
    request(`/staging/${pipelineKey}/${stagingId}/cb-action`, {
      method: 'POST',
      body: payload,
    }),
  finishAnalysis: (pipelineKey) =>
    request(`/staging/${pipelineKey}/finish-analysis`, {
      method: 'POST',
      body: {},
    }),

  // Blacklist
  getBlacklist: () => request('/blacklist'),
  addToBlacklist: ({ company_name, company_linkedin, company_website, reason, added_by }) =>
    request('/blacklist', {
      method: 'POST',
      body: {
        company_name,
        company_linkedin: company_linkedin || null,
        company_website: company_website || null,
        reason: reason || null,
        added_by: added_by || null,
      },
    }),
  removeFromBlacklist: (id) => request(`/blacklist/${id}`, { method: 'DELETE' }),
  uploadBlacklistCsv: ({ file, columnMap, defaultReason, defaultAddedBy, defaultContactedVia }) => {
    const form = new FormData()
    form.append('file', file)
    const params = new URLSearchParams()
    if (columnMap) params.set('column_map', JSON.stringify(columnMap))
    if (defaultReason) params.set('default_reason', defaultReason)
    if (defaultAddedBy) params.set('default_added_by', defaultAddedBy)
    if (defaultContactedVia) params.set('default_origin', defaultContactedVia)
    const qs = params.toString() ? `?${params.toString()}` : ''
    return request(`/blacklist/upload-csv${qs}`, {
      method: 'POST',
      body: form,
      isForm: true,
    })
  },
  checkBlacklist: ({ company_name, company_linkedin, company_website }) =>
    request('/blacklist/check', {
      method: 'POST',
      body: { company_name: company_name || null, company_linkedin: company_linkedin || null, company_website: company_website || null },
    }),
  checkBlacklistBatch: (items) =>
    request('/blacklist/check-batch', {
      method: 'POST',
      body: items,
    }),
}