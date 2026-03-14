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
      const data = await res.json()
      detail = data.detail || JSON.stringify(data)
    } catch {
      detail = await res.text()
    }
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

  getBatches: () => request('/batches'),
  getBatchEntries: (batchId) => request(`/batches/${batchId}/entries-full`),
  getBatchContext: (batchId) => request(`/batches/${batchId}/context`),
  finishLabeling: (batchId) => request(`/batches/${batchId}/finish-labeling`, { method: 'POST' }),
  startDrafting: (batchId) => request(`/batches/${batchId}/start-drafting`, { method: 'POST' }),
  getDraftEntries: (batchId) => request(`/batches/${batchId}/draft-entries-full`),
  finishDrafting: (batchId) => request(`/batches/${batchId}/finish-drafting`, { method: 'POST' }),

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

  pushLeadspicker: ({ entryIds, projectId }) =>
    request('/push/leadspicker', {
      method: 'POST',
      body: { entry_ids: entryIds, project_id: projectId },
    }),

  pushAirtable: ({ entryIds, tableName }) =>
    request('/push/airtable', {
      method: 'POST',
      body: tableName
        ? { entry_ids: entryIds, table_name: tableName }
        : { entry_ids: entryIds },
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

  // Staging
  getStagingEntries: (pipelineKey, batchId) => {
    const params = batchId ? `?batch_id=${batchId}` : ''
    return request(`/staging/${pipelineKey}/entries${params}`)
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
  finishAnalysis: ({ pipelineKey, batchId }) =>
    request(`/staging/${pipelineKey}/finish-analysis`, {
      method: 'POST',
      body: { batch_id: batchId },
    }),
  checkContacted: (payload) =>
    request('/staging/_contacted-check', {
      method: 'POST',
      body: payload,
    }),
}