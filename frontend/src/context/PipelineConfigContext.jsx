import { createContext, useContext, useEffect, useState } from 'react'
import { api } from '../lib/api'

const PipelineConfigContext = createContext({
  configs: [],
  loading: true,
  refresh: () => {},
  getConfig: () => null,
  getConfigsBySource: () => [],
  getLabel: () => '',
})

export function PipelineConfigProvider({ children }) {
  const [configs, setConfigs] = useState([])
  const [loading, setLoading] = useState(true)

  async function refresh() {
    try {
      const data = await api.listPipelineConfigs()
      setConfigs(data)
    } catch (err) {
      console.error('Failed to load pipeline configs:', err)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    refresh()
  }, [])

  function getConfig(pipelineKey) {
    return configs.find((c) => c.pipeline_key === pipelineKey) || null
  }

  function getConfigsBySource(sourceType) {
    return configs.filter((c) => c.source_type === sourceType)
  }

  function getLabel(pipelineKey) {
    const cfg = getConfig(pipelineKey)
    return cfg ? cfg.label : pipelineKey
  }

  return (
    <PipelineConfigContext.Provider value={{ configs, loading, refresh, getConfig, getConfigsBySource, getLabel }}>
      {children}
    </PipelineConfigContext.Provider>
  )
}

export function usePipelineConfigs() {
  return useContext(PipelineConfigContext)
}