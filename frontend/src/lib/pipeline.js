export const PIPELINES = [
  {
    key: 'lp_general',
    label: 'Leadspicker General',
    airtableTable: 'Leadspicker - general post',
    showCc: false,
  },
  {
    key: 'lp_czech',
    label: 'Leadspicker Czechia',
    airtableTable: 'Leadspicker - czehcia post',
    showCc: true,
  },
  {
    key: 'crunchbase',
    label: 'Crunchbase',
    airtableTable: 'Crunchbase Source',
    showCc: false,
  },
  {
    key: 'news',
    label: 'News',
    airtableTable: 'Seed round',
    showCc: false,
  },
]

export function pipelineLabel(type) {
  const found = PIPELINES.find((p) => p.key === type)
  return found ? found.label : type
}
