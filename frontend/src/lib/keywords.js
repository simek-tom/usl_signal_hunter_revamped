export const KEYWORD_REGEX = new RegExp(
  '(expand(?:s|ed|ing)?|expansion(?:s)?|scale(?:s|d|ing)?|grow|grows|grew|grown|growing|growth|global(?:ly)?|worldwide|overseas|abroad|europe(?:an)?|international(?:ly)?|cross-border|czech(?:ia)?|enter(?:s|ed|ing)?|launch(?:es|ed|ing)?)',
  'gi',
)

export function highlightText(text) {
  if (!text) {
    return [{ hit: false, value: '' }]
  }

  const parts = []
  let last = 0
  for (const match of text.matchAll(KEYWORD_REGEX)) {
    const idx = match.index ?? 0
    if (idx > last) {
      parts.push({ hit: false, value: text.slice(last, idx) })
    }
    parts.push({ hit: true, value: match[0] })
    last = idx + match[0].length
  }
  if (last < text.length) {
    parts.push({ hit: false, value: text.slice(last) })
  }
  return parts
}
