import re
import unicodedata

# Common legal-entity suffixes to strip when normalizing company names.
_LEGAL_SUFFIX = re.compile(
    r"[\s,]+("
    r"s\.?\s*r\.?\s*o\.?|spol\.\s*s\s*r\.?\s*o\.?|a\.?\s*s\.?|"
    r"v\.?\s*o\.?\s*s\.?|k\.?\s*s\.?|"            # Czech
    r"ltd\.?|llc|llp|inc\.?|corp\.?|"              # English
    r"gmbh|ag|kg|ohg|"                             # German
    r"s\.?\s*a\.?|s\.?\s*l\.?|s\.?\s*p\.?\s*a\.?" # Romance
    r")\s*$",
    re.IGNORECASE,
)


def normalize_company_name(name: str) -> str:
    """Lowercase, strip accents, remove legal suffixes, collapse whitespace."""
    # Normalize unicode (decompose accented chars)
    name = unicodedata.normalize("NFKD", name)
    name = "".join(c for c in name if not unicodedata.combining(c))
    name = name.lower().strip()
    # Remove legal suffix (repeat in case of e.g. "Acme, spol. s r.o.")
    for _ in range(2):
        name = _LEGAL_SUFFIX.sub("", name).strip()
    # Collapse internal whitespace
    name = re.sub(r"\s+", " ", name)
    return name
