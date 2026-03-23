import re
import unicodedata
from urllib.parse import urlparse

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


def clean_company_name(name: str) -> str:
    """Like normalize_company_name but also removes all spaces (for exact/substring matching)."""
    return normalize_company_name(name).replace(" ", "")


def extract_linkedin_slug(url: str | None) -> str | None:
    """Extract company slug from a LinkedIn URL, e.g. 'linkedin.com/company/google/' → 'google'."""
    if not url:
        return None
    try:
        path = urlparse(url).path.strip("/")
    except Exception:
        return None
    # Expected: "company/slug" or "in/slug"
    parts = path.split("/")
    if len(parts) >= 2 and parts[0] in ("company", "in"):
        slug = parts[1].strip().lower()
        return slug if slug else None
    return None


def extract_root_domain(url: str | None) -> str | None:
    """Extract root domain from a URL, e.g. 'https://www.apple.com/about' → 'apple.com'."""
    if not url:
        return None
    try:
        host = urlparse(url if "://" in url else f"https://{url}").hostname
    except Exception:
        return None
    if not host:
        return None
    # Strip 'www.' prefix
    if host.startswith("www."):
        host = host[4:]
    return host.lower() if host else None
