from enum import Enum


class SourceType(str, Enum):
    leadspicker = "leadspicker"
    crunchbase = "crunchbase"
    news = "news"


class PipelineType(str, Enum):
    lp_general = "lp_general"
    lp_czech = "lp_czech"
    crunchbase = "crunchbase"
    news = "news"


class EntryStatus(str, Enum):
    new = "new"
    analyzed = "analyzed"
    enriched = "enriched"
    drafted = "drafted"
    pushed_ready = "pushed-ready"
    pushed = "pushed"
    eliminated = "eliminated"


class RelevanceLabel(str, Enum):
    yes = "yes"
    no = "no"
    cc = "cc"


class PushStatus(str, Enum):
    success = "success"
    fail = "fail"


class PushTarget(str, Enum):
    leadspicker = "leadspicker"
    airtable = "airtable"
