-- ============================================================
-- STAGING TABLES
-- ============================================================

CREATE TABLE public.staging_leadspicker (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pipeline_key TEXT NOT NULL,
    batch_id UUID,
    dedup_key TEXT NOT NULL,

    -- Signal / post
    content_url TEXT,
    content_text TEXT,
    content_summary TEXT,
    ai_classifier TEXT,
    ai_pre_score REAL,

    -- Author (from LP scrape)
    author_first_name TEXT,
    author_last_name TEXT,
    author_full_name TEXT,
    author_linkedin TEXT,
    author_position TEXT,

    -- Company (from LP scrape)
    company_name TEXT,
    company_website TEXT,
    company_linkedin TEXT,
    company_country TEXT,
    company_employee_count TEXT,

    -- External IDs
    external_id TEXT,
    source_robot TEXT,

    -- Enrichment overrides (filled during analysis)
    enriched_contact_name TEXT,
    enriched_contact_linkedin TEXT,
    enriched_contact_position TEXT,
    enriched_company_name TEXT,
    enriched_company_website TEXT,
    enriched_company_linkedin TEXT,

    -- Labeling
    label TEXT CHECK (label IN ('yes', 'no', 'cc')),
    learning_data BOOLEAN DEFAULT FALSE,
    labeled_at TIMESTAMPTZ,

    -- Promotion tracking
    pipeline_entry_id UUID,   -- set after promotion to core tables
    promoted_at TIMESTAMPTZ,  -- set after promotion

    ai_chat_state JSONB,
    source_metadata JSONB DEFAULT '{}',

    fetched_at TIMESTAMPTZ DEFAULT now(),
    created_at TIMESTAMPTZ DEFAULT now(),

    UNIQUE (dedup_key)
);

CREATE INDEX idx_staging_lp_pipeline_key ON staging_leadspicker (pipeline_key);
CREATE INDEX idx_staging_lp_batch_id ON staging_leadspicker (batch_id);
CREATE INDEX idx_staging_lp_label ON staging_leadspicker (label) WHERE label IS NOT NULL;
CREATE INDEX idx_staging_lp_ai_classifier ON staging_leadspicker ((lower(ai_classifier)));
CREATE INDEX idx_staging_lp_not_promoted ON staging_leadspicker (label) WHERE label IN ('yes','cc') AND pipeline_entry_id IS NULL;


CREATE TABLE public.staging_crunchbase (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pipeline_key TEXT NOT NULL,
    batch_id UUID,
    dedup_key TEXT NOT NULL,

    -- Company
    company_name TEXT,
    company_website TEXT,
    company_linkedin TEXT,
    company_country TEXT,
    company_industry TEXT,
    company_hq_location TEXT,
    company_description TEXT,
    company_founded_on TEXT,
    company_employee_count TEXT,
    crunchbase_profile_url TEXT,

    -- Funding
    funding_series TEXT,
    last_funding_amount TEXT,
    last_funding_date TEXT,
    funding_rounds TEXT,
    investors TEXT,
    revenue_range TEXT,

    -- Signal
    content_url TEXT,
    content_summary TEXT,
    ai_classifier TEXT,
    ai_pre_score REAL,

    -- Contacts
    main_contact_linkedin TEXT,
    secondary_contact_1 TEXT,
    secondary_contact_2 TEXT,
    secondary_contact_3 TEXT,

    -- Message
    message_fin TEXT,

    -- Airtable linkage
    external_id TEXT,
    airtable_status TEXT,
    contact_enriched BOOLEAN,

    -- Labeling
    label TEXT CHECK (label IN ('yes', 'no', 'cc')),
    learning_data BOOLEAN DEFAULT FALSE,
    labeled_at TIMESTAMPTZ,

    -- Workflow
    workflow_status TEXT DEFAULT 'new'
        CHECK (workflow_status IN ('new','analyzed','pushed-ready','pushed','eliminated')),

    -- Promotion tracking
    pipeline_entry_id UUID,
    promoted_at TIMESTAMPTZ,

    ai_chat_state JSONB,
    source_metadata JSONB DEFAULT '{}',

    fetched_at TIMESTAMPTZ DEFAULT now(),
    created_at TIMESTAMPTZ DEFAULT now(),

    UNIQUE (dedup_key)
);

CREATE INDEX idx_staging_cb_pipeline_key ON staging_crunchbase (pipeline_key);
CREATE INDEX idx_staging_cb_batch_id ON staging_crunchbase (batch_id);
CREATE INDEX idx_staging_cb_label ON staging_crunchbase (label) WHERE label IS NOT NULL;
CREATE INDEX idx_staging_cb_workflow ON staging_crunchbase (workflow_status);
CREATE INDEX idx_staging_cb_not_promoted ON staging_crunchbase (label) WHERE label IN ('yes','cc') AND pipeline_entry_id IS NULL;


CREATE TABLE public.staging_news (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pipeline_key TEXT NOT NULL,
    batch_id UUID,
    dedup_key TEXT NOT NULL,

    -- Article
    content_url TEXT,
    content_title TEXT,
    content_text TEXT,
    content_summary TEXT,
    ai_classifier TEXT,
    ai_pre_score REAL,

    -- Article metadata
    article_author TEXT,
    source_name TEXT,
    source_id TEXT,
    published_at TIMESTAMPTZ,
    url_to_image TEXT,

    -- Enrichment
    enriched_company_name TEXT,
    enriched_company_website TEXT,
    enriched_company_linkedin TEXT,
    enriched_contact_name TEXT,
    enriched_contact_linkedin TEXT,
    enriched_contact_position TEXT,

    -- Labeling
    label TEXT CHECK (label IN ('yes', 'no', 'cc')),
    learning_data BOOLEAN DEFAULT FALSE,
    labeled_at TIMESTAMPTZ,

    -- Promotion tracking
    pipeline_entry_id UUID,
    promoted_at TIMESTAMPTZ,

    ai_chat_state JSONB,
    source_metadata JSONB DEFAULT '{}',

    fetched_at TIMESTAMPTZ DEFAULT now(),
    created_at TIMESTAMPTZ DEFAULT now(),

    UNIQUE (dedup_key)
);

CREATE INDEX idx_staging_news_pipeline_key ON staging_news (pipeline_key);
CREATE INDEX idx_staging_news_batch_id ON staging_news (batch_id);
CREATE INDEX idx_staging_news_label ON staging_news (label) WHERE label IS NOT NULL;
CREATE INDEX idx_staging_news_not_promoted ON staging_news (label) WHERE label IN ('yes','cc') AND pipeline_entry_id IS NULL;


-- ============================================================
-- CONTACTED COMPANIES
-- ============================================================

CREATE TABLE public.contacted_companies (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    company_name_normalized TEXT,
    domain_normalized TEXT,
    linkedin_url TEXT,
    crunchbase_url TEXT,
    fingerprint TEXT,
    first_contacted_at TIMESTAMPTZ DEFAULT now(),
    last_contacted_at TIMESTAMPTZ DEFAULT now(),
    contacted_via TEXT,       -- pipeline_key
    pipeline_entry_id UUID,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_contacted_fp ON contacted_companies (fingerprint) WHERE fingerprint IS NOT NULL;
CREATE INDEX idx_contacted_domain ON contacted_companies (domain_normalized) WHERE domain_normalized IS NOT NULL;
CREATE INDEX idx_contacted_linkedin ON contacted_companies (linkedin_url) WHERE linkedin_url IS NOT NULL;
CREATE INDEX idx_contacted_name ON contacted_companies (company_name_normalized) WHERE company_name_normalized IS NOT NULL;