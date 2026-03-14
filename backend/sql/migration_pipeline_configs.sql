CREATE TABLE public.pipeline_configs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_type TEXT NOT NULL CHECK (source_type IN ('leadspicker', 'crunchbase', 'news')),
    pipeline_key TEXT NOT NULL UNIQUE,
    label TEXT NOT NULL,
    airtable_table_name TEXT,
    lp_project_ids INTEGER[],
    default_import_params JSONB DEFAULT '{}',
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

INSERT INTO pipeline_configs (source_type, pipeline_key, label, airtable_table_name) VALUES
    ('leadspicker', 'lp_general', 'Leadspicker General', 'Leadspicker - general post'),
    ('leadspicker', 'lp_czech', 'Leadspicker Czechia', 'Leadspicker - czehcia post'),
    ('crunchbase', 'crunchbase', 'Crunchbase', 'Crunchbase Source'),
    ('news', 'news', 'News', 'Seed round');