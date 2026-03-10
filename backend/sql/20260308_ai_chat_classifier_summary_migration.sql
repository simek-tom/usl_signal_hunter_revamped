-- USL Signal Hunter v2
-- Proposed schema migration for AI classifier, summary, and drafting chat state.
-- Date: 2026-03-08

begin;

-- 1) Signal-level AI outputs used in pipeline/analysis views.
alter table public.signals
  add column if not exists ai_classifier text;

alter table public.signals
  add column if not exists content_summary text;

-- Optional compatibility alias if a plain `summary` column is preferred by BI/reporting.
-- Keep app writes/reads on content_summary; this can be populated by trigger/view later if needed.
alter table public.signals
  add column if not exists summary text;

-- Keep both summary fields in sync for newly imported records when one is missing.
update public.signals
set summary = content_summary
where summary is null
  and content_summary is not null;

update public.signals
set content_summary = summary
where content_summary is null
  and summary is not null;

-- 2) Per-entry chat crash-recovery state for Draft Assistant.
alter table public.pipeline_entries
  add column if not exists ai_chat_state jsonb;

-- 3) Helpful indexes for filtering and lookups.
create index if not exists idx_signals_ai_classifier_lower
  on public.signals ((lower(ai_classifier)));

create index if not exists idx_pipeline_entries_ai_chat_state_gin
  on public.pipeline_entries
  using gin (ai_chat_state)
  where ai_chat_state is not null;

commit;
