-- USL Signal Hunter v2
-- Crunchbase pipeline support additions
-- Date: 2026-03-09

begin;

-- 1) Add pushed-ready lifecycle state for CB Save&Next action.
do $$
begin
  if exists (
    select 1
    from pg_type t
    where t.typname = 'entry_status'
  ) and not exists (
    select 1
    from pg_type t
    join pg_enum e on e.enumtypid = t.oid
    where t.typname = 'entry_status'
      and e.enumlabel = 'pushed-ready'
  ) then
    alter type public.entry_status add value 'pushed-ready';
  end if;
exception
  when undefined_table then null;
  when undefined_object then null;
end $$;

-- 2) Faster dedup/lookups by Crunchbase external id.
create index if not exists idx_signals_source_external_id
  on public.signals (source_type, external_id)
  where external_id is not null;

commit;
