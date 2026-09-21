-- stepline core schema. Every table is CREATE TABLE IF NOT EXISTS so re-running
-- migrate() on an already-migrated database is a no-op.

create table if not exists stepline_workflows (
  id           uuid primary key default gen_random_uuid(),
  type         text not null,
  status       text not null default 'running',   -- running | completed | failed | cancelled
  current_step text,
  input        jsonb not null default '{}',
  context      jsonb not null default '{}',        -- accumulated step outputs, keyed by step name
  error        text,
  created_at   timestamptz not null default now(),
  updated_at   timestamptz not null default now()
);
create index if not exists ix_stepline_workflows_status on stepline_workflows(status);

-- Append-only audit trail. Never updated, never deleted (barring your own
-- retention policy) -- this is the "what actually happened, in order" record.
create table if not exists stepline_events (
  id          bigint generated always as identity primary key,
  workflow_id uuid not null references stepline_workflows(id) on delete cascade,
  seq         int not null,
  kind        text not null,   -- started | step_scheduled | step_completed | step_failed | timer_scheduled | completed | failed | cancelled
  step        text,
  data        jsonb not null default '{}',
  created_at  timestamptz not null default now(),
  unique (workflow_id, seq)
);

-- The work queue. A row is "due" when run_at <= now() and it's pending, or it
-- was leased but the lease expired (locked_until < now()) -- that second
-- condition, not a separate reaper process, is how a crashed worker's task
-- gets picked back up.
create table if not exists stepline_tasks (
  id           uuid primary key default gen_random_uuid(),
  workflow_id  uuid not null references stepline_workflows(id) on delete cascade,
  step         text not null,
  attempt      int not null default 0,
  max_attempts int not null default 5,
  run_at       timestamptz not null default now(),
  status       text not null default 'pending',   -- pending | leased | done | failed
  locked_by    text,
  locked_until timestamptz,
  input        jsonb not null default '{}',
  created_at   timestamptz not null default now()
);
create index if not exists ix_stepline_tasks_due
  on stepline_tasks(run_at)
  where status = 'pending';
create index if not exists ix_stepline_tasks_workflow on stepline_tasks(workflow_id);

-- Backs the once() helper: lets a step's side effect (send an email, charge a
-- card) survive a retry without repeating it, PROVIDED the process lives long
-- enough to record the result after the side effect succeeds. See the README
-- "What this does not guarantee" section -- this is at-least-once execution
-- with a de-dup helper, not a two-phase-commit exactly-once system.
create table if not exists stepline_idempotent_calls (
  key        text primary key,
  result     jsonb,
  created_at timestamptz not null default now()
);

create table if not exists stepline_schema_meta (
  version int not null
);
insert into stepline_schema_meta (version)
  select 1 where not exists (select 1 from stepline_schema_meta)
  on conflict do nothing;
