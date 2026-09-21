-- Versioned DAG execution, attempts, idempotent timeline events, and outbox.
-- Every runtime transition is inserted together with its event(s) in one
-- transaction; readers therefore never observe a completed node without the
-- corresponding completion event and unlocked dependents.

alter table stepline_workflows
  add column if not exists graph_id text,
  add column if not exists graph_version int,
  add column if not exists graph jsonb,
  add column if not exists input_summary jsonb not null default '{}',
  add column if not exists concurrency int not null default 1,
  add column if not exists paused_reason text,
  add column if not exists paused_by text;

alter table stepline_events
  add column if not exists attempt int,
  add column if not exists execution_id text,
  add column if not exists event_key text,
  add column if not exists operator text,
  add column if not exists metadata jsonb not null default '{}';

create unique index if not exists ux_stepline_events_event_key
  on stepline_events(event_key);

create table if not exists stepline_node_states (
  workflow_id uuid not null references stepline_workflows(id) on delete cascade,
  node text not null,
  status text not null check (status in (
    'pending','ready','running','succeeded','failed','skipped','blocked','paused','cancelled'
  )),
  depends_on jsonb not null default '[]',
  optional boolean not null default false,
  attempts int not null default 0,
  max_attempts int not null default 1,
  run_at timestamptz not null default now(),
  queued_seq bigint generated always as identity,
  locked_by text,
  locked_until timestamptz,
  input_summary jsonb not null default '{}',
  output_ref text,
  output jsonb not null default 'null',
  error text,
  error_class text check (error_class in ('retryable','permanent','paused','cancelled','skipped','blocked')),
  updated_at timestamptz not null default now(),
  primary key (workflow_id, node)
);
create index if not exists ix_stepline_nodes_due
  on stepline_node_states(run_at, queued_seq)
  where status = 'ready';
create index if not exists ix_stepline_nodes_workflow
  on stepline_node_states(workflow_id, status);

create table if not exists stepline_attempts (
  execution_id text primary key,
  workflow_id uuid not null references stepline_workflows(id) on delete cascade,
  node text not null,
  attempt int not null,
  status text not null check (status in ('running','succeeded','failed','paused','cancelled')),
  started_at timestamptz not null default now(),
  finished_at timestamptz,
  error_class text,
  error text,
  backoff_ms int,
  jitter_ms int,
  unique (workflow_id, node, attempt)
);

create table if not exists stepline_notifications (
  id bigint generated always as identity primary key,
  workflow_id uuid not null references stepline_workflows(id) on delete cascade,
  event_id bigint references stepline_events(id) on delete cascade,
  idempotency_key text not null unique,
  destination text not null,
  payload jsonb not null default '{}',
  status text not null default 'pending' check (status in ('pending','confirmed','dead')),
  attempts int not null default 0,
  run_at timestamptz not null default now(),
  last_error text,
  confirmed_at timestamptz,
  created_at timestamptz not null default now()
);
create index if not exists ix_stepline_notifications_due
  on stepline_notifications(workflow_id, run_at, id)
  where status = 'pending';

create table if not exists stepline_notification_subscriptions (
  workflow_id uuid not null references stepline_workflows(id) on delete cascade,
  destination text not null,
  event_kinds jsonb not null default '[]',
  primary key (workflow_id, destination)
);

create table if not exists stepline_exports (
  id bigint generated always as identity primary key,
  workflow_id uuid not null references stepline_workflows(id) on delete cascade,
  cursor_seq bigint,
  payload jsonb not null default '{}',
  created_at timestamptz not null default now()
);

insert into stepline_schema_meta (version)
  select 2 where not exists (select 1 from stepline_schema_meta where version = 2)
  on conflict do nothing;
