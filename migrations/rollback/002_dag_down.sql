drop table if exists stepline_notification_subscriptions;
drop table if exists stepline_exports;
drop table if exists stepline_notifications;
drop table if exists stepline_attempts;
drop table if exists stepline_node_states;
drop index if exists ux_stepline_events_event_key;
alter table stepline_events
  drop column if exists attempt,
  drop column if exists execution_id,
  drop column if exists event_key,
  drop column if exists operator,
  drop column if exists metadata;
alter table stepline_workflows
  drop column if exists graph_id,
  drop column if exists graph_version,
  drop column if exists graph,
  drop column if exists input_summary,
  drop column if exists concurrency,
  drop column if exists paused_reason,
  drop column if exists paused_by;
delete from stepline_schema_meta where version = 2;
