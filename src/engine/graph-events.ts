import type { PoolClient } from "pg";
import { randomUUID } from "node:crypto";

export interface TimelineInput {
  kind: string;
  node?: string | null;
  attempt?: number | null;
  executionId?: string | null;
  operator?: string | null;
  data?: Record<string, unknown>;
  metadata?: Record<string, unknown>;
  key?: string;
}

export async function appendTimelineEvent(
  client: PoolClient,
  workflowId: string,
  event: TimelineInput,
): Promise<number> {
  const eventKey = event.key ?? `${workflowId}:${event.node ?? "workflow"}:${event.kind}:${event.executionId ?? ""}:${event.attempt ?? ""}`;
  const { rows } = await client.query(
    `insert into stepline_events
       (workflow_id, seq, kind, step, attempt, execution_id, event_key, operator, data, metadata)
     values (
       $1,
       coalesce((select max(seq)+1 from stepline_events where workflow_id=$1), 1),
       $2,$3,$4,$5,$6,$7,$8,$9
     )
     on conflict (event_key) do update set event_key = excluded.event_key
     returning id, seq`,
    [
      workflowId,
      event.kind,
      event.node ?? null,
      event.attempt ?? null,
      event.executionId ?? null,
      eventKey,
      event.operator ?? null,
      JSON.stringify(event.data ?? {}),
      JSON.stringify(event.metadata ?? {}),
    ],
  );
  const id = Number(rows[0].id);
  await publishWorkflowNotifications(client, workflowId, id, event.kind);
  return id;
}

export function newExecutionId(): string {
  return randomUUID();
}

export async function enqueueNotification(
  client: PoolClient,
  input: {
    workflowId: string;
    eventId: number;
    destination: string;
    payload: Record<string, unknown>;
  },
): Promise<void> {
  const key = `notif:${input.workflowId}:${input.eventId}:${input.destination}`;
  await client.query(
    `insert into stepline_notifications
       (workflow_id,event_id,idempotency_key,destination,payload)
     values ($1,$2,$3,$4,$5)
     on conflict (idempotency_key) do nothing`,
    [
      input.workflowId,
      input.eventId,
      key,
      input.destination,
      JSON.stringify({ ...input.payload, idempotencyKey: key }),
    ],
  );
}

/** Enqueues in the same transaction as the state change/event insertion. */
export async function publishWorkflowNotifications(
  client: PoolClient,
  workflowId: string,
  eventId: number,
  kind: string,
): Promise<void> {
  const { rows } = await client.query(
    `select destination,event_kinds from stepline_notification_subscriptions where workflow_id=$1`,
    [workflowId],
  );
  for (const row of rows) {
    const kinds = row.event_kinds as string[];
    if (!Array.isArray(kinds) || kinds.length === 0 || kinds.includes(kind)) {
      await enqueueNotification(client, {
        workflowId,
        eventId,
        destination: row.destination as string,
        payload: { kind },
      });
    }
  }
}
