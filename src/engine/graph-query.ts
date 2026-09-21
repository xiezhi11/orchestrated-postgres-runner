import type { Pool } from "pg";
import type { TimelineEvent } from "../types.js";

function mapEvent(row: Record<string, unknown>): TimelineEvent {
  return {
    id: Number(row.id), workflowId: row.workflow_id as string, seq: Number(row.seq),
    kind: row.kind as string, step: (row.step as string) ?? null, data: row.data as Record<string, unknown>,
    createdAt: row.created_at as Date, attempt: row.attempt as number | null,
    executionId: row.execution_id as string | null, eventKey: row.event_key as string | null,
    operator: row.operator as string | null, metadata: row.metadata as Record<string, unknown>,
  };
}

/** Same stable sort is used by online timeline, export, and paginated replay. */
export async function getTimeline(pool: Pool, workflowId: string, opts: { limit?: number; afterSeq?: number } = {}): Promise<TimelineEvent[]> {
  const { rows } = await pool.query(
    `select * from stepline_events where workflow_id=$1 and ($2::int is null or seq>$2)
     order by seq asc,id asc limit $3`,
    [workflowId, opts.afterSeq ?? null, opts.limit ?? 1000],
  );
  return rows.map(mapEvent);
}

export async function getNodeTimeline(pool: Pool, workflowId: string, node: string): Promise<TimelineEvent[]> {
  const { rows } = await pool.query(`select * from stepline_events where workflow_id=$1 and step=$2 order by seq,id`, [workflowId, node]);
  return rows.map(mapEvent);
}

export async function getAttemptTimeline(pool: Pool, executionId: string): Promise<{ events: TimelineEvent[]; attempt: Record<string, unknown> | null }> {
  const events = (await pool.query(`select * from stepline_events where execution_id=$1 order by seq,id`, [executionId])).rows.map(mapEvent);
  const attempt = (await pool.query(`select * from stepline_attempts where execution_id=$1`, [executionId])).rows[0] ?? null;
  return { events, attempt: attempt ?? null };
}

export async function exportWorkflowRecords(pool: Pool, workflowId: string, pageSize = 250): Promise<Record<string, unknown>> {
  const [workflow, nodes, attempts, notifications] = await Promise.all([
    pool.query(`select id,type,status,graph_id,graph_version,graph,input_summary,error,created_at,updated_at from stepline_workflows where id=$1`, [workflowId]),
    pool.query(`select * from stepline_node_states where workflow_id=$1 order by node`, [workflowId]),
    pool.query(`select * from stepline_attempts where workflow_id=$1 order by node,attempt`, [workflowId]),
    pool.query(`select id,idempotency_key,destination,status,attempts,last_error,confirmed_at,created_at from stepline_notifications where workflow_id=$1 order by id`, [workflowId]),
  ]);
  const events: TimelineEvent[] = [];
  let afterSeq: number | undefined;
  for (;;) {
    const page = await getTimeline(pool, workflowId, { limit: pageSize, afterSeq });
    events.push(...page);
    if (page.length < pageSize) break;
    afterSeq = page[page.length - 1]!.seq;
  }
  return {
    exportedAt: new Date().toISOString(), order: ["seq", "id"],
    workflow: workflow.rows[0] ?? null, nodes: nodes.rows, attempts: attempts.rows,
    events, notifications: notifications.rows,
  };
}
