import type { Pool } from "pg";
import { appendTimelineEvent } from "./graph-events.js";

async function tx<T>(pool: Pool, fn: (c: import("pg").PoolClient) => Promise<T>): Promise<T> {
  const c = await pool.connect();
  try { await c.query("begin"); const v = await fn(c); await c.query("commit"); return v; }
  catch(e) { await c.query("rollback"); throw e; } finally { c.release(); }
}

export async function skipOptionalNode(pool: Pool, workflowId: string, node: string, reason: string, operator: string): Promise<void> {
  await tx(pool, async (c) => {
    await c.query(`select id from stepline_workflows where id=$1 for update`, [workflowId]);
    const r = await c.query(`select status,optional from stepline_node_states where workflow_id=$1 and node=$2 for update`, [workflowId, node]);
    const row = r.rows[0];
    if (!row) throw new Error(`node ${node} does not exist`);
    if (!row.optional) throw new Error(`node ${node} is not optional`);
    if (!["pending","ready"].includes(row.status)) throw new Error(`node ${node} cannot be skipped from status ${row.status}`);
    await c.query(`update stepline_node_states set status='skipped',error_class='skipped',error=$2,locked_by=null,locked_until=null,updated_at=now() where workflow_id=$1 and node=$3`, [workflowId, reason, node]);
    await appendTimelineEvent(c, workflowId, { kind: "skipped", node, operator, data: { reason, operator, impact: "dependents use their onSkipped rules" } });
    await applyDependencyRules(c, workflowId);
    await recompute(c, workflowId);
  });
}

export async function pauseWorkflow(pool: Pool, workflowId: string, operator: string, reason: string): Promise<void> {
  await tx(pool, async (c) => {
    await c.query(`update stepline_workflows set status='paused',paused_reason=$2,paused_by=$3,updated_at=now() where id=$1 and status='running'`, [workflowId, reason, operator]);
    await c.query(`update stepline_node_states set status='paused',error_class='paused',error=$2,updated_at=now() where workflow_id=$1 and status='ready'`, [workflowId, reason]);
    await appendTimelineEvent(c, workflowId, { kind: "paused", operator, data: { reason } });
  });
}

export async function resumeWorkflow(pool: Pool, workflowId: string, operator: string, reason = "manual resume"): Promise<void> {
  await tx(pool, async (c) => {
    await c.query(`update stepline_workflows set status='running',paused_reason=null,paused_by=null,updated_at=now() where id=$1 and status='paused'`, [workflowId]);
    await c.query(`update stepline_node_states set status='ready',error_class=null,error=null,run_at=now(),updated_at=now() where workflow_id=$1 and status='paused'`, [workflowId]);
    await appendTimelineEvent(c, workflowId, { kind: "resumed", operator, data: { reason } });
  });
}

export async function cancelWorkflow(pool: Pool, workflowId: string, operator: string, reason: string): Promise<void> {
  await tx(pool, async (c) => {
    await c.query(`update stepline_workflows set status='cancelled',updated_at=now() where id=$1 and status in ('running','paused')`, [workflowId]);
    await c.query(`update stepline_node_states set status='cancelled',error_class='cancelled',error=$2,updated_at=now() where workflow_id=$1 and status in ('pending','ready','paused')`, [workflowId, reason]);
    await appendTimelineEvent(c, workflowId, { kind: "cancelled", operator, data: { reason, runningNodes: "receive an abort signal from live workers; expired leases are recovered as cancelled" } });
  });
}

export async function applyDependencyRules(c: import("pg").PoolClient, workflowId: string): Promise<void> {
  const { rows } = await c.query(`select node,status,depends_on from stepline_node_states where workflow_id=$1 for update`, [workflowId]);
  const statuses = new Map<string,string>(rows.map((r)=>[r.node,r.status]));
  for (const row of rows) {
    if (row.status !== "pending") continue;
    const deps = row.depends_on as Array<{node:string;onSkipped?:string}>;
    if (!deps.every((d)=>["succeeded","skipped"].includes(statuses.get(d.node) ?? ""))) continue;
    const stop = deps.some((d)=>statuses.get(d.node)==="skipped" && (d.onSkipped ?? "stop")==="stop");
    await c.query(`update stepline_node_states set status=$2,error_class=$3,error=$4,updated_at=now() where workflow_id=$1 and node=$5`, [workflowId, stop?"blocked":"ready", stop?"blocked":null, stop?"upstream optional node was skipped":null, row.node]);
  }
}

export async function recompute(c: import("pg").PoolClient, workflowId: string): Promise<void> {
  const r = await c.query(`select count(*) filter (where status='failed') failed,count(*) filter (where status='blocked') blocked,count(*) filter (where status in ('pending','ready','running','paused')) active from stepline_node_states where workflow_id=$1`, [workflowId]);
  const x = r.rows[0];
  if (Number(x.failed)>0) await c.query(`update stepline_workflows set status='failed',updated_at=now() where id=$1 and status <> 'cancelled'`, [workflowId]);
  else if (Number(x.active)===0 && Number(x.blocked)===0) await c.query(`update stepline_workflows set status='completed',updated_at=now() where id=$1 and status <> 'cancelled'`, [workflowId]);
  else if (Number(x.active)===0 && Number(x.blocked)>0) await c.query(`update stepline_workflows set status='blocked',updated_at=now() where id=$1`, [workflowId]);
}
