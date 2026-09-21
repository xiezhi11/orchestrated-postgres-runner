import type { Pool, PoolClient } from "pg";
import type {
  GraphDef,
  SubmitGraphOptions,
  TimelineEvent,
} from "../types.js";
import {
  dependencyOf,
  getGraphDef,
  listRegisteredGraphs,
  retryConfigFor,
  summarizeValue,
} from "./graph-registry.js";
import { appendTimelineEvent, newExecutionId, publishWorkflowNotifications } from "./graph-events.js";

type TerminalNode = "succeeded" | "failed" | "skipped" | "blocked" | "cancelled";

export class GraphExecutionError extends Error {
  constructor(public readonly errorClass: "retryable" | "permanent" | "paused", message: string) {
    super(message);
  }
}
export const retryable = (message: string) => new GraphExecutionError("retryable", message);
export const permanent = (message: string) => new GraphExecutionError("permanent", message);
export const pause = (message: string) => new GraphExecutionError("paused", message);

export interface GraphWorkerOptions {
  maxGlobalConcurrency?: number;
  leaseSeconds?: number;
  workerId?: string;
}

export async function submitGraph<TInput>(pool: Pool, graphId: string, options: SubmitGraphOptions<TInput>): Promise<string> {
  const versions = listRegisteredGraphs().filter((g) => g.id === graphId).map((g) => g.version);
  if (!versions.length) throw new Error(`graph ${graphId} is not registered`);
  const version = Math.max(...versions);
  const def = getGraphDef(graphId, version);
  const client = await pool.connect();
  try {
    await client.query("begin");
    const { rows } = await client.query(
      `insert into stepline_workflows
       (type,status,graph_id,graph_version,graph,input,input_summary,concurrency)
       values ($1,'running',$2,$3,$4,$5,$6,$7) returning id`,
      [graphId, graphId, version, JSON.stringify(serializeGraph(def)), JSON.stringify(options.input ?? {}),
       JSON.stringify(summarizeValue(options.input ?? {})), def.concurrency ?? 1],
    );
    const workflowId = rows[0].id as string;
    const submittedId = await appendTimelineEvent(client, workflowId, {
      kind: "submitted", key: `${workflowId}:submitted`,
      data: { graphId, version, inputSummary: summarizeValue(options.input ?? {}) },
    });
    for (const n of options.notify ?? []) {
      await client.query(
        `insert into stepline_notification_subscriptions(workflow_id,destination,event_kinds) values($1,$2,$3)
         on conflict (workflow_id,destination) do update set event_kinds=excluded.event_kinds`,
        [workflowId, n.destination, JSON.stringify(n.eventKinds ?? [])],
      );
    }
    await publishWorkflowNotifications(client, workflowId, submittedId, "submitted");
    for (const [node, spec] of Object.entries(def.nodes)) {
      const deps = (spec.dependsOn ?? []).map(dependencyOf);
      await client.query(
        `insert into stepline_node_states
         (workflow_id,node,status,depends_on,optional,max_attempts,input_summary)
         values ($1,$2,$3,$4,$5,$6,$7)`,
        [workflowId, node, deps.length ? "pending" : "ready", JSON.stringify(deps),
         Boolean(spec.optional), retryConfigFor(def, node).maxAttempts,
         JSON.stringify(summarizeValue(options.input ?? {}))],
      );
      const eventId = await appendTimelineEvent(client, workflowId, {
        kind: "scheduled", node,
        data: { dependsOn: deps.map((d) => d.node), optional: Boolean(spec.optional) },
      });
      await publishWorkflowNotifications(client, workflowId, eventId, "scheduled");
    }
    await client.query("commit");
    return workflowId;
  } catch (e) { await client.query("rollback"); throw e; }
  finally { client.release(); }
}

type Lease = Pick<LeasedGraphNode, "workflowId" | "node" | "attempt" | "executionId" | "def">;
async function lockWorkflow(client: PoolClient, id: string) {
  const r = await client.query(`select status from stepline_workflows where id=$1 for update`, [id]);
  return r.rows[0] as { status: string };
}
async function finishAttempt(client: PoolClient, lease: Lease, status: string, errorClass?: string, error?: string, backoffMs?: number, jitterMs?: number) {
  await client.query(`update stepline_attempts set status=$2,finished_at=now(),error_class=$3,error=$4,backoff_ms=$5,jitter_ms=$6 where execution_id=$1`,
    [lease.executionId, status, errorClass ?? null, error ?? null, Math.round(backoffMs ?? 0), jitterMs ?? null]);
}
function outputRef(output: unknown): string | null {
  if (output && typeof output === "object" && "ref" in output) {
    const ref = (output as {ref?:unknown}).ref; return typeof ref === "string" ? ref : null;
  }
  return null;
}

export async function completeGraphNode(pool: Pool, lease: Lease, output?: unknown): Promise<void> {
  const client = await pool.connect();
  try {
    await client.query("begin");
    const wf = await lockWorkflow(client, lease.workflowId);
    if (wf.status === "cancelled") return cancelInTransaction(client, lease, "workflow cancelled before completion committed");
    await client.query(`update stepline_node_states set status='succeeded',locked_by=null,locked_until=null,output=$2,output_ref=$3,error=null,error_class=null,updated_at=now() where workflow_id=$1 and node=$4`,
      [lease.workflowId, JSON.stringify(output ?? null), outputRef(output), lease.node]);
    await finishAttempt(client, lease, "succeeded");
    await appendTimelineEvent(client, lease.workflowId, { kind: "succeeded", node: lease.node, attempt: lease.attempt, executionId: lease.executionId, data: { outputRef: outputRef(output), outputSummary: summarizeValue(output) } });
    await unlockDependents(client, lease.workflowId);
    await recomputeWorkflow(client, lease.workflowId);
    await client.query("commit");
  } catch(e) { await client.query("rollback"); throw e; } finally { client.release(); }
}

export async function failGraphNode(pool: Pool, lease: Lease, err: unknown): Promise<void> {
  const errorClass = err instanceof GraphExecutionError ? err.errorClass : "retryable";
  const message = err instanceof Error ? err.message : String(err);
  const client = await pool.connect();
  try {
    await client.query("begin");
    const wf = await lockWorkflow(client, lease.workflowId);
    if (wf.status === "cancelled" || (err instanceof Error && err.name === "AbortError")) return cancelInTransaction(client, lease, message || "aborted");
    const retry = retryConfigFor(lease.def, lease.node);
    await appendTimelineEvent(client, lease.workflowId, { kind: "failed", node: lease.node, attempt: lease.attempt, executionId: lease.executionId, data: { error: message, errorClass } });
    if (errorClass === "paused") {
      await finishAttempt(client, lease, "paused", "paused", message);
      await client.query(`update stepline_node_states set status='paused',error_class='paused',error=$2,locked_by=null,locked_until=null,updated_at=now() where workflow_id=$1 and node=$3`, [lease.workflowId, message, lease.node]);
      await client.query(`update stepline_workflows set status='paused',paused_reason=$2,paused_by='system',updated_at=now() where id=$1`, [lease.workflowId, message]);
      await appendTimelineEvent(client, lease.workflowId, { kind: "paused", node: lease.node, data: { reason: message } });
    } else if (errorClass === "permanent" || lease.attempt >= retry.maxAttempts) {
      await finishAttempt(client, lease, "failed", "permanent", message);
      await client.query(`update stepline_node_states set status='failed',error_class='permanent',error=$2,locked_by=null,locked_until=null,updated_at=now() where workflow_id=$1 and node=$3`, [lease.workflowId, message, lease.node]);
      await cancelUnstarted(client, lease.workflowId, "upstream permanent failure");
      await client.query(`update stepline_workflows set status='failed',error=$2,updated_at=now() where id=$1`, [lease.workflowId, message]);
    } else {
      const jitterMs = retry.jitterMs > 0 ? Math.floor(Math.random() * (retry.jitterMs + 1)) : 0;
      const backoffMs = Math.min(retry.backoffMs * retry.backoffFactor ** (lease.attempt - 1), retry.maxBackoffMs ?? Number.MAX_SAFE_INTEGER) + jitterMs;
      await finishAttempt(client, lease, "failed", "retryable", message, backoffMs, jitterMs);
      await client.query(`update stepline_node_states set status='ready',error_class='retryable',error=$2,run_at=now()+make_interval(secs=>$3::numeric/1000),locked_by=null,locked_until=null,updated_at=now() where workflow_id=$1 and node=$4`, [lease.workflowId, message, Math.round(backoffMs), lease.node]);
      await appendTimelineEvent(client, lease.workflowId, { kind: "retry", node: lease.node, attempt: lease.attempt, executionId: lease.executionId, data: { nextAttempt: lease.attempt + 1, backoffMs, jitterMs, retry } });
    }
    await recomputeWorkflow(client, lease.workflowId);
    await client.query("commit");
  } catch(e) { await client.query("rollback"); throw e; } finally { client.release(); }
}

async function cancelInTransaction(client: PoolClient, lease: Lease, reason: string) {
  await finishAttempt(client, lease, "cancelled", "cancelled", reason);
  await client.query(`update stepline_node_states set status='cancelled',error_class='cancelled',error=$2,locked_by=null,locked_until=null,updated_at=now() where workflow_id=$1 and node=$3`, [lease.workflowId, reason, lease.node]);
  await appendTimelineEvent(client, lease.workflowId, { kind: "cancelled", node: lease.node, attempt: lease.attempt, executionId: lease.executionId, data: { reason } });
  await cancelUnstarted(client, lease.workflowId, reason);
  await client.query(`update stepline_workflows set status='cancelled',updated_at=now() where id=$1`, [lease.workflowId]);
}
async function cancelUnstarted(client: PoolClient, workflowId: string, reason: string) {
  await client.query(`update stepline_node_states set status='cancelled',error_class='cancelled',error=$2,updated_at=now() where workflow_id=$1 and status in ('pending','ready')`, [workflowId, reason]);
}
async function unlockDependents(client: PoolClient, workflowId: string) {
  const { rows } = await client.query(`select node,status,depends_on from stepline_node_states where workflow_id=$1 for update`, [workflowId]);
  const statuses = new Map<string,string>(rows.map((r)=>[r.node,r.status]));
  for (const row of rows) {
    if (row.status !== "pending") continue;
    const deps = row.depends_on as Array<{node:string;onSkipped?:string}>;
    if (!deps.every((d)=>["succeeded","skipped"].includes(statuses.get(d.node) ?? ""))) continue;
    const stop = deps.some((d)=>statuses.get(d.node)==="skipped" && (d.onSkipped ?? "stop")==="stop");
    await client.query(`update stepline_node_states set status=$2,error_class=$3,error=$4,updated_at=now() where workflow_id=$1 and node=$5`, [workflowId, stop?"blocked":"ready", stop?"blocked":null, stop?"upstream optional node was skipped":null, row.node]);
  }
}
async function recomputeWorkflow(client: PoolClient, workflowId: string) {
  const r = await client.query(`select count(*) filter (where status='failed') failed,count(*) filter (where status='blocked') blocked,count(*) filter (where status in ('pending','ready','running','paused')) active from stepline_node_states where workflow_id=$1`, [workflowId]);
  const x = r.rows[0];
  if (Number(x.failed) > 0) await client.query(`update stepline_workflows set status='failed',updated_at=now() where id=$1 and status <> 'cancelled'`, [workflowId]);
  else if (Number(x.active)===0 && Number(x.blocked)===0) await client.query(`update stepline_workflows set status='completed',updated_at=now() where id=$1 and status <> 'cancelled'`, [workflowId]);
  else if (Number(x.active)===0 && Number(x.blocked)>0) await client.query(`update stepline_workflows set status='blocked',updated_at=now() where id=$1`, [workflowId]);
}

function serializeGraph(def: GraphDef) {
  return {
    id: def.id, version: def.version, concurrency: def.concurrency ?? 1, retry: def.retry ?? {},
    nodes: Object.fromEntries(Object.entries(def.nodes).map(([name, node]) => [name, {
      optional: Boolean(node.optional),
      dependsOn: (node.dependsOn ?? []).map(dependencyOf),
      concurrencyKey: node.concurrencyKey ?? null,
      retry: retryConfigFor(def, name),
    }])),
  };
}

export interface LeasedGraphNode {
  workflowId: string; node: string; attempt: number; executionId: string;
  def: GraphDef; controller: AbortController; input: unknown;
}

export async function leaseGraphNode(pool: Pool, workerId: string, opts: GraphWorkerOptions = {}): Promise<LeasedGraphNode | null> {
  const client = await pool.connect();
  try {
    await client.query("begin");
    await client.query("select pg_advisory_xact_lock(982314789)");
    const candidates = await client.query(
      `select n.workflow_id,n.node
       from stepline_node_states n join stepline_workflows w on w.id=n.workflow_id
       where w.status in ('running','paused')
         and (n.status='ready' or (n.status='running' and n.locked_until < now()))
         and (select count(*) from stepline_node_states x where x.workflow_id=n.workflow_id and x.status='running' and x.locked_until>=now()) < w.concurrency
         and (select count(*) from stepline_node_states x where x.status='running' and x.locked_until>=now()) < $1
       order by n.run_at,n.queued_seq limit 50 for update of n skip locked`,
      [opts.maxGlobalConcurrency ?? 4],
    );
    const picked = candidates.rows[0];
    if (!picked) { await client.query("commit"); return null; }
    const executionId = newExecutionId();
    const updated = await client.query(
      `update stepline_node_states set status='running',attempts=attempts+1,
        locked_by=$2,locked_until=now()+make_interval(secs=>$3),updated_at=now(),error=null
       where workflow_id=$1 and node=$4 returning attempts`,
      [picked.workflow_id, workerId, opts.leaseSeconds ?? 30, picked.node],
    );
    const attempt = Number(updated.rows[0].attempts);
    await client.query(
      `insert into stepline_attempts(execution_id,workflow_id,node,attempt,status) values($1,$2,$3,$4,'running')`,
      [executionId, picked.workflow_id, picked.node, attempt]);
    await appendTimelineEvent(client, picked.workflow_id as string, {
      kind: "started", node: picked.node as string, attempt, executionId,
      metadata: { logCorrelation: executionId }, data: { workerId },
    });
    await client.query("commit");
    const wf = await pool.query(`select graph_id,graph_version,input from stepline_workflows where id=$1`, [picked.workflow_id]);
    const def = getGraphDef(wf.rows[0].graph_id as string, Number(wf.rows[0].graph_version));
    return { workflowId: picked.workflow_id, node: picked.node, attempt, executionId, def, controller: new AbortController(), input: wf.rows[0].input };
  } catch (e) { await client.query("rollback"); throw e; }
  finally { client.release(); }
}
