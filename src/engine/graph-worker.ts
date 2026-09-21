import type { Pool } from "pg";
import { completeGraphNode, failGraphNode, leaseGraphNode, type GraphWorkerOptions, type LeasedGraphNode } from "./graph-runtime.js";

export interface GraphNodeRunnerOptions extends GraphWorkerOptions {
  pollIntervalMs?: number;
}

export async function runGraphWorker(pool: Pool, opts: GraphNodeRunnerOptions = {}) {
  const workerId = opts.workerId ?? `graph-worker-${process.pid}`;
  const interval = opts.pollIntervalMs ?? 200;
  const active = new Map<string, LeasedGraphNode>();
  let stopped = false;
  let processing = false;

  const watchCancels = setInterval(async () => {
    for (const lease of active.values()) {
      const r = await pool.query(`select status from stepline_workflows where id=$1`, [lease.workflowId]);
      if (r.rows[0]?.status === "cancelled") lease.controller.abort();
    }
  }, Math.max(50, interval));

  async function tick() {
    if (processing || stopped) return;
    processing = true;
    try {
      for (;;) {
        const lease = await leaseGraphNode(pool, workerId, opts);
        if (!lease) break;
        active.set(`${lease.workflowId}:${lease.node}:${lease.executionId}`, lease);
        void execute(lease);
      }
    } finally { processing = false; }
  }
  async function execute(lease: LeasedGraphNode) {
    const key = `${lease.workflowId}:${lease.node}:${lease.executionId}`;
    try {
      const outputs = await loadOutputs(pool, lease.workflowId);
      const output = await lease.def.nodes[lease.node]?.run?.({
        workflowId: lease.workflowId, node: lease.node, attempt: lease.attempt,
        executionId: lease.executionId, input: lease.input, inputSummary: undefined,
        outputs, signal: lease.controller.signal,
      });
      await completeGraphNode(pool, lease, output);
    } catch (err) { await failGraphNode(pool, lease, err); }
    finally { active.delete(key); }
  }
  async function loop() {
    while (!stopped) { await tick(); await new Promise((r)=>setTimeout(r, interval)); }
  }
  void loop();
  return () => { stopped = true; clearInterval(watchCancels); for (const l of active.values()) l.controller.abort(); };
}

async function loadOutputs(pool: Pool, workflowId: string) {
  const r = await pool.query(`select node,output from stepline_node_states where workflow_id=$1 and status='succeeded'`, [workflowId]);
  return Object.fromEntries(r.rows.map((x)=>[x.node,x.output]));
}
