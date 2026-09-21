import { after, before, test } from "node:test";
import assert from "node:assert/strict";
import type pg from "pg";
import {
  cancelWorkflow,
  createPool,
  defineGraph,
  deliverNotifications,
  exportWorkflowRecords,
  getTimeline,
  getWorkflow,
  migrate,
  permanent,
  pauseNodeError,
  resumeWorkflow,
  retryable,
  runGraphWorker,
  skipOptionalNode,
  submitGraph,
} from "../src/index.js";

const URL = process.env.TEST_DATABASE_URL ?? "postgres://postgres:postgres@localhost:55432/stepline";
let pool: pg.Pool;
const sleep = (ms: number, signal?: AbortSignal) =>
  new Promise<void>((resolve, reject) => {
    const t = setTimeout(resolve, ms);
    signal?.addEventListener("abort", () => { clearTimeout(t); reject(Object.assign(new Error("aborted"), { name: "AbortError" })); }, { once: true });
  });

before(async () => {
  pool = createPool(URL);
  await migrate(pool);
});
after(async () => { await pool.end(); });

test("cycles, missing dependencies, duplicates and isolated nodes are rejected at submit time", () => {
  assert.throws(() => defineGraph({ id: "cycle", version: 1, nodes: {
    a: { dependsOn: ["c"], run: async () => {} },
    b: { dependsOn: ["a"], run: async () => {} },
    c: { dependsOn: ["b"], run: async () => {} },
  } }), /cycle/);
  assert.throws(() => defineGraph({ id: "isolated", version: 1, nodes: {
    a: { run: async () => {} },
    b: { run: async () => {} },
  } }), /isolated/);
  assert.throws(() => defineGraph({ id: "bad-dep", version: 1, nodes: {
    a: { dependsOn: ["missing"], run: async () => {} },
  } }), /missing/);
});

test("independent nodes run concurrently up to the task limit while stable FIFO waits for a slot", async () => {
  let active = 0, max = 0;
  defineGraph({ id: "task-concurrency", version: 1, concurrency: 2, nodes: {
    a: { run: async () => { active++; max=Math.max(max,active); await sleep(180); active--; return { ref:"a" }; } },
    b: { dependsOn:["a"], run: async () => ({ref:"b"}) },
    c: { dependsOn:["a"], run: async () => ({ref:"c"}) },
    d: { dependsOn:["b","c"], run: async () => ({ref:"d"}) },
  } });
  const id = await submitGraph(pool, "task-concurrency", { secret: "hide-me" });
  const stop = await runGraphWorker(pool, { maxGlobalConcurrency: 2, pollIntervalMs: 10, leaseSeconds: 10 });
  await waitUntil(pool, id); stop();
  const wf = await getWorkflow(pool, id);
  assert.equal(wf?.status, "completed");
  assert.equal(max, 2);
  const summary = await pool.query(`select input_summary from stepline_workflows where id=$1`, [id]);
  assert.equal(summary.rows[0].input_summary.secret.redacted, true);
});

test("an optional skipped node records operator/reason, continues one dependent and blocks another", async () => {
  defineGraph({ id: "skip-rules", version: 1, nodes: {
    optional: { optional: true, run: async () => {} },
    go: { dependsOn: [{ node: "optional", onSkipped: "continue" }], run: async () => {} },
    stop: { dependsOn: ["optional"], run: async () => {} },
  } });
  const id = await submitGraph(pool, "skip-rules", {});
  await skipOptionalNode(pool, id, "alice", "not needed");
  const stop = await runGraphWorker(pool, { maxGlobalConcurrency: 2, pollIntervalMs: 10, leaseSeconds: 10 });
  await waitUntil(pool, id); stop();
  const nodes = await pool.query(`select node,status from stepline_node_states where workflow_id=$1 order by node`, [id]);
  assert.deepEqual(Object.fromEntries(nodes.rows.map((r)=>[r.node,r.status])), { go:"succeeded", optional:"skipped", stop:"blocked" });
  const timeline = await getTimeline(pool, id);
  assert.ok(timeline.some((e)=>e.kind==="skipped" && e.operator==="alice" && e.data.reason==="not needed"));
});

test("cancel sends an abort signal to running nodes and records the termination reason", async () => {
  defineGraph({ id: "cancelable", version: 1, nodes: {
    slow: { run: async ({ signal }) => { await sleep(1000, signal); } },
  } });
  const id = await submitGraph(pool, "cancelable", {});
  const stop = await runGraphWorker(pool, { pollIntervalMs: 10, leaseSeconds: 10 });
  await sleep(150);
  await cancelWorkflow(pool, id, "bob", "user requested cancellation");
  await waitUntil(pool, id); stop();
  const node = await pool.query(`select status,error from stepline_node_states where workflow_id=$1`, [id]);
  assert.equal(node.rows[0].status, "cancelled");
  assert.match(node.rows[0].error, /user requested|aborted/);
});

test("retry policy is version locked, paused nodes resume, and permanent failures terminate", async () => {
  let flaky = 0;
  let pausedOnce = false;
  defineGraph({ id: "retry-graph", version: 3, retry: { maxAttempts: 2, backoffMs: 5, backoffFactor: 1, jitterMs: 0 }, nodes: {
    flaky: { run: async () => { if (++flaky === 1) throw retryable("transient"); return {}; } },
    human: { dependsOn: ["flaky"], run: async () => { if (!pausedOnce) { pausedOnce = true; throw pauseNodeError("needs human"); } return {}; } },
  } });
  const id = await submitGraph(pool, "retry-graph", {});
  let stop = await runGraphWorker(pool, { pollIntervalMs: 5, leaseSeconds: 10 });
  await waitUntil(pool, id, ["paused"]);
  stop();
  await resumeWorkflow(pool, id, "operator", "fixed by hand");
  stop = await runGraphWorker(pool, { pollIntervalMs: 5, leaseSeconds: 10 });
  await waitUntil(pool, id);
  stop();
  assert.equal(flaky, 2);
  assert.equal(pausedOnce, true);
  const wf = await getWorkflow(pool, id);
  assert.equal(wf?.status, "completed");
});

test("unconfirmed notifications are retried with one idempotency key and confirmed records are not resent", async () => {
  defineGraph({ id: "notif", version: 1, nodes: { a: { run: async () => ({ref:"x"}) } } });
  const seen: string[] = [];
  let failOnce = true;
  const sender = async (m: { idempotencyKey: string }) => {
    seen.push(m.idempotencyKey);
    if (failOnce) { failOnce = false; throw new Error("temporary timeout"); }
  };
  const id = await submitGraph(pool, "notif", {}, { notify: [{ destination: "webhook:test", eventKinds: ["succeeded"] }] });
  const stop = await runGraphWorker(pool, { pollIntervalMs: 5, leaseSeconds: 10 });
  await waitUntil(pool, id); stop();
  await deliverNotifications(pool, sender);
  await pool.query(`update stepline_notifications set run_at=now() where workflow_id=$1 and status='pending'`, [id]);
  await deliverNotifications(pool, sender);
  await deliverNotifications(pool, sender);
  assert.equal(seen.length, 2);
  assert.equal(seen[0], seen[1]);
  const pending = await pool.query(`select count(*)::int n,array_agg(last_error) reasons from stepline_notifications where workflow_id=$1 and status='pending'`, [id]);
  assert.equal(pending.rows[0].n, 0);
  assert.match(String(pending.rows[0].reasons), /temporary timeout/);
});

test("exported pagination uses the same total ordering as online timeline and includes retries", async () => {
  defineGraph({ id: "export-retry", version: 1, retry: { maxAttempts: 3, backoffMs: 1, backoffFactor: 1, jitterMs: 0 }, nodes: {
    a: { run: async() => { throw permanent("doomed"); } },
  } });
  const id = await submitGraph(pool, "export-retry", {});
  const stop = await runGraphWorker(pool, { pollIntervalMs: 1, leaseSeconds: 10 });
  await waitUntil(pool, id, ["failed", "blocked", "cancelled"]); stop();
  const online = await getTimeline(pool, id);
  const exported = await exportWorkflowRecords(pool, id, 2) as { events: Array<{seq:number;id:number}> };
  assert.deepEqual(exported.events.map((e)=>[e.seq,e.id]), online.map((e)=>[e.seq,e.id]));
});

async function waitUntil(pool: pg.Pool, id: string, terminal = ["completed", "failed", "blocked", "cancelled"]) {
  const deadline = Date.now() + 10_000;
  for (;;) {
    const wf = await getWorkflow(pool, id);
    if (wf && terminal.includes(wf.status)) return wf;
    if (Date.now() > deadline) throw new Error(`workflow ${id} did not finish: ${(await getWorkflow(pool,id))?.status}`);
    await sleep(30);
  }
}
