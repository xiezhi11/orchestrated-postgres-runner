/** What a step's `run()` decided to do next. */
export type StepOutcome =
  | { type: "continue"; next: string; output?: unknown }
  /** Schedule `next` to run after `delayMs` — a timer, expressed as a normal transition. */
  | { type: "wait"; next: string; delayMs: number; output?: unknown }
  | { type: "complete"; output?: unknown }
  /** A deliberate business-logic failure. Not retried — use `throw` for transient errors instead. */
  | { type: "fail"; error: string };

export interface StepContext<TInput = unknown> {
  workflowId: string;
  /** The workflow's original input, fixed for its lifetime. */
  input: TInput;
  /** Every prior step's output, keyed by step name. */
  context: Record<string, unknown>;
  /** 1 on first execution; incremented on each thrown-error retry AND on each
   *  re-execution after a worker crash mid-step (a expired lease counts as an
   *  attempt) — both are bounded by the same `retry.max`. */
  attempt: number;
  /** Stable across retries of this logical step — pass to `once()` so a
   *  side-effecting call (send an email) isn't repeated on retry. */
  idempotencyKey: string;
}

export interface RetryPolicy {
  /** Total attempts before the workflow is marked failed. Default 5. */
  max?: number;
  /** Base backoff before the 2nd attempt. Default 1000. */
  backoffMs?: number;
  /** Multiplier applied per subsequent attempt. Default 2 (exponential). */
  backoffFactor?: number;
}

export interface StepDef<TInput = unknown> {
  run: (ctx: StepContext<TInput>) => Promise<StepOutcome>;
  retry?: RetryPolicy;
  /** Wall-clock timeout for one execution of `run()`. Default 30_000. */
  timeoutMs?: number;
}

export interface WorkflowDef<TInput = unknown> {
  type: string;
  start: string;
  steps: Record<string, StepDef<TInput>>;
}

export type WorkflowStatus = GraphWorkflowStatus;
export type GraphWorkflowStatus =
  | "running"
  | "completed"
  | "failed"
  | "cancelled"
  | "paused"
  | "blocked";
export type NodeStatus =
  | "pending"
  | "ready"
  | "running"
  | "succeeded"
  | "failed"
  | "skipped"
  | "blocked"
  | "paused"
  | "cancelled";
export type ErrorClass = "retryable" | "permanent" | "paused";

export interface WorkflowRecord<TInput = unknown> {
  id: string;
  type: string;
  status: WorkflowStatus;
  currentStep: string | null;
  input: TInput;
  context: Record<string, unknown>;
  error: string | null;
  createdAt: Date;
  updatedAt: Date;
}

export interface WorkflowEvent {
  id: number;
  workflowId: string;
  seq: number;
  kind: string;
  step: string | null;
  data: Record<string, unknown>;
  attempt?: number | null;
  executionId?: string | null;
  eventKey?: string | null;
  operator?: string | null;
  metadata?: Record<string, unknown>;
  createdAt: Date;
}

export interface RetryConfig {
  maxAttempts: number;
  backoffMs: number;
  backoffFactor: number;
  maxBackoffMs?: number;
  jitterMs: number;
}

export interface DependencyRule {
  node: string;
  onSkipped?: "stop" | "continue";
}

export interface GraphNodeDef<TInput = unknown> {
  optional?: boolean;
  dependsOn?: Array<string | DependencyRule>;
  concurrencyKey?: string;
  retry?: Partial<RetryConfig>;
  run?: (ctx: GraphStepContext<TInput>) => Promise<unknown | void>;
}

export interface GraphDef<TInput = unknown> {
  id: string;
  version: number;
  nodes: Record<string, GraphNodeDef<TInput>>;
  retry?: Partial<RetryConfig>;
  concurrency?: number;
}

export interface SubmitGraphOptions<TInput = unknown> {
  input: TInput;
  idempotencyKey?: string;
  notify?: Array<{ destination: string; eventKinds?: string[] }>;
}

export interface GraphStepContext<TInput = unknown> {
  workflowId: string;
  node: string;
  attempt: number;
  executionId: string;
  input: TInput;
  inputSummary: unknown;
  outputs: Record<string, unknown>;
  signal: AbortSignal;
}

export interface TimelineEvent extends WorkflowEvent {
  nodeAttempt?: number | null;
}
