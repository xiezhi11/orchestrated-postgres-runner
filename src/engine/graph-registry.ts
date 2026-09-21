import type { DependencyRule, GraphDef, RetryConfig } from "../types.js";

const graphRegistry = new Map<string, Map<number, GraphDef>>();

export const DEFAULT_RETRY: RetryConfig = {
  maxAttempts: 5,
  backoffMs: 1000,
  backoffFactor: 2,
  jitterMs: 0,
};

export function defineGraph<TInput>(def: GraphDef<TInput>): GraphDef<TInput> {
  validateGraph(def as GraphDef<unknown>);
  let versions = graphRegistry.get(def.id);
  if (!versions) {
    versions = new Map();
    graphRegistry.set(def.id, versions);
  }
  versions.set(def.version, def as GraphDef<unknown>);
  return def;
}

export function getGraphDef<TInput = unknown>(id: string, version: number): GraphDef<TInput> {
  const def = graphRegistry.get(id)?.get(version);
  if (!def) throw new Error(`graph ${id}@${version} is not registered in this process`);
  return def as GraphDef<TInput>;
}

export function listRegisteredGraphs(): Array<{ id: string; version: number }> {
  return [...graphRegistry.entries()].flatMap(([id, versions]) =>
    [...versions.keys()].map((version) => ({ id, version })),
  );
}

export function dependencyOf(edge: string | DependencyRule): DependencyRule {
  return typeof edge === "string" ? { node: edge, onSkipped: "stop" } : edge;
}

export function retryConfigFor(def: GraphDef, node: string): RetryConfig {
  return { ...DEFAULT_RETRY, ...def.retry, ...def.nodes[node]?.retry };
}

/** Validation happens before any workflow row is inserted. */
export function validateGraph(def: GraphDef): void {
  const errors: string[] = [];
  if (!def.id.trim()) errors.push("graph id is required");
  if (!Number.isInteger(def.version) || def.version < 1) errors.push("graph version must be a positive integer");
  const names = Object.keys(def.nodes);
  if (names.length === 0) errors.push("graph must contain at least one node");
  if (new Set(names).size !== names.length) errors.push("duplicate node ids are not allowed");

  for (const [name, node] of Object.entries(def.nodes)) {
    const seen = new Set<string>();
    for (const edge of node.dependsOn ?? []) {
      const dep = dependencyOf(edge);
      if (seen.has(dep.node)) errors.push(`node ${name} has duplicate dependency ${dep.node}`);
      seen.add(dep.node);
      if (!def.nodes[dep.node]) errors.push(`node ${name} depends on missing node ${dep.node}`);
      if (dep.node === name) errors.push(`node ${name} cannot depend on itself`);
      if (dep.onSkipped && !["stop", "continue"].includes(dep.onSkipped)) {
        errors.push(`node ${name} has invalid onSkipped rule for ${dep.node}`);
      }
    }
  }
  if (names.some((name) => hasCycle(def, name))) errors.push("graph contains a cycle");
  if (names.length > 0 && hasIsolatedNode(def)) errors.push("isolated nodes are not allowed");
  if (errors.length) throw new Error(`invalid graph ${def.id}: ${errors.join("; ")}`);
}

function hasCycle(def: GraphDef, start: string): boolean {
  const active = new Set<string>();
  const done = new Set<string>();
  const visit = (node: string): boolean => {
    if (active.has(node)) return true;
    if (done.has(node)) return false;
    active.add(node);
    for (const edge of def.nodes[node]?.dependsOn ?? []) {
      if (visit(dependencyOf(edge).node)) return true;
    }
    active.delete(node);
    done.add(node);
    return false;
  };
  return visit(start);
}

function hasIsolatedNode(def: GraphDef): boolean {
  if (Object.keys(def.nodes).length === 1) return false;
  const undirected = new Map<string, Set<string>>();
  for (const [node, spec] of Object.entries(def.nodes)) {
    for (const edge of spec.dependsOn ?? []) {
      const dep = dependencyOf(edge).node;
      if (!undirected.has(node)) undirected.set(node, new Set());
      if (!undirected.has(dep)) undirected.set(dep, new Set());
      undirected.get(node)!.add(dep);
      undirected.get(dep)!.add(node);
    }
  }
  const start = Object.keys(def.nodes)[0]!;
  const seen = new Set<string>([start]);
  const stack = [start];
  while (stack.length) {
    const node = stack.pop()!;
    for (const next of undirected.get(node) ?? []) {
      if (!seen.has(next)) {
        seen.add(next);
        stack.push(next);
      }
    }
  }
  return Object.keys(def.nodes).some((node) => !seen.has(node));
}

const SENSITIVE = /(password|passwd|secret|token|authorization|api[-_]?key|private[-_]?key|credential)/i;

export function summarizeValue(value: unknown, depth = 0): unknown {
  if (value == null) return value;
  if (typeof value === "string") return { type: "string", length: value.length, sha256Hint: hashLike(value) };
  if (typeof value === "number" || typeof value === "boolean") return { type: typeof value };
  if (Array.isArray(value)) return { type: "array", length: value.length };
  if (typeof value === "object") {
    if (depth >= 2) return { type: "object", truncated: true };
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>).map(([key, child]) => [
        key,
        SENSITIVE.test(key)
          ? { redacted: true, type: Array.isArray(child) ? "array" : typeof child }
          : summarizeValue(child, depth + 1),
      ]),
    );
  }
  return { type: typeof value };
}

function hashLike(value: string): string {
  let hash = 5381;
  for (const ch of value) hash = ((hash << 5) + hash + ch.charCodeAt(0)) | 0;
  return (hash >>> 0).toString(16).padStart(8, "0");
}
