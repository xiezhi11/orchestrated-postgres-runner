#!/usr/bin/env node
import { parseArgs } from "node:util";
import { createPool, migrate, rollbackMigration } from "./db.js";
import { getWorkflow, listWorkflowEvents, listWorkflows } from "./engine/query.js";
import { runWorker } from "./engine/worker.js";
import type { WorkflowStatus } from "./types.js";

const HELP = `stepline — durable, Postgres-backed step-graph workflows

USAGE
  stepline migrate                     apply pending schema migrations
  stepline rollback <version>          roll back one numbered migration
  stepline worker                      run a worker (polls forever; Ctrl+C to stop)
  stepline list [--status <s>]         list recent workflows
  stepline status <workflow-id>        show one workflow's current state
  stepline history <workflow-id>       show its full event log

Reads DATABASE_URL from the environment.
`;

async function main(): Promise<void> {
  const [cmd, ...rest] = process.argv.slice(2);
  const url = process.env.DATABASE_URL;
  if (!url) {
    console.error("error: set DATABASE_URL");
    process.exit(2);
  }
  const pool = createPool(url);

  switch (cmd) {
    case "migrate":
      await migrate(pool);
      console.log("migrated");
      break;

    case "rollback": {
      const version = Number(rest[0]);
      if (!Number.isInteger(version)) {
        console.error("usage: stepline rollback <version>");
        process.exit(2);
      }
      await rollbackMigration(pool, version);
      console.log(`rolled back ${version}`);
      break;
    }

    case "worker": {
      await migrate(pool);
      console.log("[stepline] worker starting — Ctrl+C to stop");
      const stop = await runWorker(pool);
      process.on("SIGINT", () => {
        stop();
        void pool.end().then(() => process.exit(0));
      });
      await new Promise(() => {
        /* run until SIGINT */
      });
      break;
    }

    case "list": {
      const { values } = parseArgs({ args: rest, options: { status: { type: "string" } } });
      const rows = await listWorkflows(pool, values.status as WorkflowStatus | undefined);
      for (const w of rows) {
        console.log(`${w.id}  ${w.type.padEnd(24)} ${w.status.padEnd(10)} ${w.currentStep ?? "-"}`);
      }
      await pool.end();
      break;
    }

    case "status": {
      const id = rest[0];
      if (!id) {
        console.error("usage: stepline status <workflow-id>");
        process.exit(2);
      }
      const w = await getWorkflow(pool, id);
      if (!w) {
        console.error("not found");
        process.exit(1);
      }
      console.log(JSON.stringify(w, null, 2));
      await pool.end();
      break;
    }

    case "history": {
      const id = rest[0];
      if (!id) {
        console.error("usage: stepline history <workflow-id>");
        process.exit(2);
      }
      const events = await listWorkflowEvents(pool, id);
      for (const e of events) {
        console.log(
          `${e.seq}  ${e.createdAt.toISOString()}  ${e.kind.padEnd(16)} ${(e.step ?? "").padEnd(16)} ${JSON.stringify(e.data)}`,
        );
      }
      await pool.end();
      break;
    }

    default:
      console.log(HELP);
      await pool.end();
      process.exit(cmd ? 2 : 0);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
