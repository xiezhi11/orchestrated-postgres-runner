import { readFile, readdir } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import path from "node:path";
import pg from "pg";

const { Pool } = pg;
export type { Pool as PgPool } from "pg";

const MIGRATIONS_DIR = path.join(
  path.dirname(fileURLToPath(import.meta.url)),
  "..",
  "migrations",
);
const ROLLBACKS_DIR = path.join(MIGRATIONS_DIR, "rollback");

// Arbitrary fixed key for a session-level Postgres advisory lock, held only
// around migrate()'s DDL. Plain `CREATE TABLE IF NOT EXISTS` is not safe
// against two connections migrating at the same instant — Postgres documents
// a race where both pass the existence check before either commits, and the
// implicit pg_type row each CREATE TABLE inserts collides. Any process (or
// test file) calling migrate() concurrently serializes on this lock instead.
const MIGRATION_LOCK_KEY = 869_211_734;

/**
 * Applies every migrations/*.sql file whose number is greater than the
 * currently-recorded schema version, in order, inside one transaction.
 * Safe to call on every process start — a fully-migrated database is a no-op.
 * Safe to call concurrently from multiple processes — see MIGRATION_LOCK_KEY.
 */
export async function migrate(pool: pg.Pool): Promise<void> {
  const client = await pool.connect();
  try {
    await client.query("select pg_advisory_lock($1)", [MIGRATION_LOCK_KEY]);
    try {
      await client.query("begin");
      await client.query(`create table if not exists stepline_schema_meta (version int not null)`);
      let meta = await client.query<{ version: number }>(`select max(version)::int as version from stepline_schema_meta`);
      let current = meta.rows[0]?.version ?? 0;
      if (current === 0) {
        const initialized = await client.query(
          `select to_regclass('public.stepline_workflows') is not null as exists`,
        );
        if (initialized.rows[0]?.exists) {
          await client.query(`insert into stepline_schema_meta(version) values (1)`);
          current = 1;
        }
      }
      const files = (await readdir(MIGRATIONS_DIR))
        .filter((f) => f.endsWith(".sql"))
        .sort();
      for (const file of files) {
        const numberPart = Number(file.slice(0, 3));
        if (Number.isInteger(numberPart) && numberPart <= current) continue;
        const sql = await readFile(path.join(MIGRATIONS_DIR, file), "utf8");
        await client.query(sql);
      }
      meta = await client.query<{ version: number }>(`select max(version)::int as version from stepline_schema_meta`);
      if (!meta.rows[0]?.version) {
        throw new Error("migration did not record a schema version");
      }
      await client.query("commit");
    } catch (err) {
      await client.query("rollback");
      throw err;
    } finally {
      await client.query("select pg_advisory_unlock($1)", [MIGRATION_LOCK_KEY]);
    }
  } finally {
    client.release();
  }
}

/** Applies the rollback script for one numbered migration. Used by CI and ops. */
export async function rollbackMigration(pool: pg.Pool, version: number): Promise<void> {
  const file = path.join(ROLLBACKS_DIR, String(version).padStart(3, "0") + "_dag_down.sql");
  const sql = await readFile(file, "utf8");
  const client = await pool.connect();
  try {
    await client.query("select pg_advisory_lock($1)", [MIGRATION_LOCK_KEY]);
    try {
      await client.query("begin");
      await client.query(sql);
      await client.query("commit");
    } catch (err) {
      await client.query("rollback");
      throw err;
    } finally {
      await client.query("select pg_advisory_unlock($1)", [MIGRATION_LOCK_KEY]);
    }
  } finally {
    client.release();
  }
}

export function createPool(connectionString: string): pg.Pool {
  return new Pool({ connectionString });
}
