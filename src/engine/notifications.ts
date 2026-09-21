import type { Pool } from "pg";

export type NotificationSender = (message: {
  idempotencyKey: string; destination: string; payload: Record<string, unknown>;
}) => Promise<void>;

export interface NotificationDeliveryOptions {
  timeoutMs?: number;
  batchPerWorkflow?: number;
}

/**
 * Delivers pending outbox records. Rows are not locked globally: each workflow
 * is processed in stable (run_at,id) order, while other workflows continue.
 * A poison row increments attempts, records the reason, and backs off, so it
 * cannot block another task's messages.
 */
export async function deliverNotifications(pool: Pool, send: NotificationSender, opts: NotificationDeliveryOptions = {}): Promise<number> {
  const { rows } = await pool.query(
    `select id from stepline_notifications n
     where status='pending' and run_at<=now()
       and id=(select min(s.id) from stepline_notifications s
              where s.workflow_id=n.workflow_id and s.status='pending' and s.run_at<=now())
     order by workflow_id,run_at,id
     limit ${Math.max(1, opts.batchPerWorkflow ?? 16)}`,
  );
  let delivered = 0;
  for (const row of rows) {
    const got = await pool.query(
      `select * from stepline_notifications where id=$1 and status='pending' for update skip locked`,
      [row.id],
    );
    const n = got.rows[0];
    if (!n) continue;
    try {
      await withTimeout(send({ idempotencyKey: n.idempotency_key, destination: n.destination, payload: n.payload }), opts.timeoutMs ?? 5000);
      await pool.query(`update stepline_notifications set status='confirmed',attempts=attempts+1,confirmed_at=now(),last_error=null where id=$1`, [n.id]);
      delivered++;
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      const backoff = Math.min(30_000, 1000 * 2 ** Math.min(4, Number(n.attempts)));
      await pool.query(
        `update stepline_notifications set attempts=attempts+1,last_error=$2,run_at=now()+make_interval(msecs=>$3) where id=$1`,
        [n.id, message, backoff],
      );
    }
  }
  return delivered;
}

function withTimeout<T>(p: Promise<T>, ms: number): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  return Promise.race([p, new Promise<never>((_, reject) => { timer = setTimeout(()=>reject(new Error(`notification timeout after ${ms}ms`)), ms); })])
    .finally(()=>timer && clearTimeout(timer));
}
