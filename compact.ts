#!/usr/bin/env bun
/**
 * pg_compact — Resumable, interruptible PostgreSQL table compaction.
 *
 * Reclaims bloat (dead tuples, dropped columns) by building a fresh copy of
 * each table, then swapping it in with a sub-second ACCESS EXCLUSIVE lock.
 *
 * Unlike pg_repack this tool:
 *   - Only holds ACCESS EXCLUSIVE for the brief final swap (typically < 1 s).
 *   - Is fully resumable — interrupt with Ctrl-C and pick up where you left off.
 *   - Stores all progress in a _compact schema you can query from psql.
 *   - Cleans up completely on abort.
 *
 * Requires: bun >= 1.2  (no npm packages — uses Bun's built-in SQL driver)
 *
 * Usage:
 *   pg_compact.ts start   <table> [--batch N] [--schema S]
 *   pg_compact.ts resume  <table> [--batch N] [--schema S]
 *   pg_compact.ts status  [table] [--schema S]
 *   pg_compact.ts swap    <table> [--lock-timeout MS] [--schema S]
 *   pg_compact.ts abort   <table> [--schema S]   (cancel in-progress job)
 *   pg_compact.ts abort-all
 *
 * Environment:
 *   DATABASE_URL  or POSTGRES_URL  or pass --dsn
 */

import { SQL } from "bun";

// ─── Constants ───────────────────────────────────────────────────────────────

const COMPACT_SCHEMA = "_compact";
const DEFAULT_BATCH = 50_000;
const DEFAULT_LOCK_TIMEOUT = 5_000;
const LOG_EVERY_MS = 5_000;

// ─── Interrupt handling ──────────────────────────────────────────────────────

let interrupted = false;

process.on("SIGINT", () => {
  interrupted = true;
  warn("Interrupt received — finishing current batch then stopping.");
});
process.on("SIGTERM", () => {
  interrupted = true;
  warn("Interrupt received — finishing current batch then stopping.");
});

// ─── Logging ─────────────────────────────────────────────────────────────────

const BOLD = "\x1b[1m";
const GREEN = "\x1b[32m";
const YELLOW = "\x1b[33m";
const RED = "\x1b[31m";
const DIM = "\x1b[2m";
const RESET = "\x1b[0m";

function ts(): string {
  return new Date().toTimeString().slice(0, 8);
}
function info(...args: unknown[]) {
  console.log(`${DIM}${ts()}${RESET} ${GREEN}INFO${RESET} `, ...args);
}
function warn(...args: unknown[]) {
  console.error(`${DIM}${ts()}${RESET} ${YELLOW}WARN${RESET} `, ...args);
}
function err(...args: unknown[]) {
  console.error(`${DIM}${ts()}${RESET} ${RED}ERROR${RESET}`, ...args);
}
function die(msg: string): never {
  err(msg);
  process.exit(1);
}

// ─── SQL helpers ─────────────────────────────────────────────────────────────

let db: InstanceType<typeof SQL>;

function connect(dsn: string) {
  db = new SQL(dsn || undefined);
}

/** Quote a SQL identifier. */
function q(id: string): string {
  return '"' + id.replace(/"/g, '""') + '"';
}

/** Quote a SQL literal. null → NULL. */
function lit(val: unknown): string {
  if (val === null || val === undefined) return "NULL";
  return "'" + String(val).replace(/'/g, "''") + "'";
}

/** Fully-qualified name. */
function fqn(schema: string, table: string): string {
  return `${q(schema)}.${q(table)}`;
}

/** Row-value constructor for column references: (col1, col2, ...) */
function pkTuple(pkCols: string[]): string {
  return `(${pkCols.map(q).join(", ")})`;
}

/** Row-value constructor for literal values: ('v1', 'v2', ...) */
function pkLit(vals: string[]): string {
  return `(${vals.map(lit).join(", ")})`;
}

/** Serialize compound PK values for storage in last_copied_id. */
function serializePk(vals: string[]): string {
  return JSON.stringify(vals);
}

/** Deserialize compound PK values from last_copied_id. */
function deserializePk(s: string | null): string[] | null {
  if (s === null) return null;
  return JSON.parse(s);
}

/** Run arbitrary SQL, return rows as plain objects. */
async function query<T = Record<string, unknown>>(sqlText: string): Promise<T[]> {
  return (await db.unsafe(sqlText)) as T[];
}

/** Run SQL, return the first row or null. */
async function queryOne<T = Record<string, unknown>>(sqlText: string): Promise<T | null> {
  const rows = await query<T>(sqlText);
  return rows.length > 0 ? rows[0] : null;
}

/** Run SQL, return a single scalar value. */
async function scalar<T = string>(sqlText: string): Promise<T | null> {
  const row = await queryOne<Record<string, T>>(sqlText);
  if (!row) return null;
  const keys = Object.keys(row);
  return keys.length > 0 ? row[keys[0]] : null;
}

/** Execute DML/DDL. */
async function exec(sqlText: string): Promise<void> {
  await db.unsafe(sqlText);
}

// ─── Schema / state management ───────────────────────────────────────────────

interface Job {
  source_schema: string;
  source_table: string;
  shadow_table: string;
  log_table: string;
  phase: string;
  last_copied_id: string | null;
  rows_copied: number;
  replay_seq: number;
  batch_size: number;
  started_at: string;
  updated_at: string;
}

async function ensureSchema() {
  await exec(`CREATE SCHEMA IF NOT EXISTS ${q(COMPACT_SCHEMA)}`);
  await exec(`
    CREATE TABLE IF NOT EXISTS ${q(COMPACT_SCHEMA)}.jobs (
      source_schema   text        NOT NULL,
      source_table    text        NOT NULL,
      shadow_table    text        NOT NULL,
      log_table       text        NOT NULL,
      phase           text        NOT NULL DEFAULT 'init',
      last_copied_id  text,
      rows_copied     bigint      NOT NULL DEFAULT 0,
      replay_seq      bigint      NOT NULL DEFAULT 0,
      batch_size      int         NOT NULL DEFAULT ${DEFAULT_BATCH},
      started_at      timestamptz NOT NULL DEFAULT now(),
      updated_at      timestamptz NOT NULL DEFAULT now(),
      PRIMARY KEY (source_schema, source_table)
    )
  `);
}

async function getJob(schema: string, table: string): Promise<Job | null> {
  const row = await queryOne<any>(`
    SELECT * FROM ${q(COMPACT_SCHEMA)}.jobs
    WHERE source_schema = ${lit(schema)} AND source_table = ${lit(table)}
  `);
  if (!row) return null;
  row.rows_copied = Number(row.rows_copied);
  row.replay_seq = Number(row.replay_seq);
  row.batch_size = Number(row.batch_size);
  return row as Job;
}

async function getAllJobs(): Promise<Job[]> {
  const rows = await query<any>(
    `SELECT * FROM ${q(COMPACT_SCHEMA)}.jobs ORDER BY started_at`
  );
  for (const r of rows) {
    r.rows_copied = Number(r.rows_copied);
    r.replay_seq = Number(r.replay_seq);
    r.batch_size = Number(r.batch_size);
  }
  return rows as Job[];
}

async function saveJob(schema: string, table: string, fields: Record<string, unknown>) {
  const sets = ["updated_at = now()"];
  for (const [k, v] of Object.entries(fields)) {
    sets.push(`${k} = ${lit(v)}`);
  }
  await exec(`
    UPDATE ${q(COMPACT_SCHEMA)}.jobs SET ${sets.join(", ")}
    WHERE source_schema = ${lit(schema)} AND source_table = ${lit(table)}
  `);
}

async function insertJob(
  schema: string, table: string, shadow: string, logTable: string, batchSize: number
) {
  await exec(`
    INSERT INTO ${q(COMPACT_SCHEMA)}.jobs
      (source_schema, source_table, shadow_table, log_table, batch_size)
    VALUES (${lit(schema)}, ${lit(table)}, ${lit(shadow)}, ${lit(logTable)}, ${batchSize})
    ON CONFLICT (source_schema, source_table) DO NOTHING
  `);
  const existing = await getJob(schema, table);
  if (existing && existing.phase !== "init") {
    die(
      `A compaction job already exists for ${schema}.${table} (phase: ${existing.phase}). ` +
        `Use 'resume', 'status', or 'abort' first.`
    );
  }
}

// ─── Introspection ───────────────────────────────────────────────────────────

async function getPkColumns(schema: string, table: string): Promise<string[]> {
  const r = await query<{ attname: string }>(`
    SELECT a.attname
    FROM pg_index i
    JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
    WHERE i.indrelid = '${fqn(schema, table)}'::regclass AND i.indisprimary
    ORDER BY array_position(i.indkey, a.attnum)
  `);
  const cols = r.map((row) => row.attname);
  if (cols.length === 0) die(`Table ${schema}.${table} has no PRIMARY KEY.`);
  return cols;
}

async function getColumns(schema: string, table: string): Promise<string> {
  const val = await scalar<string>(`
    SELECT string_agg(quote_ident(attname), ', ' ORDER BY attnum)
    FROM pg_attribute
    WHERE attrelid = '${fqn(schema, table)}'::regclass
      AND attnum > 0 AND NOT attisdropped
  `);
  return val || "";
}

interface IndexDef {
  relname: string;
  indexdef: string;
  indisprimary: boolean;
}

async function getIndexDefs(schema: string, table: string): Promise<IndexDef[]> {
  return await query<IndexDef>(`
    SELECT c2.relname, pg_get_indexdef(i.indexrelid) AS indexdef, i.indisprimary
    FROM pg_index i
    JOIN pg_class c2 ON c2.oid = i.indexrelid
    WHERE i.indrelid = '${fqn(schema, table)}'::regclass
  `);
}

async function getTableSize(schema: string, table: string): Promise<number> {
  const v = await scalar<string>(
    `SELECT pg_total_relation_size('${fqn(schema, table)}'::regclass)`
  );
  return v ? Number(v) : 0;
}

async function getTableSizePretty(schema: string, table: string): Promise<string> {
  return (
    (await scalar<string>(
      `SELECT pg_size_pretty(pg_total_relation_size('${fqn(schema, table)}'::regclass))`
    )) || "0 bytes"
  );
}

async function getRowEstimate(schema: string, table: string): Promise<number> {
  const v = await scalar<string>(
    `SELECT reltuples::bigint FROM pg_class WHERE oid = '${fqn(schema, table)}'::regclass`
  );
  return v ? Number(v) : 0;
}

// ─── Core operations ─────────────────────────────────────────────────────────

async function createShadow(schema: string, table: string, shadow: string) {
  const src = fqn(schema, table);
  const dst = fqn(COMPACT_SCHEMA, shadow);
  await exec(`DROP TABLE IF EXISTS ${dst}`);
  await exec(`
    CREATE TABLE ${dst}
    (LIKE ${src} INCLUDING DEFAULTS INCLUDING CONSTRAINTS
     INCLUDING GENERATED INCLUDING STORAGE)
  `);
  info(`Created shadow table ${COMPACT_SCHEMA}.${shadow}`);
}

async function createChangeLog(logTable: string, pkCols: string[]) {
  const tbl = fqn(COMPACT_SCHEMA, logTable);
  const pkColDefs = pkCols.map((c) => `${q(c)} text NOT NULL`).join(", ");
  const idxCols = pkCols.map(q).join(", ");
  await exec(`DROP TABLE IF EXISTS ${tbl}`);
  await exec(`
    CREATE TABLE ${tbl} (
      seq bigserial PRIMARY KEY,
      op  char(1) NOT NULL,
      ${pkColDefs}
    )
  `);
  await exec(`CREATE INDEX ON ${tbl} (${idxCols})`);
  info(`Created change log ${COMPACT_SCHEMA}.${logTable}`);
}

async function installTrigger(
  schema: string, table: string, logTable: string, pkCols: string[]
) {
  const src = fqn(schema, table);
  const logTbl = fqn(COMPACT_SCHEMA, logTable);
  const funcName = fqn(COMPACT_SCHEMA, `trig_${table}`);
  const trigName = q(`_compact_cdc_${table}`);

  const oldRefs = pkCols.map((c) => `OLD.${q(c)}`).join(", ");
  const newRefs = pkCols.map((c) => `NEW.${q(c)}`).join(", ");
  const colList = pkCols.map(q).join(", ");

  await exec(`DROP TRIGGER IF EXISTS ${trigName} ON ${src}`);
  await exec(`
    CREATE OR REPLACE FUNCTION ${funcName}() RETURNS trigger
    LANGUAGE plpgsql AS $_pg_compact_trig_$
    BEGIN
      IF TG_OP = 'DELETE' THEN
        INSERT INTO ${logTbl} (op, ${colList}) VALUES ('D', ${oldRefs});
        RETURN OLD;
      ELSE
        INSERT INTO ${logTbl} (op, ${colList}) VALUES ('U', ${newRefs});
        RETURN NEW;
      END IF;
    END;
    $_pg_compact_trig_$
  `);
  await exec(`
    CREATE TRIGGER ${trigName}
    AFTER INSERT OR UPDATE OR DELETE ON ${src}
    FOR EACH ROW EXECUTE FUNCTION ${funcName}()
  `);
  info(`Installed CDC trigger on ${schema}.${table}`);
}

interface BulkCopyResult {
  lastPk: string[] | null;
  total: number;
  finished: boolean;
}

async function bulkCopy(
  schema: string, table: string, shadow: string, pkCols: string[],
  batchSize: number, lastPk: string[] | null, rowsSoFar: number
): Promise<BulkCopyResult> {
  const src = fqn(schema, table);
  const dst = fqn(COMPACT_SCHEMA, shadow);
  const cols = await getColumns(schema, table);
  const estRows = await getRowEstimate(schema, table);
  const pk = pkTuple(pkCols);
  const pkSelect = pkCols.map(q).join(", ");
  const pkOrder = pkCols.map(q).join(", ");

  let total = rowsSoFar;
  let last = lastPk;
  let lastLogTime = Date.now();

  while (!interrupted) {
    const whereClause = last === null ? "" : `WHERE ${pk} > ${pkLit(last)}`;

    // Find the boundary PK of this batch (the last row in PK order)
    const maxRow = await queryOne<Record<string, string | null>>(`
      SELECT ${pkSelect} FROM (
        SELECT ${pkSelect} FROM ${src}
        ${whereClause}
        ORDER BY ${pkOrder}
        LIMIT ${batchSize}
      ) t
      ORDER BY ${pkCols.map(c => `${q(c)} DESC`).join(", ")}
      LIMIT 1
    `);
    const batchMaxPk = maxRow ? pkCols.map(c => maxRow[c] ?? "") : null;

    if (batchMaxPk === null) {
      await saveJob(schema, table, {
        phase: "copied", last_copied_id: last === null ? null : serializePk(last), rows_copied: total,
      });
      info(`Bulk copy complete: ${total.toLocaleString()} rows copied.`);
      return { lastPk: last, total, finished: true };
    }

    // Copy the batch in a transaction with progress update
    const lowerBound = last === null
      ? ""
      : `${pk} > ${pkLit(last)} AND`;

    await db.begin(async (tx) => {
      await tx.unsafe(`
        INSERT INTO ${dst} (${cols})
        SELECT ${cols} FROM ${src}
        WHERE ${lowerBound} ${pk} <= ${pkLit(batchMaxPk)}
        ORDER BY ${pkOrder}
      `);

      // Count inserted
      const countRows = await tx.unsafe(
        `SELECT count(*) AS n FROM ${dst}
         WHERE ${last === null ? "true" : `${pk} > ${pkLit(last)}`}
           AND ${pk} <= ${pkLit(batchMaxPk)}`
      );
      const inserted = Number((countRows[0] as any)?.n ?? 0);
      total += inserted;
      last = batchMaxPk;

      await tx.unsafe(`
        UPDATE ${q(COMPACT_SCHEMA)}.jobs
        SET last_copied_id = ${lit(serializePk(last))}, rows_copied = ${total}, updated_at = now()
        WHERE source_schema = ${lit(schema)} AND source_table = ${lit(table)}
      `);
    });

    const now = Date.now();
    if (now - lastLogTime >= LOG_EVERY_MS) {
      const pct = estRows > 0 ? ((total / estRows) * 100).toFixed(1) : "?";
      info(`  copied ${total.toLocaleString()} rows (${pct}%) — last pk: ${trunc(last.join(", "), 40)}`);
      lastLogTime = now;
    }
  }

  warn(`Stopped after ${total.toLocaleString()} rows. Resume with: pg_compact.ts resume ${table}`);
  return { lastPk: last, total, finished: false };
}

async function createIndexes(schema: string, table: string, shadow: string) {
  const dstFqn = fqn(COMPACT_SCHEMA, shadow);
  const indexDefs = await getIndexDefs(schema, table);

  info(`Creating ${indexDefs.length} index(es) on shadow table...`);

  for (const idx of indexDefs) {
    let newDef = idx.indexdef.replace(/\bON\s+\S+\s+USING/, `ON ${dstFqn} USING`);

    const newIdxName = `_compact_${idx.relname}`;
    newDef = newDef.replace(
      /CREATE\s+(UNIQUE\s+)?INDEX\s+\S+/,
      (_, unique: string | undefined) => `CREATE ${unique || ""}INDEX ${q(newIdxName)}`
    );

    const t0 = Date.now();
    info(`  building: ${newIdxName} ...`);
    await exec(newDef);

    if (idx.indisprimary) {
      await exec(`ALTER TABLE ${dstFqn} ADD PRIMARY KEY USING INDEX ${q(newIdxName)}`);
    }

    const elapsed = ((Date.now() - t0) / 1000).toFixed(1);
    info(`  done in ${elapsed}s: ${newIdxName}`);
  }

  await saveJob(schema, table, { phase: "indexed" });
  info("All indexes created.");
}

interface ReplayResult {
  seq: number;
  replayed: number;
}

async function replayChanges(
  schema: string, table: string, shadow: string, logTable: string,
  pkCols: string[], fromSeq: number
): Promise<ReplayResult> {
  const src = fqn(schema, table);
  const dst = fqn(COMPACT_SCHEMA, shadow);
  const logTbl = fqn(COMPACT_SCHEMA, logTable);
  const cols = await getColumns(schema, table);
  const pk = pkTuple(pkCols);
  const pkSelect = pkCols.map(q).join(", ");
  const REPLAY_BATCH = 10_000;

  let totalReplayed = 0;
  let currentSeq = fromSeq;

  while (true) {
    const changes = await query<{ seq: string; op: string; [key: string]: string }>(`
      SELECT seq, op, ${pkSelect}
      FROM ${logTbl}
      WHERE seq > ${currentSeq}
      ORDER BY seq
      LIMIT ${REPLAY_BATCH}
    `);

    if (changes.length === 0) {
      return { seq: currentSeq, replayed: totalReplayed };
    }

    const maxSeq = Number(changes[changes.length - 1].seq);

    // Deduplicate: for each composite PK, only the latest operation matters
    const latest = new Map<string, { op: string; vals: string[] }>();
    for (const row of changes) {
      const vals = pkCols.map(c => row[c]);
      const key = vals.join("\0");
      latest.set(key, { op: row.op.trim(), vals });
    }

    const allEntries = [...latest.values()];
    const upsertEntries = allEntries.filter(e => e.op !== "D");

    await db.begin(async (tx) => {
      if (allEntries.length > 0) {
        const idList = allEntries.map(e => pkLit(e.vals)).join(", ");
        await tx.unsafe(`DELETE FROM ${dst} WHERE ${pk} IN (${idList})`);
      }

      if (upsertEntries.length > 0) {
        const idList = upsertEntries.map(e => pkLit(e.vals)).join(", ");
        await tx.unsafe(`
          INSERT INTO ${dst} (${cols})
          SELECT ${cols} FROM ${src}
          WHERE ${pk} IN (${idList})
        `);
      }

      await tx.unsafe(`DELETE FROM ${logTbl} WHERE seq <= ${maxSeq}`);
      await tx.unsafe(`
        UPDATE ${q(COMPACT_SCHEMA)}.jobs
        SET replay_seq = ${maxSeq}, updated_at = now()
        WHERE source_schema = ${lit(schema)} AND source_table = ${lit(table)}
      `);
    });

    totalReplayed += latest.size;
    currentSeq = maxSeq;
  }
}

async function countPendingChanges(logTable: string, fromSeq: number): Promise<number> {
  const v = await scalar<string>(
    `SELECT count(*) FROM ${fqn(COMPACT_SCHEMA, logTable)} WHERE seq > ${fromSeq}`
  );
  return v ? Number(v) : 0;
}

async function doSwap(
  schema: string, table: string, shadow: string, logTable: string,
  pkCols: string[], replaySeq: number, lockTimeoutMs: number
): Promise<boolean> {
  const src = fqn(schema, table);
  const dst = fqn(COMPACT_SCHEMA, shadow);
  const logTbl = fqn(COMPACT_SCHEMA, logTable);
  const cols = await getColumns(schema, table);
  const trigName = q(`_compact_cdc_${table}`);
  const oldName = `${table}__pre_compact`;
  const pk = pkTuple(pkCols);
  const pkSelect = pkCols.map(q).join(", ");

  // Collect index names from both tables before the swap
  const srcIndexes = await getIndexDefs(schema, table);
  const shadowIndexes = await getIndexDefs(COMPACT_SCHEMA, shadow);

  // Pre-swap catch-up (no lock)
  info("Pre-swap catch-up replay...");
  let { seq, replayed } = await replayChanges(
    schema, table, shadow, logTable, pkCols, replaySeq
  );
  let remaining = await countPendingChanges(logTable, seq);
  info(`  replayed ${replayed.toLocaleString()} changes, ${remaining.toLocaleString()} remain.`);

  if (remaining > 10_000) {
    warn(`Still ${remaining.toLocaleString()} pending changes — run 'resume' to catch up more.`);
    return false;
  }

  // ── The critical section: brief exclusive lock ──
  info(`Acquiring ACCESS EXCLUSIVE lock (timeout ${lockTimeoutMs} ms)...`);

  try {
    await db.begin(async (tx) => {
      await tx.unsafe(`SET LOCAL lock_timeout = '${lockTimeoutMs}ms'`);
      await tx.unsafe(`LOCK TABLE ${src} IN ACCESS EXCLUSIVE MODE`);
      const t0 = Date.now();

      // Final replay under lock
      const finalChanges = await tx.unsafe(
        `SELECT seq, op, ${pkSelect}
         FROM ${logTbl}
         WHERE seq > ${seq}
         ORDER BY seq`
      ) as { seq: string; op: string; [key: string]: string }[];

      if (finalChanges.length > 0) {
        const latest = new Map<string, { op: string; vals: string[] }>();
        for (const row of finalChanges) {
          const vals = pkCols.map(c => row[c]);
          const key = vals.join("\0");
          latest.set(key, { op: row.op.trim(), vals });
        }

        const allEntries = [...latest.values()];
        const upsertEntries = allEntries.filter(e => e.op !== "D");

        if (allEntries.length > 0) {
          const idList = allEntries.map(e => pkLit(e.vals)).join(", ");
          await tx.unsafe(`DELETE FROM ${dst} WHERE ${pk} IN (${idList})`);
        }
        if (upsertEntries.length > 0) {
          const idList = upsertEntries.map(e => pkLit(e.vals)).join(", ");
          await tx.unsafe(`
            INSERT INTO ${dst} (${cols})
            SELECT ${cols} FROM ${src}
            WHERE ${pk} IN (${idList})
          `);
        }

        info(`  replayed ${latest.size} final changes under lock.`);
      }

      // Drop trigger, swap tables
      await tx.unsafe(`DROP TRIGGER IF EXISTS ${trigName} ON ${src}`);
      await tx.unsafe(`ALTER TABLE ${src} RENAME TO ${q(oldName)}`);
      await tx.unsafe(`ALTER TABLE ${dst} SET SCHEMA ${q(schema)}`);
      await tx.unsafe(`ALTER TABLE ${fqn(schema, shadow)} RENAME TO ${q(table)}`);

      // Rename indexes: first move old indexes out of the way, then give
      // the new indexes their original names.
      for (const idx of srcIndexes) {
        await tx.unsafe(
          `ALTER INDEX ${fqn(schema, idx.relname)} RENAME TO ${q(idx.relname + "__pre_compact")}`
        );
      }
      for (const idx of shadowIndexes) {
        // Find the original name by stripping the _compact_ prefix
        const origName = idx.relname.replace(/^_compact_/, "");
        if (origName !== idx.relname) {
          await tx.unsafe(
            `ALTER INDEX ${fqn(schema, idx.relname)} RENAME TO ${q(origName)}`
          );
        }
      }

      const elapsedMs = Date.now() - t0;
      info(`  swap complete — lock held for ${elapsedMs} ms.`);
    });
  } catch (e: any) {
    const msg = e?.message || String(e);
    if (msg.includes("lock timeout") || msg.includes("55P03")) {
      err(
        `Could not acquire lock within ${lockTimeoutMs} ms. ` +
          `The table is busy. Try again or increase --lock-timeout.`
      );
      return false;
    }
    throw e;
  }

  // Clean up CDC artifacts (log table, trigger function, job row)
  try { await exec(`DROP TABLE IF EXISTS ${logTbl}`); } catch {}
  try { await exec(`DROP FUNCTION IF EXISTS ${fqn(COMPACT_SCHEMA, `trig_${table}`)}()`); } catch {}
  await exec(`
    DELETE FROM ${q(COMPACT_SCHEMA)}.jobs
    WHERE source_schema = ${lit(schema)} AND source_table = ${lit(table)}
  `);

  info("");
  info(`${BOLD}SUCCESS.${RESET} Old table kept as ${schema}.${oldName}`);
  info(`  Verify, then drop it:  DROP TABLE ${fqn(schema, oldName)};`);
  return true;
}

async function doAbort(schema: string, table: string) {
  const job = await getJob(schema, table);
  if (!job) {
    info(`No job found for ${schema}.${table} — nothing to do.`);
    return;
  }

  const src = fqn(schema, table);
  const trigName = q(`_compact_cdc_${table}`);

  try { await exec(`DROP TRIGGER IF EXISTS ${trigName} ON ${src}`); } catch {}
  try { await exec(`DROP TABLE IF EXISTS ${fqn(COMPACT_SCHEMA, job.shadow_table)}`); } catch {}
  try { await exec(`DROP TABLE IF EXISTS ${fqn(COMPACT_SCHEMA, job.log_table)}`); } catch {}
  try { await exec(`DROP FUNCTION IF EXISTS ${fqn(COMPACT_SCHEMA, `trig_${table}`)}()`); } catch {}
  await exec(`
    DELETE FROM ${q(COMPACT_SCHEMA)}.jobs
    WHERE source_schema = ${lit(schema)} AND source_table = ${lit(table)}
  `);

  info(`Cleaned up compaction job for ${schema}.${table}`);
}

// ─── CLI commands ────────────────────────────────────────────────────────────

async function cmdStart(schema: string, table: string, batchSize: number) {
  await ensureSchema();

  if (await getJob(schema, table)) {
    die(
      `Job already exists for ${schema}.${table}. ` +
        `Use 'resume' to continue, or 'abort' to start over.`
    );
  }

  const pkCols = await getPkColumns(schema, table);
  const shadow = `${table}__new`;
  const logTable = `${table}__log`;

  info(`Starting compaction of ${schema}.${table}`);
  info(`  PK column(s):    ${pkCols.join(", ")}`);
  info(`  Estimated rows:  ${(await getRowEstimate(schema, table)).toLocaleString()}`);
  info(`  Table size:      ${await getTableSizePretty(schema, table)}`);
  info(`  Batch size:      ${batchSize.toLocaleString()}`);

  await createShadow(schema, table, shadow);
  await createChangeLog(logTable, pkCols);
  await installTrigger(schema, table, logTable, pkCols);
  await insertJob(schema, table, shadow, logTable, batchSize);
  await saveJob(schema, table, { phase: "copying" });

  const result = await bulkCopy(schema, table, shadow, pkCols, batchSize, null, 0);
  if (!result.finished) return;

  await saveJob(schema, table, { phase: "indexing" });
  await createIndexes(schema, table, shadow);

  info("Replaying accumulated changes...");
  const { seq, replayed } = await replayChanges(schema, table, shadow, logTable, pkCols, 0);
  await saveJob(schema, table, { phase: "ready", replay_seq: seq });
  info(`Replayed ${replayed.toLocaleString()} changes. Ready for swap.`);
  info("");
  info(`Next step:  ${BOLD}pg_compact.ts swap ${table}${RESET}`);
}

async function cmdResume(schema: string, table: string, batchSize: number) {
  await ensureSchema();
  const job = await getJob(schema, table);
  if (!job) die(`No job found for ${schema}.${table}. Use 'start' first.`);

  const pkCols = await getPkColumns(schema, table);
  const { shadow_table: shadow, log_table: logTable } = job;
  let phase = job.phase;

  info(`Resuming compaction of ${schema}.${table} (phase: ${phase})`);

  if (phase === "init" || phase === "copying") {
    await installTrigger(schema, table, logTable, pkCols);
    await saveJob(schema, table, { phase: "copying" });

    const result = await bulkCopy(
      schema, table, shadow, pkCols, batchSize,
      deserializePk(job.last_copied_id), job.rows_copied
    );
    if (!result.finished) return;
    phase = "copied";
  }

  if (phase === "copied" || phase === "indexing") {
    await saveJob(schema, table, { phase: "indexing" });
    await createIndexes(schema, table, shadow);
    phase = "indexed";
  }

  if (phase === "indexed" || phase === "ready") {
    info("Replaying accumulated changes...");
    const { seq, replayed } = await replayChanges(
      schema, table, shadow, logTable, pkCols, job.replay_seq
    );
    const remaining = await countPendingChanges(logTable, seq);
    await saveJob(schema, table, { phase: "ready", replay_seq: seq });
    info(`Replayed ${replayed.toLocaleString()} changes, ${remaining.toLocaleString()} pending.`);
    info("");
    info(`Next step:  ${BOLD}pg_compact.ts swap ${table}${RESET}`);
  }

  if (phase === "swapped") {
    info("Already swapped. Run 'abort' to clean up metadata.");
  }
}

async function cmdSwap(schema: string, table: string, lockTimeoutMs: number) {
  const job = await getJob(schema, table);
  if (!job) die(`No job found for ${schema}.${table}.`);
  if (job.phase !== "indexed" && job.phase !== "ready") {
    die(`Job is in phase '${job.phase}'. Must be 'ready' or 'indexed'. Run 'resume' first.`);
  }

  const pkCols = await getPkColumns(schema, table);
  await doSwap(
    schema, table, job.shadow_table, job.log_table, pkCols, job.replay_seq, lockTimeoutMs
  );
}

async function cmdStatus(schema: string, table: string | null) {
  await ensureSchema();

  let jobs: Job[];
  if (table) {
    const j = await getJob(schema, table);
    jobs = j ? [j] : [];
  } else {
    jobs = await getAllJobs();
  }

  if (jobs.length === 0) {
    console.log("No active compaction jobs.");
    return;
  }

  for (const j of jobs) {
    let pending = 0;
    if (j.phase !== "init" && j.phase !== "swapped") {
      try { pending = await countPendingChanges(j.log_table, j.replay_seq); } catch { pending = -1; }
    }

    let srcSize = 0, shadowSize = 0;
    try { srcSize = await getTableSize(j.source_schema, j.source_table); } catch {}
    try { shadowSize = await getTableSize(COMPACT_SCHEMA, j.shadow_table); } catch {}

    const estRows = await getRowEstimate(j.source_schema, j.source_table);
    const pct = estRows > 0 ? ((j.rows_copied / estRows) * 100).toFixed(1) : "?";

    console.log(`\n${"─".repeat(60)}`);
    console.log(`  Table:       ${j.source_schema}.${j.source_table}`);
    console.log(`  Phase:       ${BOLD}${j.phase}${RESET}`);
    console.log(`  Rows copied: ${j.rows_copied.toLocaleString()} / ~${estRows.toLocaleString()} (${pct}%)`);
    console.log(`  Last ID:     ${trunc(j.last_copied_id, 50)}`);
    console.log(`  Pending CDC: ${pending.toLocaleString()} changes`);
    console.log(`  Source size: ${prettyBytes(srcSize)}`);
    console.log(`  Shadow size: ${prettyBytes(shadowSize)}`);
    if (srcSize > 0 && shadowSize > 0) {
      const savings = srcSize - shadowSize;
      console.log(`  Savings:     ${prettyBytes(savings)} (${((savings / srcSize) * 100).toFixed(1)}%)`);
    }
    console.log(`  Started:     ${j.started_at}`);
    console.log(`  Updated:     ${j.updated_at}`);
    console.log(`${"─".repeat(60)}`);
  }
}

async function cmdAbort(schema: string, table: string) {
  await ensureSchema();
  await doAbort(schema, table);
}

async function cmdAbortAll() {
  await ensureSchema();
  const jobs = await getAllJobs();
  if (jobs.length === 0) { info("No active jobs."); return; }
  for (const j of jobs) {
    await doAbort(j.source_schema, j.source_table);
  }
}

// ─── Utilities ───────────────────────────────────────────────────────────────

function trunc(s: string | null | undefined, maxLen: number): string {
  if (s == null) return "NULL";
  return s.length <= maxLen ? s : s.slice(0, maxLen - 3) + "...";
}

function prettyBytes(n: number): string {
  for (const unit of ["B", "KB", "MB", "GB", "TB"]) {
    if (Math.abs(n) < 1024) return `${n.toFixed(1)} ${unit}`;
    n /= 1024;
  }
  return `${n.toFixed(1)} PB`;
}

// ─── Argument parsing ────────────────────────────────────────────────────────

function parseArgs() {
  const args = process.argv.slice(2);
  const flags: Record<string, string> = {};
  const positional: string[] = [];

  for (let i = 0; i < args.length; i++) {
    const a = args[i];
    if (a.startsWith("--")) {
      const key = a.replace(/^--/, "");
      if (i + 1 < args.length && !args[i + 1].startsWith("--")) {
        flags[key] = args[++i];
      } else {
        flags[key] = "true";
      }
    } else {
      positional.push(a);
    }
  }

  return { flags, positional };
}

function usage(): never {
  console.log(`
${BOLD}pg_compact${RESET} — Resumable PostgreSQL table compaction

${BOLD}USAGE${RESET}
  pg_compact.ts <command> [table] [options]

${BOLD}COMMANDS${RESET}
  start   <table>   Begin compacting a table
  resume  <table>   Resume after interruption
  status  [table]   Show progress (all jobs if table omitted)
  swap    <table>   Final cutover (brief ACCESS EXCLUSIVE lock)
  abort   <table>   Cancel an in-progress job and clean up
  abort-all         Cancel all in-progress jobs

${BOLD}OPTIONS${RESET}
  --dsn <url>           Connection string (default: $DATABASE_URL or $POSTGRES_URL)
  --schema <name>       Source table schema (default: public)
  --batch <n>           Rows per batch (default: 50,000)
  --lock-timeout <ms>   Lock timeout for swap (default: 5,000)

${BOLD}WORKFLOW${RESET}
  1. pg_compact.ts start  shopify_data__product
  2. pg_compact.ts status shopify_data__product    # monitor
  3. pg_compact.ts swap   shopify_data__product    # cutover (<1s lock)
  4. DROP TABLE public.shopify_data__product__pre_compact;

${BOLD}MONITOR FROM PSQL${RESET}
  SELECT * FROM _compact.jobs;
`);
  process.exit(0);
}

// ─── Main ────────────────────────────────────────────────────────────────────

async function main() {
  const { flags, positional } = parseArgs();

  if (positional.length === 0 || flags["help"] || positional[0] === "help") {
    usage();
  }

  const command = positional[0];
  const table = positional[1] || null;
  const schema = flags["schema"] || "public";
  const batchSize = flags["batch"] ? parseInt(flags["batch"], 10) : DEFAULT_BATCH;
  const lockTimeout = flags["lock-timeout"]
    ? parseInt(flags["lock-timeout"], 10)
    : DEFAULT_LOCK_TIMEOUT;

  const dsn = flags["dsn"] || process.env.DATABASE_URL || process.env.POSTGRES_URL || "";
  connect(dsn);

  try {
    switch (command) {
      case "start":
        if (!table) die("Usage: pg_compact.ts start <table>");
        await cmdStart(schema, table, batchSize);
        break;
      case "resume":
        if (!table) die("Usage: pg_compact.ts resume <table>");
        await cmdResume(schema, table, batchSize);
        break;
      case "swap":
        if (!table) die("Usage: pg_compact.ts swap <table>");
        await cmdSwap(schema, table, lockTimeout);
        break;
      case "status":
        await cmdStatus(schema, table);
        break;
      case "abort":
        if (!table) die("Usage: pg_compact.ts abort <table>");
        await cmdAbort(schema, table);
        break;
      case "abort-all":
        await cmdAbortAll();
        break;
      default:
        die(`Unknown command: ${command}. Run with --help for usage.`);
    }
  } finally {
    db?.close();
  }
}

main().catch((e) => {
  err(e.message || e);
  process.exit(1);
});
