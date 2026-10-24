# Lakeflow Connect (Standard) Ingestion: Landing to Bronze

Study notes covering ingestion methods, schema handling, semi-structured data and interview-ready answers.

---

## 1. Big picture

<img src="https://blog.nashtechglobal.com/wp-content/uploads/2025/09/Databricks-Lakeflow-connect-1024x574.png" height=500/>

- **Lakeflow Connect Standard connectors** is a grouping label for existing Databricks features that ingest from cloud object storage (ADLS, S3, GCS) and message buses (Kafka). There is nothing separate to install or enable.
- Three ingestion styles:
  - **Batch**: read everything every time (one-off or small data).
  - **Incremental batch**: run on a schedule and process only what is new.
  - **Streaming**: continuous micro-batches (still near real-time, not true real-time).
- Three core methods:

| Method | Style | Idempotent? | Typical use |
|---|---|---|---|
| CTAS / `spark.read` | Batch | No | Small, one-off or ad hoc loads |
| `COPY INTO` | Incremental batch | Yes, per file | Simple scheduled loads, SQL-only teams, thousands of files |
| Auto Loader (`cloudFiles`) | Incremental batch or streaming | Yes, via checkpoint | Recommended default; millions of files, schema evolution |

- Typical layout: files land in a **Unity Catalog volume** (`/Volumes/<catalog>/landing/<volume>/<entity>/<format>/`), then load into **managed Delta tables** in a `bronze` schema. Catalog layers: `landing`, `bronze`, `silver`, `gold`.
- Ingestion (Lakeflow Standard) covers **source files to Bronze**. Orchestration of source-to-landing is a separate concern and was out of scope here.

---

## 2. Source-side context: CDC and fallbacks

- **CDC (change data capture)** must be enabled at the source database. It reads the transaction log and writes changes to change tables, so downstream systems get inserts, updates and deletes without re-reading full tables.
- SQL Server style CDC columns: operation code (1 delete, 2 insert, 3 update-before, 4 update-after), LSN (log sequence number), update mask. A control table stores the last processed LSN.
- CDC does **not** follow DDL. After a source column change, a new capture instance is usually needed.
- **No CDC available?** Use a watermark (`last_updated > last_run`, with a small overlap window for late data) or hash-based change detection (compare a row hash against the previous load).
- Files that carry changes (for example a CSV with an `op` column of I/U/D) are just rows to ingestion. Applying them is a Silver concern.

---

## 3. CTAS and `spark.read`

```sql
CREATE TABLE t AS SELECT * FROM read_files('<path>', format => 'csv', header => true);
```
```python
spark.read.format("csv").option("header", True).load(path).write.saveAsTable("t")
```
- No memory of loaded files. Rerun with `append` and every row doubles; `overwrite` is the safe form.
- `inferSchema` is fine for practice; use an explicit schema for real Bronze.
- `_metadata` is a hidden column; select it explicitly (`_metadata.file_path`, `_metadata.file_modification_time`).

---

## 4. COPY INTO

```sql
COPY INTO bronze.customers
FROM (SELECT *, _metadata.file_path AS _source_file,
             _metadata.file_modification_time AS _file_mod_time,
             current_timestamp() AS _ingested_at
      FROM '/Volumes/.../customers/csv/')
FILEFORMAT = CSV
FORMAT_OPTIONS ('header'='true','inferSchema'='true','mergeSchema'='true')
COPY_OPTIONS ('mergeSchema'='true');
```
- SQL only (call through `spark.sql()` from Python).
- Idempotent **at the file level**: loaded files are recorded in the target table metadata and skipped next time.
- `FORMAT_OPTIONS` control parsing; `COPY_OPTIONS` control behaviour (`mergeSchema`, `force`). `force` reloads files and creates duplicates.
- **Append only.** An updated record creates a second row; a delete marker is just another row. History is whatever is appended.
- The same file loaded again gives 0 rows. An identical row in a **new** file is inserted again (no row-level dedup).
- Not deprecated, but Auto Loader or streaming tables are recommended for new work.

---

## 5. Auto Loader

<img src='https://freedium-mirror.cfd/img/medium/700/1*UXLAmbhbdfoPderNL5hzLw.png' height=500/>

```python
(spark.readStream.format("cloudFiles")
   .option("cloudFiles.format", "csv")
   .option("header", "true")
   .option("cloudFiles.schemaLocation", schema_path)
   .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
   .load(src)
   .select("*",
           F.col("_metadata.file_path").alias("_source_file"),
           F.col("_metadata.file_modification_time").alias("_file_mod_time"),
           F.current_timestamp().alias("_ingested_at"))
 .writeStream
   .option("checkpointLocation", checkpoint_path)
   .option("mergeSchema", "true")
   .trigger(availableNow=True)
   .toTable("bronze.customers"))
```

**Built-in tracking (summary)**
- **Checkpoint** (`checkpointLocation`) remembers which files were processed and which micro-batches were committed.
- **Schema location** (`cloudFiles.schemaLocation`) remembers the inferred and evolved schema, one numbered file per version.
- Each location must be unique per stream. Never share a checkpoint or schema location between two streams.
- Common pattern: use the **same folder** for both, one folder per table (for example `/Volumes/<catalog>/bronze/checkpoints/<table>/`). Auto Loader keeps the schema in a `_schemas/` subfolder, so the two do not clash. Keep checkpoints out of the landing folder.

### 5.1 How Auto Loader identifies a file (is the filename enough?)

- The identity of a file is its **full path** (folder plus filename), not just the filename. `sales/2026-10-01/part-0001.parquet` and `sales/2026-10-02/part-0001.parquet` are two different files.
- Every discovered file is recorded in a **key-value store (RocksDB) inside the checkpoint**, with its path and metadata such as size and modification time.
- **Content is never checked.** Auto Loader does not hash file contents, so:

| Scenario | Default behaviour (`allowOverwrites = false`) | Result |
|---|---|---|
| New file at a new path | Processed | Expected |
| Same file copied to a **new path or name** | Processed again | **Duplicate rows** (path is new) |
| File **overwritten at the same path** with new content | **Ignored** (path already known) | **New data silently missed** |
| Same as above with `cloudFiles.allowOverwrites = true` | Reprocessed when the modification time changes | Whole file loaded again, so old rows are duplicated |
| Identical rows inside a different new file | Processed | Duplicate rows (row-level dedup is a Silver job) |

- **So yes: the path alone is enough for Auto Loader, but only if files are immutable.** The design rule for the landing zone is *write once, never overwrite, never rename*: every extract gets a **new, unique file name** (for example with a run timestamp or run ID). ADF should write `fact_sales_<run_id>.parquet`, not overwrite `fact_sales.parquet`.
- Inspect what has been tracked:
  ```sql
  SELECT * FROM cloud_files_state('/Volumes/<catalog>/bronze/checkpoints/<table>');
  ```
  This lists every file the stream has discovered and when it was committed. It is the quickest way to answer "was this file loaded?".
- The state grows with every file. `cloudFiles.maxFileAge` can expire old entries for very high-volume feeds. Use it carefully: an expired file that reappears would be loaded again.

### 5.2 What is inside the checkpoint folder

```
<checkpoint>/
├── metadata          ← stream ID (stays the same across restarts)
├── offsets/0, 1, 2…  ← write-ahead log: the plan for each micro-batch (which files), written BEFORE processing
├── commits/0, 1, 2…  ← written AFTER the batch is safely committed to the target
├── sources/0/rocksdb ← Auto Loader file state: every discovered file (5.1)
└── _schemas/0, 1, 2… ← schema versions (when schemaLocation points here)
```

- **`offsets/N` without `commits/N`** means batch N was planned but not finished. On restart, Spark re-runs batch N with **exactly the same files**.
- **`offsets` and `commits` are both present** means batch N is done. The next run starts with batch N+1.
- Never edit these files by hand. To reprocess, delete the whole checkpoint (and the target data if needed).

### 5.3 What is inside the schema location

- `_schemas/0` is the first inferred schema; every evolution writes the next number (`1`, `2`…). The **highest number is the current stream schema**.
- On restart, Auto Loader reads the latest schema file instead of inferring again, so the schema stays stable across runs.
- With `addNewColumns`: a new column appears → the stream writes a new schema version → it fails on purpose → the retry starts with the new version.
- This is the **stream** schema. The **table** schema changes only through `mergeSchema` on the writer or `ALTER TABLE` (see 6.2).

### 5.4 Exactly-once processing (the key concept)

**Definition:** every file is processed, and its rows are committed to the target table, **exactly one time**. That holds even if the job crashes mid-batch, is retried, or is run twice.

It rests on three guarantees that work together:

| Piece | Role | How |
|---|---|---|
| **Replayable source** | A failed batch can be read again with the same input | Landed files are immutable; the batch's file list is fixed in `offsets/N` |
| **Write-ahead log** | Spark always knows what each batch contains | `offsets/N` is written before processing; `commits/N` after success |
| **Idempotent sink** | Writing the same batch twice changes nothing | The Delta sink stores the stream ID and batch ID in the Delta transaction log. If batch N is already committed, the retry skips the write |

**Failure walk-through: batch 5 contains files A and B**

| Crash point | What survives | On restart | Duplicates? |
|---|---|---|---|
| Before `offsets/5` is written | Nothing about batch 5 | Lists files again; A and B are planned fresh | No |
| After `offsets/5`, before the Delta commit | The plan (A, B) | Re-runs batch 5 with A and B; nothing was committed before | No |
| After the Delta commit, before `commits/5` | Plan plus data in the table | Re-runs batch 5; the Delta sink sees batch 5 already committed and **skips the write** | No |
| After `commits/5` | Everything | Starts at batch 6 | No |

**Where exactly-once stops (common traps)**

- **It covers one stream and one checkpoint.** Delete or change the checkpoint and the stream starts from zero, reloading every file.
- **It is about files, not rows.** A copied or renamed file and identical rows in a new file are all ingested again (5.1).
- **Overwritten files** at the same path are skipped by default: no duplicates, but **missed data**.
- **`foreachBatch` loses the automatic guarantee.** Spark may re-run a batch after a failure, and a plain `append` inside `foreachBatch` then writes the rows twice. Make the batch function idempotent:
  - Use `MERGE` with a deterministic key (a re-run then updates instead of inserting again), **or**
  - Pass the batch ID to Delta so a repeated batch is skipped:
    ```python
    def write_batch(df, batch_id):
        (df.write.format("delta").mode("append")
           .option("txnAppId", "silver_fact_sales")   # unique, stable per stream
           .option("txnVersion", batch_id)            # Delta skips a batch_id it already has
           .saveAsTable("silver.fact_sales"))
    ```
- **Other sinks** (JDBC, REST APIs, files written by hand) are not idempotent by default. With them, you usually get **at-least-once**, and dedup must happen downstream.
- **Multiple writers:** two streams writing the same table each need their own checkpoint and their own `txnAppId`.

**Delivery guarantees compared**

| Guarantee | Meaning | Typical example |
|---|---|---|
| At-most-once | No duplicates, but data can be lost | Fire-and-forget logging |
| At-least-once | Nothing lost, duplicates possible | Kafka producers, most APIs, `foreachBatch` with plain append |
| **Exactly-once** | Nothing lost and no duplicates | Auto Loader or Delta source → Delta sink with a checkpoint |

### 5.5 Other behaviours worth knowing

- File discovery: **directory listing** (default; lists the folder and skips paths already in the state) or **file events / notifications** (Event Grid plus a queue on Azure; better at very large file counts).
- `trigger(availableNow=True)` processes everything available and stops (incremental batch). A continuous trigger gives streaming, but it is **micro-batch**, hence "near real-time".
- Use `availableNow`, not the deprecated `once=True`.
- Replay: delete the checkpoint (and target data if needed) to reprocess from scratch.
- **Downstream layers read Delta tables, not files.** Files are read only at landing → bronze. `readStream.table("bronze.x")` tracks Delta **table versions** in its own checkpoint. This is clean only from **append-only** tables: a table with updates (for example an SCD2 Silver) needs change data feed, `skipChangeCommits` or a batch read.

---

## 6. Schema evolution

### 6.1 Modes (`cloudFiles.schemaEvolutionMode`)

| Mode | New column behaviour | Notes |
|---|---|---|
| `addNewColumns` | Stream **fails once**, schema file is updated, restart succeeds | Default **when no schema is provided** |
| `rescue` | Schema never changes; new data lands in `_rescued_data` as JSON | Never fails; promote columns deliberately |
| `failOnNewColumns` | Fails until the schema or hints are changed manually | Strict, contract-driven feeds |
| `none` | New columns ignored (rescued if a rescued column is set) | Default **when a schema is provided** |

- **Fail once, then succeed** is by design. In a scheduled job set **task retries to 1 or 2** (one retry per evolution, a second for margin). Declarative pipelines and continuous streams restart themselves.
- **Rescued data column (`_rescued_data`)** captures values that do not fit the schema: type mismatches, case mismatches and, in `rescue` mode, new columns.
- `schemaHints` pin types for chosen columns; `inferColumnTypes` turns on real type inference.
- Defaults for CSV and JSON: everything is inferred as **string**, including nested JSON fields, until `inferColumnTypes` is set.

### 6.2 Two schemas, two places to look

- **Stream schema**: in `schemaLocation`. List the folder and read the newest file.
- **Table schema**: the Delta sink. It changes only if `mergeSchema` is set on the writer or an `ALTER TABLE ... ADD COLUMN` is run. Table ACLs on shared compute can block the automatic path, so `ALTER TABLE` is the fallback.

```sql
DESCRIBE TABLE EXTENDED bronze.customers;   -- current columns and types
DESCRIBE HISTORY bronze.customers;          -- writes and versions
```
```python
spark.read.option("versionAsOf", 3).table("bronze.customers").printSchema()
```

### 6.3 Change scenarios

| Change | Auto Loader behaviour | Handling |
|---|---|---|
| New column | `addNewColumns`: fail once, then added | Allow; sink needs `mergeSchema` |
| Renamed column | Seen as drop plus add; old column becomes NULL for new rows | Keep both in Bronze; map in Silver (`COALESCE(old, new)`) |
| Dropped column | No failure; NULL for rows from new files | Alert via data-quality checks |
| Type change | No failure; value goes to `_rescued_data`, column is NULL | Monitor `_rescued_data`; fix in Silver |
| Type widening (int to bigint) | Existing type is not changed; value rescued if it does not fit | `schemaHints`, or keep Bronze as strings |
| Case change (`Age` vs `age`) | Treated as a mismatch, goes to `_rescued_data` | Standardise names in Silver |
| Column order change | Fine with headers (columns map by name); **headerless CSV breaks silently** | Always require headers |

### 6.4 Renames and who tells you

- Neither Auto Loader nor Delta can tell a rename from a drop plus add. The mapping must be declared by a person: a Silver transform, or a config table (`table, old_name, new_name, effective_date`) applied by a generic function, changed through a pull request.
- Sources rarely announce changes. Some CDC and replication tools capture DDL (for example a schema history topic or a schema registry with compatibility rules), but many just show a new column. Do not rely on it.
- In practice: **data contracts and change management**, **detection** (fail on drift, alert on non-empty `_rescued_data`, schema-diff checks) and a **runbook** (pause Silver, confirm mapping with the producer, update the mapping, replay).

### 6.5 Common industry approach

- **Bronze**: `addNewColumns` with rescued data, job retries and `schemaHints` for the few columns whose types matter.
- **Strict or regulated feeds**: `failOnNewColumns` so every change is approved.
- **High-volume event feeds that must never fail**: `rescue`, with columns promoted deliberately.
- `none` is rare; it silently ignores new data.
- **Silver** always has an explicit schema contract. Bronze is a superset that only grows.

---

## 7. Bronze design principles

- Raw, append-only, minimal transformation. Keep every version and every duplicate.
- Add lineage columns: `_source_file`, `_file_mod_time`, `_ingested_at`. These are not CDC columns; real CDC columns come from the source.
- **Do not cast types in Bronze.** Example: a phone number inferred as a number loses leading zeros. Keep strings (or use `schemaHints` for specific columns) and type in Silver.
- Do not drop Bronze columns; dropping needs column mapping and breaks replay.
- **Silver** does the work: dedupe by business key (`ROW_NUMBER() ... ORDER BY _ingested_at DESC`), apply changes, honour delete markers, select explicit columns (never `SELECT *`), reconcile renames, quarantine bad rows.
  - SCD1: `MERGE INTO` or `AUTO CDC ... STORED AS SCD TYPE 1`.
  - SCD2: `AUTO CDC ... STORED AS SCD TYPE 2` or a manual `MERGE`.

---

## 8. Semi-structured data (JSON)

### 8.1 Ways to land JSON in Bronze

| Approach | How | When |
|---|---|---|
| Parsed (struct and array columns) | `cloudFiles.format = json` with inference and evolution | Stable structure, direct querying |
| Raw string | Read as text, keep one raw column | Maximum safety, parse in Silver |
| `VARIANT` | Parse to VARIANT (newer runtimes) | Volatile or unknown structure |

### 8.2 File format details

- Default JSON expectation is **JSON Lines** (one object per line). Pretty-printed files or a top-level array need `multiLine = true`.
- Real landing zones usually use JSON Lines: better with large files and bad records. A single broken line is isolated; one broken array file fails as a whole.
- Online validators often reject JSON Lines because they expect a single document.

### 8.3 Event-style payloads (for example Kafka exports)

- Typical envelope: `event_id`, `topic`, `partition`, `offset`, `key`, `event_ts`, and a nested `value` payload.
- Delivery is usually **at-least-once**, so duplicates are normal. Bronze keeps them; Silver dedupes on `event_id`.
- If the payload column is a **string**, nested drift (new nested fields) does not fail the stream; only new top-level keys do.
- Keep the raw payload string in Bronze (resilient), then parse in Silver.

### 8.4 STRUCT vs VARIANT

| | STRUCT | VARIANT |
|---|---|---|
| Schema | Fixed, declared up front | None; values describe themselves |
| New fields | Need schema evolution | Just appear |
| Type safety | Enforced at write | Cast at read time |
| Access | `value.store.city` | `v:store.city::string` or `variant_get(...)` |
| Performance | Best (columnar, stats, pruning per field) | Good, much better than re-parsing JSON text, less optimised than STRUCT |
| Rows may differ in shape | No | Yes |
| Best for | Stable contracts (Silver, Gold) | Volatile payloads, fast-changing sources |

A raw JSON string is the worst for querying (re-parsed on every read); keep it only as the raw Bronze copy.

### 8.5 Parsing in Silver

```python
# Infer a schema from a sample that contains the optional fields
sample = (spark.table("bronze.orders")
          .filter("value LIKE '%delivery%'").select("value").limit(1).first()[0])
ddl = spark.range(1).select(F.schema_of_json(F.lit(sample))).first()[0]

# STRUCT with an explicit schema
parsed = spark.table("bronze.orders").withColumn("o", F.from_json("value", schema))

# VARIANT
v = spark.table("bronze.orders").select(F.parse_json("value").alias("v"))
v.selectExpr("v:store.city::string AS city", "v:totals.total::double AS total")
```
- `schema_of_json` needs a **literal** string and sees only that one sample, so fields present in few rows can be missed. `schema_of_json_agg` (SQL) merges across all rows where the runtime supports it.
- `from_json` ignores unknown fields and turns mismatched types into NULL: keep the raw string alongside and quarantine rows where the parsed result is NULL.
- Flatten arrays with `explode()`; nested fields with dot notation.
- VARIANT needs a recent runtime; check availability.

---

## 9. Common gotchas

| Symptom | Cause | Fix |
|---|---|---|
| `DELTA_INVALID_CHARACTERS_IN_COLUMN_NAMES` | Alias contained a trailing space (or one of `,;{}()\n\t=`) | Remove the character; the read can succeed and the failure appears only at write |
| `DELTA_METADATA_MISMATCH` | Target table lacks a column the stream now produces | `mergeSchema` on the writer, or `ALTER TABLE ... ADD COLUMN` |
| Stream fails once after a new column | Expected `addNewColumns` behaviour | Rerun or enable job retries |
| Leading zeros lost | Phone inferred as number | Keep strings in Bronze; `schemaHints` |
| BIT column fails to load from `True`/`False` | Source loader expects 1/0 | Stage and convert, or replace values |
| Rerun doubles rows | CTAS or `append` with no file tracking | Use COPY INTO or Auto Loader, or `overwrite` |
| Renamed file reingested | Tracking is by path | Avoid renaming landed files |
| Updated file not picked up | Same path overwritten; already in the checkpoint state | Write new files with unique names; `allowOverwrites` only if a full reload of that file is acceptable |
| Duplicates after a job retry with `foreachBatch` | Plain append is not idempotent | `MERGE`, or `txnAppId` + `txnVersion` |
| Everything reloaded | Checkpoint deleted or path changed | Treat the checkpoint path as permanent; change it only on purpose |

---

## 10. Break-it test checklist

- Duplicate file (same name, then renamed copy)
- Identical row in a new file
- Late-arriving file
- New column, renamed column, dropped column
- Type change and a bad value (rescued data)
- Killed run, then rerun (check: no duplicate rows, `cloud_files_state` shows each file once)
- Overwrite a landed file at the same path (expect: ignored by default)
- Kill a `foreachBatch` job mid-batch and rerun (plain append vs `txnVersion`)
- Replay from a deleted checkpoint
- Incremental (watermark) load and hash-based change detection

---

## 11. Interview one-liners

- "COPY INTO is idempotent at the file level, not the row level. Row-level duplicates and SCD are handled in Silver."
- "Bronze stays raw and append-only, with lineage metadata on every row. I do not cast types there."
- "Auto Loader tracks files in a checkpoint and the schema in `schemaLocation`. Reprocessing a path needs a new checkpoint or `allowOverwrites`."
- "Auto Loader identifies a file by its full path, stored in RocksDB in the checkpoint. It never looks at content, so the landing zone must be write-once with unique file names."
- "Exactly-once comes from three things together: a replayable source, the offset write-ahead log, and an idempotent Delta sink that skips a batch ID it already committed."
- "Exactly-once is file-level and per checkpoint. Copied files, identical rows and `foreachBatch` appends can still duplicate, so I make batch functions idempotent and dedupe in Silver."
- "`addNewColumns` fails once by design, so I configure job retries and the next attempt runs with the evolved schema."
- "I split changes into additive and breaking. Additive ones evolve in Bronze; renames look like drop plus add, so Bronze keeps both and Silver maps them."
- "Bronze is a superset that evolves automatically; Silver has an explicit contract so upstream drift cannot break downstream."
- "Type changes do not break the stream; they land in `_rescued_data`, which I alert on."
- "Bronze keeps the Kafka payload raw. Silver parses it with an explicit schema, or VARIANT when the payload is volatile, and quarantines rows that fail to parse."
- "Near real-time means micro-batch. It is incremental processing with checkpointed state, not per-event latency."
- "Strict feeds use `failOnNewColumns` so schema changes need approval."

---

## 12. Quick decision guide

| Situation | Choice |
|---|---|
| One-off or small load | CTAS / `spark.read` |
| Simple scheduled SQL load, modest file count | `COPY INTO` |
| Anything growing, many files, schema drift | Auto Loader |
| Source files change shape often | `addNewColumns` plus rescued data plus retries |
| Regulated or contract-driven feed | `failOnNewColumns` |
| Must never fail on drift | `rescue` |
| Stable nested payload | STRUCT in Silver |
| Volatile nested payload | VARIANT |
| Need row-level history or dedup | Silver (`MERGE` / `AUTO CDC`) |