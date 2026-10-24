# Auto Loader in Depth: Batch Mode

A simple guide to how Auto Loader works when you run it as a **scheduled batch job**. Continuous streaming is left out on purpose.

---

## 1. What Auto Loader is

- Auto Loader reads files from cloud storage (ADLS, S3, GCS or a Unity Catalog volume) and loads them into a Delta table.
- Its main promise: **each file is loaded exactly once**, even if you run the job many times.
- It is written with `spark.readStream.format("cloudFiles")`, but don't let the word *stream* confuse you. With the right trigger it behaves like a normal batch job.

---

## 2. Batch mode = `availableNow`

```python
(spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", checkpoint_path)
    .load(source_path)
 .writeStream
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)      # ← this makes it batch
    .toTable("bronze.my_table"))
```

What `availableNow=True` does:
1. Looks at the folder and finds all files **not loaded before**.
2. Loads them (in one or more small batches).
3. **Stops.** The cluster can shut down.

You run it on a schedule (for example every hour or once a day) from a Databricks Job. You pay only while it runs.

> Do not use the old `trigger(once=True)`. It is deprecated; `availableNow` replaces it.

**Why not just use `spark.read`?**

| | `spark.read` | Auto Loader (`availableNow`) |
|---|---|---|
| Remembers loaded files? | No | Yes (checkpoint) |
| Second run with old + new files | Loads **everything again** → duplicates | Loads **only new files** |
| Schema changes | You handle them | Built-in tracking and evolution |
| Good for | One-off or full reloads | Any folder that keeps getting new files |

---

## 3. How Auto Loader finds new files (in batch)

There are two separate jobs. Keep them apart in your head:

| Job | Question it answers | How |
|---|---|---|
| **Discovery** | "Which files are in the folder?" | Lists the folder (or reads a file-events cache) |
| **Tracking** | "Which of these did I already load?" | Checks its memory in the checkpoint |

### Discovery options for batch jobs

**a) Directory listing (default)**
- Auto Loader lists all files under the path, then drops the ones it already knows.
- Nothing to set up.
- Cost grows with the **number of files left in the folder**. On ADLS, one list call returns up to 5,000 files. A folder with 50,000 files needs about 10 calls per run, which is fine. Millions of files get slow and expensive.
- Good for: small or slow-growing folders, daily or hourly batch runs, practice projects.

**b) File events (recommended by Databricks for production)**
- Turned on for a **Unity Catalog external location**. Databricks keeps a cache of new-file notifications.
- First run: one full listing. Later runs: read new files from the cache, **with no listing**.
- If a job doesn't run for **more than 7 days**, the cache has expired and it does a full listing again.
- Option: `cloudFiles.useManagedFileEvents = true` (on newer runtimes it becomes the default when the external location has file events).
- Good for: large folders, many jobs on the same storage.

**Tip to keep listing cheap:** `cloudFiles.cleanSource` (newer runtimes) can **move or delete files after they are loaded** (after a retention period, default 30 days). The folder stays small, so listing stays fast. Don't use it if other jobs also read that folder.

> The discovery method does **not** affect correctness. The checkpoint always decides what is new, so you can switch methods later and keep the same checkpoint.

---

## 4. How Auto Loader remembers files (tracking)

### 4.1 What it stores

- Every file it sees is written into a small database (**RocksDB**) inside the checkpoint folder.
- For each file it keeps the **full path** plus details such as size and modification time.

### 4.2 Is the filename enough?

The identity of a file is its **full path** (folder + filename), not just the name:

```
landing/sales/2026-10-01/part-001.parquet   ← file 1
landing/sales/2026-10-02/part-001.parquet   ← file 2 (different folder = different file)
```

Auto Loader **never reads the content** to compare files. That gives these results:

| What happens in the folder | What Auto Loader does | Result |
|---|---|---|
| New file, new path | Loads it | ✅ Correct |
| Same file copied under a **new name** | Loads it again | ❌ Duplicate rows |
| File **overwritten** at the same path with new data | Skips it (path already known) | ❌ New data **missed** |
| Same rows inside a different new file | Loads them | ❌ Duplicate rows (fix in Silver) |

**The rule:** files in the landing folder must be **write-once**:
- Every extract writes a **new file with a unique name** (add a date, timestamp or run ID).
- Never overwrite or rename a file after it lands.

Example for ADF: write `fact_sales_20261023_0700.parquet`, not `fact_sales.parquet` every time.

> `cloudFiles.allowOverwrites = true` makes Auto Loader reload a file when its modification time changes. The whole file is loaded again, so old rows are duplicated. Keep the default (`false`) and land new files instead.

### 4.3 Check what was loaded

```sql
SELECT * FROM cloud_files_state('/Volumes/<catalog>/bronze/checkpoints/<table>');
```

This shows every file the job has seen. It is the fastest way to answer "Was this file loaded?".

---

## 5. Inside the checkpoint folder

```
/Volumes/<catalog>/bronze/checkpoints/fact_sales/
├── metadata          ← ID of this job's stream (never changes)
├── offsets/0, 1, 2…  ← the PLAN for each batch: which files (written before loading)
├── commits/0, 1, 2…  ← the DONE mark for each batch (written after loading)
├── sources/0/rocksdb ← list of every file seen (section 4)
└── _schemas/0, 1, 2… ← schema versions (section 6)
```

How to read it:
- `offsets/5` exists, `commits/5` doesn't → batch 5 was started but not finished. The next run redoes batch 5 with **exactly the same files**.
- Both exist → batch 5 is finished. The next run starts at batch 6.

Rules:
- **One checkpoint per table.** Never share one between two jobs.
- **Keep it outside the landing folder** (for example in a separate `checkpoints` volume).
- **Never edit files inside it.**
- **Deleting it = Auto Loader forgets everything** and reloads all files. Only do that on purpose (and clear the target table too).

---

## 6. Inside the schema location

- Set with `cloudFiles.schemaLocation`. It's usually the **same folder** as the checkpoint; Auto Loader puts the schema in a `_schemas/` subfolder.
- `_schemas/0` = the first schema it inferred. Every change writes the next number (`1`, `2`…). The **highest number is the current schema**.
- On every run Auto Loader reads the latest schema file instead of guessing again, so the schema stays stable.

What happens when a new column appears (default mode `addNewColumns`):
1. Auto Loader sees the new column.
2. It saves a new schema version.
3. **It stops the job with an error, on purpose.**
4. The next run (a retry) starts with the new schema and succeeds.

So set **1–2 retries** on the job task. Also add `.option("mergeSchema", "true")` on the write, so the Delta table can accept the new column.

> There are two schemas: the **reader schema** (in `_schemas/`) and the **table schema** (in Delta). `mergeSchema` is what lets the table schema follow the reader.

---

## 7. Exactly-once, explained simply

**Meaning:** every file is loaded into the table **one time**: not zero times, not two times. This holds even if the job crashes halfway or is run again.

It works because of three things working together:

| # | Piece | Simple explanation |
|---|---|---|
| 1 | **Files don't change** | A failed batch can be re-read and gives the same data |
| 2 | **Plan first, then mark done** | `offsets/N` (plan) is written before loading, `commits/N` (done) after |
| 3 | **Delta remembers the batch number** | Each write to Delta records the job ID and batch number. If the same batch is written again, Delta **skips it** |

**Crash test: batch 5 = files A and B**

| Job crashes… | What the next run does | Duplicates? |
|---|---|---|
| Before the plan is saved | Plans batch 5 again from scratch | No |
| After the plan, before writing to Delta | Loads A and B again (nothing was written yet) | No |
| After writing to Delta, before the "done" mark | Tries batch 5 again → Delta sees batch 5 is already there → **skips** | No |
| After the "done" mark | Starts batch 6 | No |

**Where exactly-once does NOT protect you**

| Situation | Why | What to do |
|---|---|---|
| Checkpoint deleted or path changed | Auto Loader's memory is gone | Treat the checkpoint path as permanent |
| Same data in a copied or renamed file | New path = new file | Unique file names; dedupe in Silver |
| File overwritten at the same path | Path already known → skipped | Never overwrite landed files |
| Your own logic in `foreachBatch` | Spark may re-run a batch; a plain `append` writes it twice | Use `MERGE`, or add `txnAppId` + `txnVersion` (below) |

**Making `foreachBatch` safe** (you'll use this for Silver):
```python
def write_batch(df, batch_id):
    (df.write.format("delta").mode("append")
       .option("txnAppId", "silver_fact_sales")   # fixed name for this job
       .option("txnVersion", batch_id)            # Delta skips a batch number it already has
       .saveAsTable("silver.fact_sales"))
```

**Three delivery levels**

| Level | Lost data? | Duplicates? |
|---|---|---|
| At-most-once | Possible | No |
| At-least-once | No | Possible |
| **Exactly-once** (Auto Loader → Delta with a checkpoint) | No | No |

---

## 8. Recommended setup for a batch job

| Item | Recommendation |
|---|---|
| Trigger | `availableNow=True`, run by a scheduled Databricks Job |
| Landing files | Write-once, unique names per extract |
| Checkpoint + schema | One folder per table, e.g. `/Volumes/<catalog>/bronze/checkpoints/<table>`, in its own volume |
| Schema mode | `addNewColumns` + `mergeSchema` on the write + 1–2 job retries |
| Metadata columns | `_metadata.file_path`, `_metadata.file_modification_time`, `current_timestamp()` as ingested time |
| Write mode | Append only into Bronze |
| Discovery | Directory listing for small or medium folders; file events for big ones |
| Duplicates from data | Not Auto Loader's job; dedupe in Silver |

---

## 9. Test it yourself

1. Run the job twice with no new files → 0 new rows the second time.
2. Add one new file → only that file's rows are added.
3. Copy an old file under a new name → its rows appear twice (proves tracking is by path).
4. Overwrite an old file with new data → nothing is loaded (proves overwrites are skipped).
5. Add a file with a new column → the first run fails, the retry succeeds, and the column appears.
6. Kill the job mid-run, then rerun → no duplicate rows.
7. Run `cloud_files_state(...)` → each file appears once.
8. Delete the checkpoint and rerun → every file is loaded again.

---

## 10. Interview lines

- "I run Auto Loader as a batch job with `availableNow`. It loads only new files, then stops."
- "Auto Loader tracks files by full path in a RocksDB store inside the checkpoint. It never compares content, so landing files must be write-once with unique names."
- "Exactly-once comes from three things: replayable files, the offsets and commits log, and a Delta sink that skips a batch number it already committed."
- "Exactly-once is per file and per checkpoint. Copied files and duplicate rows still need dedup in Silver, and `foreachBatch` needs `MERGE` or `txnVersion` to stay idempotent."
- "Discovery and tracking are separate. Listing or file events only find candidate files; the checkpoint decides what is new."
- "A new column makes the job fail once by design, so I configure job retries and `mergeSchema` on the write."