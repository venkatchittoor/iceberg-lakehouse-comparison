# Delta Lake vs Apache Iceberg — Side-by-Side Comparison

> Based on hands-on experience building the same e-commerce Medallion Architecture pipeline
> in both formats: a Databricks / Delta Lake pipeline and this local PySpark / Iceberg pipeline.

---

## Summary Table

| Capability | Delta Lake | Apache Iceberg |
|---|---|---|
| Created by | Databricks (2019) | Netflix (2018), Apache Foundation |
| Primary ecosystem | Databricks / Spark | Engine-agnostic (Spark, Flink, Trino, DuckDB, …) |
| Storage format | Parquet + `_delta_log/` JSON/Parquet | Parquet/ORC/Avro + metadata JSON + Avro manifests |
| Time travel syntax | `VERSION AS OF` / `TIMESTAMP AS OF` | `VERSION AS OF` / `TIMESTAMP AS OF` |
| Schema evolution | `MERGE SCHEMA`, Auto-merge | `ALTER TABLE ADD/DROP/RENAME COLUMN` (safe by design) |
| Partition evolution | Requires full rewrite | In-place with `ADD PARTITION FIELD` (no rewrite) |
| Catalog | Unity Catalog (Databricks-managed) | Hadoop / REST / Hive / Glue / Nessie |
| Multi-engine reads | Limited outside Databricks | First-class (Spark, Flink, Trino, DuckDB, Hive) |
| Data quality (DLT) | `@dlt.expect` / `@dlt.expect_or_drop` | Custom checks (no native equivalent) |
| Streaming | Structured Streaming (native) | Incremental reads + Flink sink (maturing) |
| Local / open dev | Complex without Databricks | Runs fully local with PySpark (this repo) |
| Best fit | Databricks-first orgs | Multi-engine / cloud-agnostic data platforms |

---

## 1. Overview

### Delta Lake
Delta Lake was created by Databricks in 2019 and open-sourced under the Linux Foundation in
2021. It extends Apache Parquet with a transaction log (`_delta_log/`) that turns an ordinary
object-storage prefix into an ACID table. The primary runtime is Apache Spark, and the richest
feature set is available on the Databricks platform (Unity Catalog, Delta Live Tables, Photon).

### Apache Iceberg
Apache Iceberg was created at Netflix in 2018 to solve correctness problems they experienced
with Hive tables at petabyte scale. It became a top-level Apache project in 2020. Iceberg
defines a table spec — not a runtime — which means any engine that implements the spec
(Spark, Flink, Trino, DuckDB, Hive, StarRocks) can read and write Iceberg tables
interchangeably without format conversion.

**Bottom line:** Delta Lake is a product tied to Databricks' execution engine. Iceberg is
an open standard with pluggable engines.

---

## 2. Table Format — Storage & Metadata

### Delta Lake
```
s3://bucket/my_table/
├── _delta_log/
│   ├── 00000000000000000000.json   ← commit 0 (schema, add files)
│   ├── 00000000000000000001.json   ← commit 1 (update)
│   └── 00000000000000000010.checkpoint.parquet  ← compacted log
└── part-00000-<uuid>.parquet
```
- The `_delta_log/` is a sequential JSON/Parquet commit log.
- Every write appends a new JSON file listing added/removed Parquet files.
- Checkpoints compact the log every 10 commits by default.
- Schema and statistics live inside each commit JSON.

### Apache Iceberg
```
iceberg_warehouse/silver/orders_enriched/
├── metadata/
│   ├── v1.metadata.json            ← snapshot 1 (schema, partition spec, snapshot ref)
│   ├── v2.metadata.json            ← snapshot 2
│   ├── <uuid>-m0.avro              ← manifest file (list of data files + stats)
│   └── snap-<id>-<uuid>.avro       ← manifest list (list of manifests)
└── data/
    └── 00000-27-<uuid>.parquet
```
- Three-level metadata hierarchy: **metadata file → manifest list → manifest files → data files**.
- Manifests (Avro) carry per-file column statistics enabling aggressive file pruning.
- Snapshot isolation is explicit: each snapshot ID points to a complete, immutable table state.
- In this project, `explore_iceberg.py` queries `.history`, `.snapshots`, `.manifests`, and
  `.files` as first-class Spark tables.

**Key difference:** Iceberg's manifest layer carries richer per-file statistics and cleanly
separates the snapshot pointer from the file list, enabling more efficient planning at scale.

---

## 3. Time Travel

Both formats support querying past states of a table, but the mechanics differ.

### Delta Lake
```python
# By version number
df = spark.read.format("delta").option("versionAsOf", 0).load("s3://…/my_table")

# By timestamp
df = spark.read.format("delta").option("timestampAsOf", "2024-01-01").load("…")

# SQL
spark.sql("SELECT * FROM my_table VERSION AS OF 0")
spark.sql("SELECT * FROM my_table TIMESTAMP AS OF '2024-01-01'")
```
- Versions are sequential integers (0, 1, 2 …) matching `_delta_log` commit numbers.
- Retention controlled by `delta.logRetentionDuration` (default 30 days).
- `VACUUM` permanently deletes files older than the retention window.

### Apache Iceberg
```python
# By snapshot ID (as used in explore_iceberg.py)
spark.sql(f"SELECT * FROM local.silver.orders_enriched VERSION AS OF {snapshot_id}")

# By timestamp
spark.sql("SELECT * FROM local.silver.orders_enriched TIMESTAMP AS OF '2026-04-20 03:54:30'")

# Inspect the full history
spark.sql("SELECT * FROM local.silver.orders_enriched.history")
spark.sql("SELECT * FROM local.silver.orders_enriched.snapshots")
```
- Snapshots use globally unique 64-bit IDs, not sequential integers.
- `is_current_ancestor` in `.history` shows whether a snapshot is in the current lineage —
  critical after a `createOrReplace` (which cuts a new root, as seen in our second pipeline run).
- Retention controlled per-table via `write.metadata.delete-after-commit.enabled`.

**Observed in this project:**
After running `pipeline.py` twice, snapshot `8667773288305875514` showed
`is_current_ancestor = False` — it was replaced rather than appended to.
Time travel to that orphan snapshot still worked because the underlying Parquet file had not
yet been expired.

**Key difference:** Iceberg snapshot IDs are content-addressed and globally unique;
Delta versions are local sequential integers. Iceberg makes orphan vs. ancestor lineage
explicitly queryable.

---

## 4. Schema Evolution

### Delta Lake
```python
# Opt-in per write
df.write.format("delta").option("mergeSchema", "true").mode("append").save("…")

# Session-wide
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# DDL
spark.sql("ALTER TABLE my_table ADD COLUMNS (discount_pct DOUBLE)")
```
- `mergeSchema` allows additive changes (new columns, widening types).
- Breaking changes (drop column, rename) require `overwriteSchema = true`, which rewrites data.
- Column mapping mode (`delta.columnMapping.mode = name`) enables rename/drop without rewrite
  but must be explicitly enabled and requires reader/writer version upgrades.

### Apache Iceberg
```python
# DDL — always safe, zero data rewrite
spark.sql("ALTER TABLE local.silver.orders_enriched ADD COLUMN discount_pct DOUBLE")
spark.sql("ALTER TABLE local.silver.orders_enriched RENAME COLUMN old_name TO new_name")
spark.sql("ALTER TABLE local.silver.orders_enriched ALTER COLUMN price TYPE DOUBLE")
spark.sql("ALTER TABLE local.silver.orders_enriched DROP COLUMN deprecated_col")
```
- Iceberg tracks columns by **integer ID**, not name. Rename/drop never require a data rewrite.
- Safe by design: no flags to set, no version upgrades needed.
- Type promotions (int → long, float → double) are always safe.

**Observed in this project:**
`explore_iceberg.py` added `discount_pct DOUBLE` with a single `ALTER TABLE` statement.
Column count went 20 → 21 with zero Parquet files rewritten. Existing rows read `NULL`
for the new column — confirmed in the output table.

**Key difference:** Iceberg schema evolution is safe by default for all operations.
Delta Lake requires opt-in flags and column mapping mode upgrades for rename/drop.

---

## 5. Partition Evolution

This is one of Iceberg's most significant architectural advantages.

### Delta Lake
```python
# Partition is baked into the table at CREATE time
df.write.format("delta").partitionBy("order_year", "order_month").save("…")

# To change partitioning: full rewrite required
spark.sql("REPLACE TABLE my_table USING delta PARTITIONED BY (order_year) AS SELECT * FROM my_table")
```
- Partition scheme is fixed at table creation.
- Changing partitioning requires rewriting all data files — expensive on large tables.
- Z-ORDER is a Delta-specific alternative for physical clustering without strict partitioning.

### Apache Iceberg
```python
# Add a partition field to an existing table — zero rewrite
spark.sql("ALTER TABLE local.silver.orders_enriched ADD PARTITION FIELD order_year")
spark.sql("ALTER TABLE local.silver.orders_enriched ADD PARTITION FIELD order_month")

# Drop a partition field — zero rewrite
spark.sql("ALTER TABLE local.silver.orders_enriched DROP PARTITION FIELD order_month")

# Hidden partitioning — no partition column needed in the data
spark.sql("ALTER TABLE t ADD PARTITION FIELD months(order_date)")  # bucket, truncate, etc.
```
- Iceberg stores the partition spec **per snapshot**. Old files keep their original spec;
  new writes use the new spec.
- The query engine handles mixed-spec reads transparently.
- Hidden partitioning transforms (bucket, truncate, date functions) mean partition values
  are derived at write time and never need to exist as physical columns.

**Observed in this project:**
`explore_iceberg.py` section 4 showed `_partition: struct<>` — unpartitioned.
The output explained exactly how `ADD PARTITION FIELD order_year` would work without a
data rewrite, which is impossible in Delta Lake without a full table replacement.

**Key difference:** Iceberg partition evolution is metadata-only. Delta Lake requires
full data rewrite to change partitioning.

---

## 6. Catalog

The catalog determines how tables are discovered, versioned, and secured across engines.

### Delta Lake — Unity Catalog
```python
spark = SparkSession.builder \
    .config("spark.databricks.service.address", "https://<workspace>.azuredatabricks.net") \
    ...
```
- **Unity Catalog** is Databricks' governed metastore (cloud-hosted, Databricks-managed).
- Provides fine-grained access control, data lineage, and audit logs out of the box.
- Tables registered as `catalog.schema.table` (three-level namespace).
- External access requires the Delta Sharing protocol or a Databricks compute cluster.
- No local-only option; requires a Databricks workspace.

### Apache Iceberg — Pluggable Catalogs
```python
# Hadoop catalog — local filesystem, no server (used in this project)
.config("spark.sql.catalog.local.type", "hadoop")
.config("spark.sql.catalog.local.warehouse", "./iceberg_warehouse")

# REST catalog — language-agnostic HTTP API
.config("spark.sql.catalog.prod.type", "rest")
.config("spark.sql.catalog.prod.uri", "https://catalog.example.com")

# AWS Glue
.config("spark.sql.catalog.glue", "org.apache.iceberg.aws.glue.GlueCatalog")

# Project Nessie — git-like branching for data
.config("spark.sql.catalog.nessie.uri", "http://localhost:19120/api/v1")
```
- Catalog is pluggable: Hadoop, Hive, REST, Glue, Nessie, Polaris, Gravitino.
- Same table readable by Spark, Trino, Flink, DuckDB without any format conversion.
- In this project: Hadoop catalog over local filesystem — zero infrastructure, runs fully offline.

**Key difference:** Unity Catalog is a managed, opinionated governance platform tied to
Databricks. Iceberg catalogs are open and interoperable, from a local folder to a
production REST service.

---

## 7. Multi-Engine Support

### Delta Lake
| Engine | Support |
|---|---|
| Databricks Spark | Native, full feature set |
| Apache Spark (OSS) | Good via `delta-core` library |
| Trino / Presto | Read-only via Delta connector |
| Apache Flink | Limited, community-maintained |
| DuckDB | Read-only via `delta` extension |
| pandas | Via `deltalake` Python library |

- Best experience is on Databricks; OSS engines often lag on newer Delta protocol features.
- Delta Sharing enables cross-platform reads but requires a sharing server.

### Apache Iceberg
| Engine | Support |
|---|---|
| Apache Spark | Full read/write (this project) |
| Apache Flink | Full read/write (streaming-first) |
| Trino / Presto | Full read/write |
| DuckDB | Full read via `iceberg` extension |
| Apache Hive | Full read/write |
| StarRocks / Doris | Full read/write |
| pandas / PyArrow | Full read via `pyiceberg` library |

- Any engine implementing the Iceberg table spec gets full ACID + time travel + schema evolution.
- `pyiceberg` (installed in this project) allows pure-Python reads without Spark.

**Key difference:** Iceberg was designed from day one for multi-engine interoperability.
Delta Lake's broadest support requires Databricks; OSS support is improving but uneven.

---

## 8. Data Quality

### Delta Lake — Delta Live Tables (DLT)
```python
import dlt

@dlt.table
@dlt.expect("valid_price", "unit_price > 0")
@dlt.expect_or_drop("valid_quantity", "quantity BETWEEN 1 AND 100")
@dlt.expect_or_fail("order_id_not_null", "order_id IS NOT NULL")
def silver_orders():
    return (
        dlt.read("bronze_orders")
           .filter(col("status") != "cancelled")
    )
```
- `@dlt.expect` — tracks violations as metrics, keeps bad rows.
- `@dlt.expect_or_drop` — silently removes violating rows.
- `@dlt.expect_or_fail` — fails the pipeline on any violation.
- Violation counts are surfaced in the DLT pipeline UI as quality metrics.
- Requires a Databricks workspace; not available in OSS Spark.

### Apache Iceberg — Custom Checks
```python
# No native framework — implement as DataFrame assertions before writing
def validate_silver(df):
    invalid_price = df.filter("unit_price <= 0").count()
    invalid_qty   = df.filter("quantity < 1 OR quantity > 100").count()
    null_orders   = df.filter("order_id IS NULL").count()

    if null_orders > 0:
        raise ValueError(f"Data quality failure: {null_orders} null order_ids")
    if invalid_price > 0:
        print(f"WARNING: {invalid_price} rows with non-positive price")

    return df.filter("unit_price > 0").filter("quantity BETWEEN 1 AND 100")
```
- No native DQ framework; validation is hand-rolled before the Iceberg write.
- Third-party tools fill the gap: **Great Expectations**, **Soda Core**, **deequ** (Spark),
  or **Apache Griffin**.
- Some catalogs (Nessie, Polaris) add constraint-like features at the catalog layer.

**Key difference:** Delta Live Tables is a polished, integrated DQ framework tied to
Databricks. Iceberg requires external tooling or custom code for equivalent guarantees.

---

## 9. Streaming

### Delta Lake — Structured Streaming
```python
# Write stream
df_stream.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/checkpoints/orders") \
    .outputMode("append") \
    .table("silver.orders_enriched")

# Read stream (continuous)
spark.readStream.format("delta").table("bronze.orders")
```
- Structured Streaming is a first-class citizen in Delta Lake.
- `_delta_log` is the changelog; Spark reads new commit entries incrementally.
- Change Data Feed (CDF) exposes row-level changes (`_change_type`, `_commit_version`) for
  downstream CDC pipelines.
- Auto Loader (Databricks) detects new files in cloud storage and ingests them automatically.

### Apache Iceberg — Incremental Reads + Flink
```python
# Spark incremental read between two snapshots
df = spark.read \
    .option("start-snapshot-id", start_id) \
    .option("end-snapshot-id",   end_id) \
    .format("iceberg") \
    .load("local.silver.orders_enriched")

# Apache Flink streaming sink (production pattern)
table_env.execute_sql("""
    INSERT INTO prod.silver.orders_enriched
    SELECT * FROM kafka_source
""")
```
- Spark streaming support for Iceberg exists but is less mature than Delta.
- **Apache Flink** is the preferred streaming engine for Iceberg in production (Netflix,
  Apple, LinkedIn all use Flink → Iceberg).
- Iceberg's snapshot model makes incremental reads natural: scan only snapshots after a
  given ID.
- No native Auto Loader equivalent; file discovery is handled by the ingestion engine.

**Key difference:** Delta Lake + Spark Structured Streaming is a battle-tested, seamless
combo on Databricks. Iceberg streaming is production-grade but relies on Flink or
custom incremental-snapshot patterns for best results.

---

## 10. When to Choose Each

### Choose Delta Lake when…

| Scenario | Reason |
|---|---|
| Your team is on Databricks | Native integration, Unity Catalog, DLT, Photon — no friction |
| You need managed data quality | `@dlt.expect` with UI metrics requires no extra tooling |
| Your primary engine is Spark | Delta's Spark integration is the most mature in the ecosystem |
| You want Auto Loader for cloud ingest | Automatic file discovery without custom orchestration |
| Governance & lineage are critical now | Unity Catalog delivers this out of the box |
| You're already in the Azure / AWS Databricks ecosystem | Least operational overhead |

### Choose Apache Iceberg when…

| Scenario | Reason |
|---|---|
| Multi-engine is a requirement | Spark + Flink + Trino + DuckDB all read the same table natively |
| You want cloud / vendor independence | Runs on any object store with any catalog, including locally |
| Partition evolution without rewrites | Evolve partitioning on 10TB tables in seconds, not hours |
| Flink is your streaming engine | Iceberg is the de-facto standard sink for Flink pipelines |
| You need a local dev environment | This project runs fully offline — no cloud account needed |
| You're building a lakehouse on Snowflake / AWS / GCP | Iceberg is the native open format across all three |
| Schema evolution safety matters | All column ops (add/rename/drop) are safe with zero rewrites |

### The Honest Middle Ground

Both formats are converging. Delta Lake 3.x added Universal Format (UniForm) to expose Delta
tables as Iceberg to external engines. Iceberg added row-level deletes (v2 spec) matching
Delta's MERGE performance. The choice today is less about features and more about
**ecosystem fit** and **operational model**:

- **Databricks-first org** → Delta Lake. You get the full platform, not just the format.
- **Platform-agnostic org** → Apache Iceberg. You keep your options open across clouds,
  engines, and vendors.

---

## 11. Metadata Architecture

Iceberg's 5-level hierarchy is what makes all its advanced features possible:

```
Catalog
  └── Snapshot  (v1.metadata.json, v2.metadata.json …)
        └── Manifest List  (snap-<id>-<uuid>.avro)
              └── Manifest Files  (<uuid>-m0.avro …)
                    └── Parquet Data Files
```

Each level is immutable once written. A completed write creates a new Snapshot pointer; all lower
levels are reused or newly appended, never modified. This absolute immutability is the foundation
of time travel, schema evolution, and partition evolution — every operation adds new metadata
rather than patching existing files.

**Old Parquet files are never rewritten by any Iceberg metadata operation.** New files are always
written alongside old ones. Compaction (`rewrite_data_files`) is an explicit, opt-in operation.

### Which Levels Each Feature Touches

| Feature | Catalog | Snapshot | Manifest List | Manifest Files | Parquet |
|---|:---:|:---:|:---:|:---:|:---:|
| Table History | — | ✅ | — | — | — |
| Time Travel | — | ✅ | ✅ | ✅ | ✅ |
| Schema Evolution | — | ✅ | — | — | — |
| Partition Evolution | — | ✅ | ✅ | ✅ | ✅ |
| Table Metadata | ✅ | ✅ | ✅ | ✅ | — |

**Table History** and **Schema Evolution** touch only the Snapshot layer — they complete in
milliseconds regardless of table size.

**Time Travel** traverses all levels down to Parquet (read-only). **Partition Evolution** updates
the Snapshot's partition spec and creates new Manifest Files + Parquet for subsequent writes, but
never rewrites existing files.

**Table Metadata** queries (`.files`, `.snapshots`, `.manifests`) stop just before Parquet —
instantaneous even on tables with billions of rows.

### Delta Lake Comparison

Delta Lake uses a simpler two-level structure: `_delta_log/` JSON commits (with inline statistics
and operation metadata) pointing directly to Parquet data files. Checkpoints compact the log every
10 commits. This is easier to reason about but provides less separation between metadata and data —
changing partitioning or column names requires touching data files because partition values and
column names are embedded in the Parquet file paths and schema.

---

## 12. Running Scorecard

Across the five features demonstrated in `explore_iceberg.py`:

| Feature | Winner | Reason |
|---|---|---|
| Table History | **Delta Lake edge** | Richer per-commit metadata out of the box: operation type, user, cluster ID, row-level metrics |
| Time Travel | **Iceberg edge** | Globally unique content-addressed snapshot IDs vs. local sequential integers — better for multi-engine safety and cross-cloud replication |
| Schema Evolution | **Iceberg edge** | All operations (add/rename/drop) safe by default; Delta requires opt-in column mapping mode (protocol version upgrade) |
| Partition Evolution | **Iceberg edge** | Milliseconds (0.23 s observed) vs. full data rewrite; metadata-only by design |
| Table Metadata | **Tie** | Both expose rich metadata via SQL; Iceberg uses 3-layer Avro manifests, Delta uses `_delta_log` JSON commits |

**Score: Iceberg 3 — Delta Lake 1 — Tie 1** (on this feature set)

Outside this feature set, Delta Lake leads on DLT integrated data quality, Structured Streaming
maturity, and Unity Catalog governance. The scorecard above is scoped to open table format
capabilities, not platform capabilities.

---

## Project Reference

| Artifact | Delta Lake project | Iceberg project (this repo) |
|---|---|---|
| Pipeline | `pipeline.py` (DLT + Unity Catalog) | `pipeline.py` (PySpark + Hadoop catalog) |
| Data gen | `generate_data.py` | `generate_data.py` |
| Feature demo | N/A | `explore_iceberg.py` |
| Warehouse | Databricks workspace | `./iceberg_warehouse/` (local) |
| Catalog | `unity_catalog.schema.table` | `local.bronze/silver/gold.table` |
| Data quality | `@dlt.expect_or_drop` | Pre-write DataFrame filter |
| Time travel | `VERSION AS OF 0` (int) | `VERSION AS OF <snapshot_id>` (int64) |
