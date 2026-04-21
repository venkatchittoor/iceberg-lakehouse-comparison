# Iceberg Lakehouse — Medallion Architecture on Apache Iceberg

The same e-commerce Medallion Architecture pipeline — 200 customers, 50 products, 1,000 orders,
2,000 order items — built twice: once on **Delta Lake** (Databricks) and once on **Apache Iceberg**
(local PySpark, no cloud account required). The goal is a hands-on, side-by-side comparison of
both open table formats across the dimensions that matter in production: time travel, schema
evolution, partition evolution, catalog portability, and multi-engine support. The Iceberg
pipeline in this repo runs entirely on a laptop — no Databricks workspace, no AWS account, no
Docker — using a local Hadoop catalog backed by the filesystem.

---

## Delta Lake vs Apache Iceberg — Quick Reference

| Dimension | Delta Lake | Apache Iceberg |
|---|---|---|
| **Created by** | Databricks (2019) | Netflix (2018) → Apache Foundation |
| **Table format** | Parquet + `_delta_log/` JSON commit log | Parquet + metadata JSON + Avro manifests |
| **Time travel** | `VERSION AS OF <int>` (sequential) | `VERSION AS OF <int64 snapshot_id>` (content-addressed) |
| **Schema evolution** | `mergeSchema` opt-in; rename/drop needs column mapping mode | All ops safe by default — columns tracked by ID, not name |
| **Partition evolution** | Full data rewrite required | Metadata-only `ADD PARTITION FIELD` — zero rewrite |
| **Catalog** | Unity Catalog (Databricks-managed) | Pluggable: Hadoop, REST, Glue, Hive, Nessie |
| **Multi-engine** | Best on Databricks; limited OSS engine parity | First-class Spark, Flink, Trino, DuckDB, Hive |
| **Data quality** | `@dlt.expect` / `@dlt.expect_or_drop` (DLT) | Custom checks or external tools (Great Expectations, deequ) |

> Full 10-dimension analysis: **[COMPARISON.md](COMPARISON.md)**

---

## Project Structure

```
iceberg-lakehouse-comparison/
├── generate_data.py      # Synthetic e-commerce data generator (Faker-based)
├── pipeline.py           # Bronze → Silver → Gold Medallion pipeline (PySpark + Iceberg)
├── explore_iceberg.py    # Interactive demo of 5 Iceberg-specific features
├── requirements.txt      # Python dependencies
├── .env.example          # Local config template (no credentials needed)
├── COMPARISON.md         # Detailed Delta Lake vs Iceberg comparison (10 sections)
└── README.md             # This file
```

---

## What Was Built

### Medallion Layers

```
./data/*.csv  (generated)
      │
      ▼
BRONZE ──── local.bronze.customers       200 rows   raw ingest, _ingested_at watermark
            local.bronze.products         50 rows   raw ingest, _ingested_at watermark
            local.bronze.orders        1,000 rows   raw ingest, _ingested_at watermark
            local.bronze.order_items   2,000 rows   raw ingest, _ingested_at watermark
      │
      ▼
SILVER ──── local.silver.orders_enriched  1,575 rows  joined + cleaned (cancelled orders removed)
                                                       margins computed (unit_margin, line_margin)
                                                       order_year / order_month extracted
      │
      ▼
GOLD ─────  local.gold.revenue_by_category    90 rows  revenue + margin % by category/month
            local.gold.top_customers         192 rows  lifetime value, avg order value per customer
            local.gold.product_performance    50 rows  revenue, units sold, margin % per product
```

### Gold Layer Highlights

| Table | Top result |
|---|---|
| `revenue_by_category` | Home & Garden — 58% margin in Apr 2025 |
| `top_customers` | Christie Diaz (WA) — $20,797 lifetime value across 8 orders |
| `product_performance` | "Cloned methodical neural-net" — $51,778 revenue, 61.5% margin |

---

## Iceberg Features Demonstrated (`explore_iceberg.py`)

### 1. Table History
Queries `local.silver.orders_enriched.history` — Iceberg's built-in snapshot log.
After two pipeline runs, the table shows two snapshots with `is_current_ancestor` flags:

```
┌────────────────────────────┬─────────────────────┬─────────────┬───────────────────────┐
│ made_current_at            │ snapshot_id         │ parent_id   │ is_current_ancestor   │
├────────────────────────────┼─────────────────────┼─────────────┼───────────────────────┤
│ 2026-04-20 03:54:30        │ 8667773288305875514 │ —           │ False  ← replaced     │
│ 2026-04-20 04:04:08        │ 5148357350648033783 │ —           │ True   ← HEAD         │
└────────────────────────────┴─────────────────────┴─────────────┴───────────────────────┘
```

`is_current_ancestor = False` on the first snapshot confirms `createOrReplace` cut a new
lineage root rather than appending — the orphan snapshot remains queryable until expiry.

**Why it matters:** Snapshot history enables compliance auditing, pipeline debugging, governance reviews, and recovery planning. Knowing exactly when each version was created — and whether it's still in the current lineage — answers "what did this table look like at Tuesday's close?" without external logging.

**Delta Lake edge:** Delta's `_delta_log` records richer operation metadata out of the box — operation type, user, cluster ID, and row-level metrics per commit. Iceberg's history is leaner but catalog-portable: the same `.history` query works identically on Spark, Trino, and DuckDB without platform dependency.

**Flow:** This query only touches Snapshot JSON — no Parquet files are read.

### 2. Time Travel
Queries the first (orphan) snapshot by ID even after the table was replaced:

```python
spark.sql(f"SELECT * FROM local.silver.orders_enriched VERSION AS OF 8667773288305875514")
```

The underlying Parquet file survives the replacement and is fully readable — zero data loss
until `expire_snapshots` is explicitly called.

**Why it matters:** Time travel is the safety net for accidental bad writes, audit requirements ("show me Q4 close-of-business data"), and reproducible pipeline debugging.

**Iceberg edge:** Snapshot IDs are globally unique 64-bit integers — not local sequential integers like Delta's version numbers. This matters for multi-table consistency, multi-engine safety (any engine references the same ID), and cross-cloud replication where sequential integers would collide.

**Flow:** Pinning at the Snapshot level automatically constrains all lower levels — Manifest List, Manifest Files, and Parquet data files all follow from the snapshot pointer.

### 3. Schema Evolution — Zero File Rewrite
`ALTER TABLE ... ADD COLUMN discount_pct DOUBLE` added a 21st column in milliseconds.
Iceberg tracks columns by **integer ID**, not name — so add, rename, and drop never
require rewriting Parquet files. Existing rows read `NULL` for the new column immediately.

```
Columns BEFORE (20): order_item_id, order_id, order_date, … _processed_at
Columns AFTER  (21): order_item_id, order_id, order_date, … _processed_at, discount_pct
```

The column persisted across the subsequent `pipeline.py` run (`createOrReplace`) because
Iceberg schema metadata lives outside the data files.

**Why it matters:** Adding columns without downtime or file rewrites means schema evolution is safe in production — no migration windows, no cloud cost spikes from rewriting petabyte tables.

**Bottom line:** Both formats support schema evolution. Iceberg makes ALL operations (add/rename/drop) safe by default — columns are tracked by integer ID, not name. Delta Lake requires opt-in column mapping mode (a protocol version upgrade) before rename and drop become non-destructive.

**Flow:** Only the Snapshot JSON is updated — Parquet files are never touched regardless of table size.

### 4. Partition Evolution
`SELECT * FROM local.silver.orders_enriched.partitions` returned `_partition: struct<>` —
the table is currently unpartitioned. In Iceberg, partitioning can be added at any time:

```sql
ALTER TABLE local.silver.orders_enriched ADD PARTITION FIELD order_year;
ALTER TABLE local.silver.orders_enriched ADD PARTITION FIELD order_month;
```

New writes use the new spec; existing files keep their original spec. The engine handles
mixed-spec reads transparently. **This is impossible without a full rewrite in Delta Lake.**

**Why it matters:** Partitioning decisions made at table creation become wrong as data volumes and query patterns change. Re-partitioning a 10 TB table without hours of rewrites and cloud costs is a meaningful operational advantage.

**Iceberg edge:** Partition evolution completes in milliseconds (0.23 seconds observed) vs. a full data rewrite in Delta Lake. The mixed `spec_id` state is a deliberate tradeoff — pay the rewrite cost when you choose to, via off-peak compaction using `rewrite_data_files`.

**Flow:** The only feature that changes multiple metadata levels — but old Parquet files are never rewritten. Only the partition spec in the Snapshot JSON is updated; new writes create new Manifest Files and Parquet under the new spec.

### 5. Table Metadata
Three metadata tables exposed as first-class Spark views:

| Metadata table | What it shows |
|---|---|
| `.files` | 1 Parquet file, 1,575 records, 43.9 KB |
| `.snapshots` | 2 snapshots (`append` operations), committed timestamps |
| `.manifests` | 1 Avro manifest, `added_data_files_count = 1`, zero deletes |

**Why it matters:** Instant table health checks — file counts, snapshot auditing, manifest inspection — all via standard SQL with no special tools. Useful for debugging and capacity planning without leaving your query engine.

**Both formats:** Both Iceberg and Delta Lake expose rich metadata. Iceberg uses a 3-layer architecture above Parquet (Snapshot → Manifest List → Manifest Files → Parquet). Delta Lake uses a simpler `_delta_log` JSON commit structure with inline statistics.

**Flow:** Metadata queries (`.files`, `.snapshots`, `.manifests`) stop at Level 3 — they never touch Parquet data files. Results are instantaneous regardless of table size.

---

## Metadata Architecture

Iceberg uses a 5-level hierarchy separating catalog pointer from actual data:

```
Catalog
  └── Snapshot  (v1.metadata.json, v2.metadata.json …)
        └── Manifest List  (snap-<id>-<uuid>.avro)
              └── Manifest Files  (<uuid>-m0.avro …)
                    └── Parquet Data Files
```

Each level is immutable once written. When a write completes, only the Snapshot pointer updates — lower levels are reused, never modified. This immutability is what makes time travel, schema evolution, and partition evolution metadata-only operations: you always add new metadata, never patch old files.

### Which Levels Each Feature Touches

| Feature | Catalog | Snapshot | Manifest List | Manifest Files | Parquet |
|---|:---:|:---:|:---:|:---:|:---:|
| Table History | — | ✅ | — | — | — |
| Time Travel | — | ✅ | ✅ | ✅ | ✅ |
| Schema Evolution | — | ✅ | — | — | — |
| Partition Evolution | — | ✅ | ✅ | ✅ | ✅ |
| Table Metadata | ✅ | ✅ | ✅ | ✅ | — |

**Key insight:** Table History and Schema Evolution touch only the Snapshot layer — milliseconds regardless of table size. Time Travel and Partition Evolution traverse all levels down to Parquet (read-only for travel; new files only for evolution). Table Metadata queries stop just before Parquet — instantaneous even on tables with billions of rows.

---

## Running Scorecard

| Feature | Winner | Reason |
|---|---|---|
| Table History | **Delta Lake edge** | Richer operation metadata out of the box (type, user, cluster, row metrics) |
| Time Travel | **Iceberg edge** | Globally unique content-addressed snapshot IDs vs. local sequential integers |
| Schema Evolution | **Iceberg edge** | All ops safe by default; Delta requires opt-in column mapping mode |
| Partition Evolution | **Iceberg edge** | Milliseconds vs. full data rewrite; metadata-only by design |
| Table Metadata | **Tie** | Both expose rich metadata; different architecture, equivalent utility |

**Overall: Iceberg 3 — Delta Lake 1 — Tie 1** (on this feature set)

> This scorecard covers the five features in `explore_iceberg.py`. Delta Lake leads on DLT data quality, Structured Streaming maturity, and Unity Catalog governance — dimensions outside this feature set.

---

## Key Findings

### Iceberg's 3 Biggest Advantages

**1. Partition evolution without data rewrite.**
Changing a partition scheme on a large Delta table requires rewriting all data files.
Iceberg stores the partition spec per snapshot — evolving partitioning is a metadata
operation that takes milliseconds regardless of table size.

**2. Schema evolution is safe by default.**
Every column operation (add, rename, drop, type widen) is safe in Iceberg because columns
are tracked by integer ID, not name. Delta Lake requires opting into column mapping mode
(a protocol version upgrade) before rename/drop become non-destructive.

**3. True multi-engine interoperability.**
The same `./iceberg_warehouse/` folder is readable by Spark, DuckDB, Trino, Flink, and
PyArrow without any format conversion or sharing protocol. Delta Lake's best multi-engine
story requires Delta Sharing or UniForm (which re-exposes Delta as Iceberg anyway).

### Delta Lake's 3 Biggest Advantages

**1. Delta Live Tables (DLT) — integrated data quality.**
`@dlt.expect`, `@dlt.expect_or_drop`, and `@dlt.expect_or_fail` give declarative,
monitored data quality with zero extra tooling. Iceberg has no equivalent; you reach for
Great Expectations, deequ, or hand-rolled DataFrame assertions.

**2. Structured Streaming is a first-class citizen.**
Delta + Spark Structured Streaming is battle-tested at massive scale, with Change Data Feed
exposing row-level changes for CDC pipelines. Iceberg streaming is production-grade on
Flink but the Spark streaming integration is less mature.

**3. Unity Catalog — governed lakehouse out of the box.**
Fine-grained access control, column-level security, data lineage, and audit logs come
built-in on Databricks. Getting comparable governance on Iceberg requires assembling
Nessie / Polaris + a separate access control layer.

### Architecture Insights

**4. Iceberg's metadata architecture is what enables all its advanced features.**
The 3-layer hierarchy above Parquet (Snapshot → Manifest List → Manifest Files) creates the
separation that makes time travel, schema evolution, and partition evolution possible without data
rewrites. Immutability is the foundation — every operation adds new metadata, never modifies
existing files.

**5. Old Parquet files are never rewritten in any Iceberg operation.**
Immutability is absolute. Whether you add a column, change the partition spec, or create a new
snapshot, existing Parquet files are untouched. New files are written alongside old ones;
compaction (`rewrite_data_files`) is an explicit, opt-in operation.

**6. Partition evolution creates temporary mixed-spec state — by design.**
After `ADD PARTITION FIELD`, old files retain their original spec while new writes use the new
spec. The query engine handles this transparently. The mixed state is resolved via off-peak
compaction when you choose to pay the rewrite cost — not forced on you at partition change time.

### When to Choose Each

| Choose Delta Lake when… | Choose Apache Iceberg when… |
|---|---|
| Your team is on Databricks | You need multi-engine (Flink + Trino + Spark) |
| You need DLT data quality with UI metrics | Partition evolution without rewrites is required |
| Structured Streaming + CDC are core | Vendor / cloud independence matters |
| Unity Catalog governance is a requirement | Local development with zero infrastructure |
| You're in Azure / AWS Databricks ecosystem | Snowflake, AWS, or GCP as the primary platform |

---

## How to Run

### Prerequisites
- Python 3.9+
- Java 11 or 17 (required by PySpark — check with `java -version`)

### Setup

```bash
# Clone
git clone https://github.com/venkatchittoor/iceberg-lakehouse-comparison.git
cd iceberg-lakehouse-comparison

# Install dependencies
pip install -r requirements.txt

# Copy env (no credentials needed — runs fully local)
cp .env.example .env
```

### Execute

```bash
# 1. Generate synthetic e-commerce data → ./data/*.csv
python generate_data.py

# 2. Run the Medallion pipeline → ./iceberg_warehouse/
python pipeline.py

# 3. Explore Iceberg-specific features (time travel, schema evolution, metadata)
python explore_iceberg.py
```

> The Iceberg Spark runtime JAR (`iceberg-spark-runtime-3.5_2.12:1.5.2`, ~40 MB) is
> downloaded automatically via `spark.jars.packages` on first run and cached in `~/.ivy2/`.
> Subsequent runs start in seconds.

### Iceberg Catalog Config (local Hadoop)

```
spark.sql.catalog.local          = org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.local.type     = hadoop
spark.sql.catalog.local.warehouse = ./iceberg_warehouse
```

No Hive metastore, no REST catalog server, no Docker — just the filesystem.

---

## Related Project

**[ecommerce-pipeline](https://github.com/venkatchittoor/ecommerce-pipeline)** — the Delta Lake
side of this comparison: the same e-commerce dataset built as a Databricks Medallion pipeline
using Delta Live Tables, Unity Catalog, and `@dlt.expect` data quality constraints.

---

## Author

**Venkat Chittoor**
[github.com/venkatchittoor](https://github.com/venkatchittoor)
