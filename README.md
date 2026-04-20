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

### 2. Time Travel
Queries the first (orphan) snapshot by ID even after the table was replaced:

```python
spark.sql(f"SELECT * FROM local.silver.orders_enriched VERSION AS OF 8667773288305875514")
```

The underlying Parquet file survives the replacement and is fully readable — zero data loss
until `expire_snapshots` is explicitly called.

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

### 4. Partition Evolution
`SELECT * FROM local.silver.orders_enriched.partitions` returned `_partition: struct<>` —
the table is currently unpartitioned. In Iceberg, partitioning can be added at any time:

```sql
ALTER TABLE local.silver.orders_enriched ADD PARTITION FIELD order_year;
ALTER TABLE local.silver.orders_enriched ADD PARTITION FIELD order_month;
```

New writes use the new spec; existing files keep their original spec. The engine handles
mixed-spec reads transparently. **This is impossible without a full rewrite in Delta Lake.**

### 5. Table Metadata
Three metadata tables exposed as first-class Spark views:

| Metadata table | What it shows |
|---|---|
| `.files` | 1 Parquet file, 1,575 records, 43.9 KB |
| `.snapshots` | 2 snapshots (`append` operations), committed timestamps |
| `.manifests` | 1 Avro manifest, `added_data_files_count = 1`, zero deletes |

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
