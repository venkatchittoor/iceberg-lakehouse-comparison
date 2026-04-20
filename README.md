# Iceberg Lakehouse — Medallion Architecture on Apache Iceberg

A local end-to-end Medallion Architecture pipeline using **PySpark 3.5** and **Apache Iceberg**, with no cloud dependencies.

## Architecture

```
CSV files (./data/)
      │
      ▼
 BRONZE layer  — raw ingest, schema-as-read, _ingested_at watermark
      │
      ▼
 SILVER layer  — joined, cleaned, cancelled orders filtered, margins computed
      │
      ▼
 GOLD layer    — 3 aggregation tables ready for BI / reporting
```

## Layers

| Layer  | Table(s)                                                              |
|--------|-----------------------------------------------------------------------|
| Bronze | `customers`, `products`, `orders`, `order_items`                     |
| Silver | `orders_enriched`                                                     |
| Gold   | `revenue_by_category`, `top_customers`, `product_performance`        |

## Quickstart

```bash
# 1. install dependencies
pip install -r requirements.txt

# 2. copy env (no credentials needed — runs fully local)
cp .env.example .env

# 3. generate synthetic data
python generate_data.py

# 4. run the pipeline
python pipeline.py
```

## Iceberg Catalog

Uses a **local Hadoop catalog** backed by the filesystem at `./iceberg_warehouse/`.  
No Hive metastore or REST catalog is required.

```
spark.sql.catalog.local                  = org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.local.type             = hadoop
spark.sql.catalog.local.warehouse        = ./iceberg_warehouse
```

## Requirements

- Python 3.9+
- Java 11 or 17 (required by PySpark)
