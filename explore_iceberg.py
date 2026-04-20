"""
Iceberg-specific feature exploration
Demonstrates capabilities not available in basic Delta Lake:
  1. Time Travel         — query a table AS OF a past snapshot
  2. Table History       — full snapshot log with timestamps and operations
  3. Schema Evolution    — add a column live, zero data rewrite
  4. Partition Evolution — inspect the current partition spec
  5. Table Metadata      — files, snapshots, and manifests counts
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from tabulate import tabulate

WAREHOUSE = os.getenv("WAREHOUSE_PATH", "./iceberg_warehouse")
CATALOG   = "local"
TABLE     = f"{CATALOG}.silver.orders_enriched"


# ---------------------------------------------------------------------------
# SparkSession (same config as pipeline.py)
# ---------------------------------------------------------------------------

def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("IcebergExplorer")
        .config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2",
        )
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", WAREHOUSE)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.showConsoleProgress", "false")
        .master("local[*]")
        .getOrCreate()
    )


def section(title: str):
    print()
    print("=" * 65)
    print(f"  {title}")
    print("=" * 65)


def show(df, limit: int = 5):
    rows = [list(r) for r in df.limit(limit).collect()]
    print(tabulate(rows, headers=df.columns, tablefmt="rounded_outline"))


# ---------------------------------------------------------------------------
# 1. TABLE HISTORY — list every snapshot
# ---------------------------------------------------------------------------

def show_history(spark: SparkSession):
    section("1. TABLE HISTORY — all snapshots")

    history = spark.sql(f"SELECT * FROM {TABLE}.history ORDER BY made_current_at")
    show(history, limit=20)

    snapshots = history.collect()
    print(f"\n  Total snapshots: {len(snapshots)}")
    return snapshots


# ---------------------------------------------------------------------------
# 2. TIME TRAVEL — query AS OF the first snapshot
# ---------------------------------------------------------------------------

def time_travel(spark: SparkSession, snapshots):
    section("2. TIME TRAVEL — query AS OF first snapshot")

    if not snapshots:
        print("  No snapshots found — run pipeline.py first.")
        return

    first_snapshot_id = snapshots[0]["snapshot_id"]
    first_ts          = snapshots[0]["made_current_at"]
    print(f"  Querying snapshot_id={first_snapshot_id}  ({first_ts})")

    df_past = spark.sql(
        f"SELECT order_id, customer_name, category, subtotal "
        f"FROM {TABLE} VERSION AS OF {first_snapshot_id} "
        f"LIMIT 5"
    )
    show(df_past)

    current_count = spark.table(TABLE).count()
    past_count    = spark.sql(
        f"SELECT COUNT(*) AS cnt FROM {TABLE} VERSION AS OF {first_snapshot_id}"
    ).collect()[0]["cnt"]

    print(f"\n  Rows at snapshot {first_snapshot_id} : {past_count:,}")
    print(f"  Rows at HEAD (current)              : {current_count:,}")


# ---------------------------------------------------------------------------
# 3. SCHEMA EVOLUTION — add a column without rewriting data
# ---------------------------------------------------------------------------

def schema_evolution(spark: SparkSession):
    section("3. SCHEMA EVOLUTION — add column (no data rewrite)")

    before = spark.table(TABLE).columns
    print(f"  Columns BEFORE ({len(before)}): {', '.join(before)}")

    # Add a nullable column — Iceberg stores it in metadata only
    spark.sql(f"ALTER TABLE {TABLE} ADD COLUMN discount_pct DOUBLE")

    after = spark.table(TABLE).columns
    print(f"  Columns AFTER  ({len(after)}): {', '.join(after)}")
    print()
    print("  New column 'discount_pct' added — existing rows read NULL, zero files rewritten.")

    # Populate the new column for illustration
    spark.sql(f"""
        ALTER TABLE {TABLE} ALTER COLUMN discount_pct
        COMMENT 'Illustrative discount percentage (added via schema evolution)'
    """)

    sample = spark.sql(
        f"SELECT order_id, subtotal, discount_pct FROM {TABLE} LIMIT 5"
    )
    show(sample)


# ---------------------------------------------------------------------------
# 4. PARTITION EVOLUTION — inspect the current partition spec
# ---------------------------------------------------------------------------

def partition_evolution(spark: SparkSession):
    section("4. PARTITION EVOLUTION — current partition spec")

    parts = spark.sql(f"SELECT * FROM {TABLE}.partitions")

    if parts.rdd.isEmpty():
        print("  Table is currently UNPARTITIONED (no partition spec applied).")
        print()
        print("  In Iceberg you can evolve partitioning at any time with:")
        print(f"    ALTER TABLE {TABLE} ADD PARTITION FIELD order_year")
        print(f"    ALTER TABLE {TABLE} ADD PARTITION FIELD order_month")
        print()
        print("  New writes use the new spec; old files keep their original spec.")
        print("  Iceberg handles mixed-spec reads transparently — no full rewrite.")
    else:
        show(parts)
        print(f"\n  Total partition files: {parts.count():,}")

    # Show the spec as reported by DESCRIBE EXTENDED
    desc = spark.sql(f"DESCRIBE EXTENDED {TABLE}")
    spec_rows = [
        list(r) for r in desc.collect()
        if r["col_name"].strip().lower() in ("# partitioning", "part 0", "part 1")
           or "partition" in r["col_name"].lower()
    ]
    if spec_rows:
        print()
        print(tabulate(spec_rows, headers=["col_name", "data_type", "comment"],
                       tablefmt="rounded_outline"))


# ---------------------------------------------------------------------------
# 5. TABLE METADATA — files, snapshots, manifests
# ---------------------------------------------------------------------------

def table_metadata(spark: SparkSession):
    section("5. TABLE METADATA — files / snapshots / manifests")

    # Data files
    files_df = spark.sql(f"SELECT file_path, file_format, record_count, file_size_in_bytes "
                         f"FROM {TABLE}.files")
    file_rows     = files_df.count()
    total_records = files_df.agg(F.sum("record_count")).collect()[0][0] or 0
    total_bytes   = files_df.agg(F.sum("file_size_in_bytes")).collect()[0][0] or 0

    print(f"\n  Data files   : {file_rows}")
    print(f"  Total records: {total_records:,}")
    print(f"  Total size   : {total_bytes / 1024:.1f} KB")
    print()
    print("  Sample files:")
    show(files_df.select(
        F.regexp_extract("file_path", r"[^/]+\.parquet$", 0).alias("filename"),
        "file_format", "record_count", "file_size_in_bytes"
    ))

    # Snapshots
    snaps_df   = spark.sql(f"SELECT snapshot_id, committed_at, operation "
                           f"FROM {TABLE}.snapshots ORDER BY committed_at")
    snap_count = snaps_df.count()
    print(f"\n  Snapshots ({snap_count}):")
    show(snaps_df, limit=20)

    # Manifests
    manifests_df    = spark.sql(f"SELECT path, length, partition_spec_id, "
                                f"added_data_files_count, existing_data_files_count, "
                                f"deleted_data_files_count "
                                f"FROM {TABLE}.manifests")
    manifest_count  = manifests_df.count()
    print(f"\n  Manifests ({manifest_count}):")
    show(manifests_df.select(
        F.regexp_extract("path", r"[^/]+-m\d+\.avro$", 0).alias("manifest_file"),
        "partition_spec_id", "added_data_files_count",
        "existing_data_files_count", "deleted_data_files_count"
    ), limit=20)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    print(f"\nExploring Iceberg table: {TABLE}")
    print(f"Warehouse: {WAREHOUSE}")

    snapshots = show_history(spark)
    time_travel(spark, snapshots)
    schema_evolution(spark)
    partition_evolution(spark)
    table_metadata(spark)

    section("Done")
    spark.stop()


if __name__ == "__main__":
    main()
