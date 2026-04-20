"""
Iceberg Medallion Architecture Pipeline
BRONZE -> SILVER -> GOLD using PySpark + Apache Iceberg (local hadoop catalog)
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DoubleType, TimestampType
)
from tabulate import tabulate

DATA_DIR = "./data"
WAREHOUSE = os.getenv("WAREHOUSE_PATH", "./iceberg_warehouse")
CATALOG = "local"


# ---------------------------------------------------------------------------
# SparkSession with Iceberg extensions and local hadoop catalog
# ---------------------------------------------------------------------------

def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("IcebergMedallionPipeline")
        .config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2",
        )
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        # local hadoop-based catalog (no Hive / REST needed)
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", WAREHOUSE)
        # reduce noise
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.showConsoleProgress", "false")
        .master("local[*]")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def drop_and_create_namespace(spark: SparkSession, namespace: str):
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{namespace}")


def preview(spark: SparkSession, table: str, n: int = 5):
    df = spark.table(table)
    rows = [list(r) for r in df.limit(n).collect()]
    cols = df.columns
    print(tabulate(rows, headers=cols, tablefmt="rounded_outline"))
    print(f"  total rows: {df.count():,}\n")


# ---------------------------------------------------------------------------
# BRONZE — ingest raw CSVs as-is into Iceberg tables
# ---------------------------------------------------------------------------

def bronze(spark: SparkSession):
    print("=" * 60)
    print("BRONZE — ingesting raw CSVs")
    print("=" * 60)

    drop_and_create_namespace(spark, "bronze")

    tables = {
        "customers": f"{DATA_DIR}/customers.csv",
        "products":  f"{DATA_DIR}/products.csv",
        "orders":    f"{DATA_DIR}/orders.csv",
        "order_items": f"{DATA_DIR}/order_items.csv",
    }

    for name, path in tables.items():
        target = f"{CATALOG}.bronze.{name}"
        df = spark.read.option("header", True).option("inferSchema", True).csv(path)
        df = df.withColumn("_ingested_at", F.current_timestamp())

        (
            df.writeTo(target)
            .tableProperty("write.format.default", "parquet")
            .createOrReplace()
        )
        print(f"\n[bronze.{name}]")
        preview(spark, target)


# ---------------------------------------------------------------------------
# SILVER — join, cast, and clean into a single enriched orders fact table
# ---------------------------------------------------------------------------

def silver(spark: SparkSession):
    print("=" * 60)
    print("SILVER — joining and cleaning")
    print("=" * 60)

    drop_and_create_namespace(spark, "silver")

    customers  = spark.table(f"{CATALOG}.bronze.customers")
    products   = spark.table(f"{CATALOG}.bronze.products")
    orders     = spark.table(f"{CATALOG}.bronze.orders")
    order_items = spark.table(f"{CATALOG}.bronze.order_items")

    # filter out cancelled orders
    orders_clean = orders.filter(F.col("status") != "cancelled")

    enriched = (
        order_items
        .join(orders_clean,  "order_id")
        .join(customers,     "customer_id")
        .join(products,      "product_id")
        .select(
            order_items["order_item_id"],
            orders_clean["order_id"],
            orders_clean["order_date"].cast(TimestampType()),
            orders_clean["status"],
            customers["customer_id"],
            F.concat_ws(" ", customers["first_name"], customers["last_name"]).alias("customer_name"),
            customers["city"].alias("customer_city"),
            customers["state"].alias("customer_state"),
            products["product_id"],
            products["product_name"],
            products["category"],
            products["price"].alias("list_price"),
            order_items["unit_price"],
            order_items["quantity"],
            order_items["subtotal"],
            (order_items["unit_price"] - products["cost"]).alias("unit_margin"),
            (order_items["subtotal"] - products["cost"] * order_items["quantity"]).alias("line_margin"),
            F.year(orders_clean["order_date"]).alias("order_year"),
            F.month(orders_clean["order_date"]).alias("order_month"),
            F.current_timestamp().alias("_processed_at"),
        )
    )

    target = f"{CATALOG}.silver.orders_enriched"
    (
        enriched.writeTo(target)
        .tableProperty("write.format.default", "parquet")
        .createOrReplace()
    )

    print(f"\n[silver.orders_enriched]")
    preview(spark, target)


# ---------------------------------------------------------------------------
# GOLD — three aggregation tables for reporting
# ---------------------------------------------------------------------------

def gold(spark: SparkSession):
    print("=" * 60)
    print("GOLD — building aggregation tables")
    print("=" * 60)

    drop_and_create_namespace(spark, "gold")

    silver_df = spark.table(f"{CATALOG}.silver.orders_enriched")

    # --- 1. Revenue by category per month ---
    revenue_by_category = (
        silver_df
        .groupBy("order_year", "order_month", "category")
        .agg(
            F.sum("subtotal").alias("total_revenue"),
            F.sum("line_margin").alias("total_margin"),
            F.count("order_item_id").alias("num_items"),
            F.countDistinct("order_id").alias("num_orders"),
        )
        .withColumn("margin_pct",
            F.round(F.col("total_margin") / F.col("total_revenue") * 100, 2))
        .orderBy("order_year", "order_month", "category")
    )

    t1 = f"{CATALOG}.gold.revenue_by_category"
    revenue_by_category.writeTo(t1).tableProperty("write.format.default", "parquet").createOrReplace()
    print(f"\n[gold.revenue_by_category]")
    preview(spark, t1)

    # --- 2. Top customers by lifetime value ---
    top_customers = (
        silver_df
        .groupBy("customer_id", "customer_name", "customer_city", "customer_state")
        .agg(
            F.sum("subtotal").alias("lifetime_value"),
            F.sum("line_margin").alias("total_margin"),
            F.countDistinct("order_id").alias("num_orders"),
            F.sum("quantity").alias("total_items"),
        )
        .withColumn("avg_order_value",
            F.round(F.col("lifetime_value") / F.col("num_orders"), 2))
        .orderBy(F.col("lifetime_value").desc())
    )

    t2 = f"{CATALOG}.gold.top_customers"
    top_customers.writeTo(t2).tableProperty("write.format.default", "parquet").createOrReplace()
    print(f"\n[gold.top_customers]")
    preview(spark, t2)

    # --- 3. Product performance summary ---
    product_performance = (
        silver_df
        .groupBy("product_id", "product_name", "category")
        .agg(
            F.sum("subtotal").alias("total_revenue"),
            F.sum("quantity").alias("total_units_sold"),
            F.sum("line_margin").alias("total_margin"),
            F.countDistinct("order_id").alias("num_orders"),
            F.avg("unit_price").alias("avg_selling_price"),
        )
        .withColumn("margin_pct",
            F.round(F.col("total_margin") / F.col("total_revenue") * 100, 2))
        .orderBy(F.col("total_revenue").desc())
    )

    t3 = f"{CATALOG}.gold.product_performance"
    product_performance.writeTo(t3).tableProperty("write.format.default", "parquet").createOrReplace()
    print(f"\n[gold.product_performance]")
    preview(spark, t3)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    bronze(spark)
    silver(spark)
    gold(spark)

    print("=" * 60)
    print("Pipeline complete.")
    print(f"Iceberg warehouse: {WAREHOUSE}")
    print("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()
