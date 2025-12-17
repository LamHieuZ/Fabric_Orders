from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as _sum, avg, count,
    month, year, max as _max, min as _min,
    datediff, current_date, first, lit
)

# ======================================================
# SPARK SESSION
# ======================================================
def build_spark():
    return (
        SparkSession.builder
        .appName("silver_to_gold")

        # Iceberg + Nessie
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .config("spark.sql.catalog.iceberg.uri", "http://nessie:19120/api/v1")
        .config("spark.sql.catalog.iceberg.ref", "main")
        .config("spark.sql.catalog.iceberg.warehouse", "s3a://warehouse/")

        # MinIO
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

        .getOrCreate()
    )

# ======================================================
# MAIN
# ======================================================
def main():
    spark = build_spark()

    print(" Loading Silver tables ...")
    header = spark.table("iceberg.silver.orders_header")
    items  = spark.table("iceberg.silver.orders_items")

    header_count = header.count()
    items_count  = items.count()

    print(f"orders_header rows: {header_count}")
    print(f"orders_items rows : {items_count}")

    # ==================================================
    # 1) PRODUCT MONTHLY (ITEM LEVEL) — GUARD
    # ==================================================
    if items_count > 0:
        print("Creating gold.product_monthly ...")

        product_monthly = (
            items.join(header.select("orderId", "orderDate"), "orderId")
            .withColumn("year", year("orderDate"))
            .withColumn("month", month("orderDate"))
            .groupBy("productId", "year", "month")
            .agg(
                _sum("quantity").alias("totalSold"),
                _sum("price").alias("totalRevenue"),
                avg("unitPrice").alias("avgPrice")
            )
            .orderBy("productId", "year", "month")
        )

        product_monthly.writeTo("iceberg.gold.product_monthly").createOrReplace()
        print(" Saved: iceberg.gold.product_monthly")
    else:
        print(" orders_items empty → skip gold.product_monthly")

    # ==================================================
    # 2) CUSTOMER RFM — GUARD
    # ==================================================
    if items_count > 0:
        print("Creating gold.customer_rfm ...")

        rfm = (
            items.join(
                header.select("orderId", "customerIdHash", "orderDate"),
                "orderId"
            )
            .groupBy("customerIdHash")
            .agg(
                _max("orderDate").alias("lastOrder"),
                count("orderId").alias("frequency"),
                _sum("price").alias("monetary")
            )
            .withColumn("recency", datediff(current_date(), col("lastOrder")))
        )

        rfm.writeTo("iceberg.gold.customer_rfm").createOrReplace()
        print(" Saved: iceberg.gold.customer_rfm")
    else:
        print(" orders_items empty → skip gold.customer_rfm")

    # ==================================================
    # 3) PRODUCT ANOMALY — GUARD
    # ==================================================
    if items_count > 0:
        print("Creating gold.product_anomaly ...")

        product_basic = (
            items.groupBy("productId")
            .agg(
                avg("unitPrice").alias("avgPrice"),
                _min("unitPrice").alias("minPrice"),
                _max("unitPrice").alias("maxPrice"),
                avg("quantity").alias("avgQuantity"),
                _sum("quantity").alias("qtySold")
            )
        )

        anomalies = (
            product_basic
            .withColumn("lowPriceAnomaly", col("minPrice") < col("avgPrice") * 0.4)
            .withColumn("highPriceAnomaly", col("maxPrice") > col("avgPrice") * 2.0)
            .withColumn("highQuantityAnomaly", col("qtySold") > col("avgQuantity") * 3.0)
        )

        anomalies.writeTo("iceberg.gold.product_anomaly").createOrReplace()
        print("✓ Saved: iceberg.gold.product_anomaly")
    else:
        print("⚠ orders_items empty → skip gold.product_anomaly")

    # ==================================================
    # 4) GOLD ORDERS — FOR DBSCAN (HEADER ONLY)
    # ==================================================
    print("Creating gold.orders (DBSCAN input) ...")

    gold_orders = (
        header.select(
            "orderId",
            "orderDate",
            "requiredDate",
            "shippedDate",
            "freight",
            "approvedByOrg1"
        )
        # proxy features (do items chưa có)
        .withColumn("total_amount", col("freight"))
        .withColumn("avg_price", col("freight"))
        .withColumn("num_items", lit(1))
    )

    gold_orders.writeTo("iceberg.gold.orders").createOrReplace()
    print(" Saved: iceberg.gold.orders")

    print(" ALL GOLD TABLES CREATED SUCCESSFULLY!")

# ======================================================
if __name__ == "__main__":
    main()
