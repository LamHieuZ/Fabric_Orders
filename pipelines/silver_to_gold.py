from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

from pyspark.sql.functions import (
    col,
    sum as _sum,
    avg,
    count,
    month,
    year,
    max as _max,
    min as _min,
    datediff,
    current_date,
    lit
)

# ======================================================
# SPARK SESSION
# ======================================================
def build_spark():
    return (
        SparkSession.builder
        .appName("silver_to_gold")

        # Iceberg + Nessie
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        )
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config(
            "spark.sql.catalog.iceberg.catalog-impl",
            "org.apache.iceberg.nessie.NessieCatalog"
        )
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

    print("Loading SILVER.orders_header ...")
    header = spark.table("iceberg.silver.orders_header")

    print(f"orders_header rows: {header.count()}")

    # ==================================================
    # 1) GOLD.PRODUCT_MONTHLY (HEADER-BASED)
    # ==================================================
    print("Creating gold.product_monthly (header-based) ...")

    product_monthly = (
        header
        .withColumn("year", year("orderDate"))
        .withColumn("month", month("orderDate"))
        .groupBy("year", "month")
        .agg(
            count("orderId").alias("totalOrders"),
            _sum("freight").alias("totalRevenue"),
            avg("freight").alias("avgOrderValue")
        )
        .orderBy("year", "month")
    )

    product_monthly.writeTo(
        "iceberg.gold.product_monthly"
    ).createOrReplace()

    print("✓ Saved: iceberg.gold.product_monthly")

    # ==================================================
    # 2) GOLD.CUSTOMER_RFM (HEADER-BASED)
    # ==================================================
    print("Creating gold.customer_rfm (header-based) ...")

    customer_rfm = (
        header
        .groupBy("customerIdHash")
        .agg(
            _max("orderDate").alias("lastOrder"),
            count("orderId").alias("frequency"),
            _sum("freight").alias("monetary")
        )
        .withColumn(
            "recency",
            datediff(current_date(), col("lastOrder"))
        )
    )

    customer_rfm.writeTo(
        "iceberg.gold.customer_rfm"
    ).createOrReplace()

    print("✓ Saved: iceberg.gold.customer_rfm")

    # ==================================================
    # 3) GOLD.PRODUCT_ANOMALY (HEADER-BASED)
    # ==================================================
    print("Creating gold.product_anomaly (header-based) ...")

    product_basic = (
        header
        .groupBy("shipperID")
        .agg(
            avg("freight").alias("avgPrice"),
            _min("freight").alias("minPrice"),
            _max("freight").alias("maxPrice"),
            count("orderId").alias("qtySold")
        )
    )

    # 👉 Tính trung bình qtySold toàn bảng
    avg_qty = product_basic.select(avg("qtySold")).collect()[0][0]

    product_anomaly = (
        product_basic
        .withColumn(
            "lowPriceAnomaly",
            col("minPrice") < col("avgPrice") * 0.4
        )
        .withColumn(
            "highPriceAnomaly",
            col("maxPrice") > col("avgPrice") * 2.0
        )
        .withColumn(
            "highQuantityAnomaly",
            col("qtySold") > lit(avg_qty)
        )
    )

    product_anomaly.writeTo(
        "iceberg.gold.product_anomaly"
    ).createOrReplace()

    print("✓ Saved: iceberg.gold.product_anomaly")


    # ==================================================
    # 4) GOLD.ORDERS (FOR DBSCAN)
    # ==================================================
    print("Creating gold.orders (DBSCAN input) ...")

    gold_orders = (
        header
        .select(
            "orderId",
            "orderDate",
            "requiredDate",
            "shippedDate",
            "freight",
            "approvedByOrg1"
        )
        .withColumn("total_amount", col("freight"))
        .withColumn("avg_price", col("freight"))
        .withColumn("num_items", lit(1))
    )

    gold_orders.writeTo(
        "iceberg.gold.orders"
    ).createOrReplace()

    print("✓ Saved: iceberg.gold.orders")

    print("ALL GOLD TABLES CREATED SUCCESSFULLY!")

# ======================================================
if __name__ == "__main__":
    main()
