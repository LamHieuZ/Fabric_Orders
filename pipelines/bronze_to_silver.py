import json
import io
from minio import Minio

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode, to_timestamp, when
)

# ======================================================
# 0) CONFIG
# ======================================================
BRONZE_BUCKET = "fabric"
BRONZE_PREFIX = "bronze/orders/"

WAREHOUSE_BUCKET = "warehouse"

MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_SECURE = False

NESSIE_URI = "http://nessie:19120/api/v1"
NESSIE_REF = "main"

# ======================================================
# 1) MINIO CLIENT
# ======================================================
minio = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=MINIO_SECURE
)

def ensure_bucket(bucket: str):
    if not minio.bucket_exists(bucket):
        minio.make_bucket(bucket)

def ensure_warehouse_ready():
    ensure_bucket(WAREHOUSE_BUCKET)
    try:
        minio.put_object(
            WAREHOUSE_BUCKET,
            "README.txt",
            data=io.BytesIO(b"Iceberg warehouse root"),
            length=len(b"Iceberg warehouse root"),
            content_type="text/plain"
        )
    except Exception:
        pass

# ======================================================
# 2) LOAD BRONZE FILES (RAW JSON)
# ======================================================
def load_bronze_json_lines():
    if not minio.bucket_exists(BRONZE_BUCKET):
        return []

    lines = []
    for obj in minio.list_objects(
        BRONZE_BUCKET,
        prefix=BRONZE_PREFIX,
        recursive=True
    ):
        if obj.object_name.endswith("/"):
            continue

        try:
            raw = minio.get_object(
                BRONZE_BUCKET,
                obj.object_name
            ).read().decode("utf-8")

            data = json.loads(raw)

            if isinstance(data, list):
                for rec in data:
                    lines.append(json.dumps(rec))
            elif isinstance(data, dict):
                lines.append(json.dumps(data))
        except Exception as e:
            print(f"Skip bad file {obj.object_name}: {e}")

    return lines

# ======================================================
# 3) TRANSFORM BRONZE → SILVER
# ======================================================
def transform_bronze_to_silver(df):
    # ---------------- orders_header ----------------
    df_header = (
        df.select(
            col("orderId").cast("string").alias("orderId"),
            col("customerIdHash").cast("string").alias("customerIdHash"),
            col("employee").cast("int").alias("employee"),
            to_timestamp(col("orderDate")).alias("orderDate"),
            to_timestamp(col("requiredDate")).alias("requiredDate"),
            to_timestamp(col("shippedDate")).alias("shippedDate"),
            col("shipperID").cast("int").alias("shipperID"),
            col("freight").cast("double").alias("freight"),
            col("status").cast("string").alias("status"),
            col("approvedByOrg1").cast("boolean").alias("approvedByOrg1")
        )
    )

    # ---------------- orders_items ----------------
    df_items = (
        df
        .filter(col("OrderDetail").isNotNull())
        .filter(col("OrderDetail.items").isNotNull())
        .select(
            col("orderId").cast("string").alias("orderId"),
            explode(col("OrderDetail.items")).alias("item")
        )
        .select(
            col("orderId"),
            col("item.productId").cast("string").alias("productId"),
            col("item.quantity").cast("int").alias("quantity"),
            col("item.unitPrice").cast("double").alias("unitPrice"),
            col("item.discount").cast("double").alias("discount"),
            col("item.price").cast("double").alias("price")
        )
    )

    # ---------------- cleaning + derived ----------------
    df_items = (
        df_items
        .filter(col("quantity") > 0)
        .filter(col("unitPrice") >= 0)
        .withColumn(
            "discount",
            when((col("discount") < 0) | (col("discount") > 1), 0)
            .otherwise(col("discount"))
        )
        .withColumn(
            "totalPrice",
            col("quantity") * col("unitPrice") * (1 - col("discount"))
        )
    )

    return df_header, df_items

# ======================================================
# 4) SPARK SESSION
# ======================================================
def build_spark():
    return (
        SparkSession.builder
        .appName("bronze_to_silver")

        .config("spark.sql.caseSensitive", "true")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        )

        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config(
            "spark.sql.catalog.iceberg.catalog-impl",
            "org.apache.iceberg.nessie.NessieCatalog"
        )
        .config("spark.sql.catalog.iceberg.uri", NESSIE_URI)
        .config("spark.sql.catalog.iceberg.ref", NESSIE_REF)
        .config(
            "spark.sql.catalog.iceberg.warehouse",
            f"s3a://{WAREHOUSE_BUCKET}/"
        )

        .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

        .getOrCreate()
    )

# ======================================================
# 5) MAIN
# ======================================================
def main():
    ensure_warehouse_ready()
    spark = build_spark()

    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.silver")

    json_lines = load_bronze_json_lines()
    if not json_lines:
        print("No bronze data → stop")
        return

    rdd = spark.sparkContext.parallelize(json_lines)
    df_bronze = spark.read.json(rdd)

    df_header, df_items = transform_bronze_to_silver(df_bronze)

    df_header.writeTo(
        "iceberg.silver.orders_header"
    ).createOrReplace()

    df_items.writeTo(
        "iceberg.silver.orders_items"
    ).createOrReplace()

    print("orders_header rows:", df_header.count())
    print("orders_items rows :", df_items.count())

    spark.stop()

# ======================================================
if __name__ == "__main__":
    main()
