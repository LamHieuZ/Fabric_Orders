import json
import io
from minio import Minio

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, to_timestamp, when
from pyspark.sql.types import *

# ======================================================
# 0) CONFIG
# ======================================================
BRONZE_BUCKET = "fabric"
BRONZE_PREFIX = "bronze/orders/"

# Iceberg warehouse → s3a://warehouse/
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
    """Ensure MinIO bucket exists"""
    if not minio.bucket_exists(bucket):
        minio.make_bucket(bucket)
        print(f"Created bucket: {bucket}")
    else:
        print(f"ℹBucket exists: {bucket}")


def ensure_warehouse_ready():
    """
    CRITICAL for Airflow:
    Iceberg warehouse = s3a://warehouse/
    → bucket 'warehouse' MUST exist before Spark writes metadata
    """
    ensure_bucket(WAREHOUSE_BUCKET)

    # create a zero-byte marker so MinIO UI shows the folder
    try:
        minio.put_object(
            WAREHOUSE_BUCKET,
            "README.txt",
            data=io.BytesIO(b"Iceberg warehouse root"),
            length=len(b"Iceberg warehouse root"),
            content_type="text/plain"
        )
        print("warehouse bucket initialized")
    except Exception as e:
        print(f"ℹwarehouse marker already exists: {e}")


# ======================================================
# 2) LOAD BRONZE FILES
# ======================================================
def load_bronze_files():
    print("🔥 Loading bronze files from MinIO...")

    if not minio.bucket_exists(BRONZE_BUCKET):
        print(f"Bronze bucket not found: {BRONZE_BUCKET}")
        return []

    rows = []
    objs = minio.list_objects(
        BRONZE_BUCKET,
        prefix=BRONZE_PREFIX,
        recursive=True
    )

    for obj in objs:
        if obj.object_name.endswith("/"):
            continue

        try:
            data = minio.get_object(
                BRONZE_BUCKET,
                obj.object_name
            ).read().decode("utf-8")
            rows.append(json.loads(data))
        except Exception as e:
            print(f"Skip bad bronze file {obj.object_name}: {e}")

    print(f"Loaded {len(rows)} bronze records")
    return rows


# ======================================================
# 3) BRONZE SCHEMA
# ======================================================
bronze_schema = StructType([
    StructField("orderId", StringType()),
    StructField("customerIdHash", StringType()),
    StructField("employee", StringType()),
    StructField("orderDate", StringType()),
    StructField("requiredDate", StringType()),
    StructField("shippedDate", StringType()),
    StructField("shipperID", StringType()),
    StructField("freight", StringType()),
    StructField("status", StringType()),
    StructField("approvedByOrg1", StringType()),

    StructField("details", StructType([
        StructField("orderId", StringType()),
        StructField("items", ArrayType(
            StructType([
                StructField("productId", StringType()),
                StructField("quantity", StringType()),
                StructField("unitPrice", StringType()),
                StructField("discount", StringType()),
                StructField("price", StringType())
            ])
        ))
    ]))
])


# ======================================================
# 4) TRANSFORM BRONZE → SILVER
# ======================================================
def transform_bronze_to_silver(df):
    print("⚙ Transforming Bronze → Silver ...")

    # -------- orders_header --------
    df_header = df.select(
        col("orderId"),
        col("customerIdHash"),
        to_timestamp("orderDate").alias("orderDate"),
        to_timestamp("requiredDate").alias("requiredDate"),
        to_timestamp("shippedDate").alias("shippedDate"),
        col("shipperID").cast("int").alias("shipperID"),
        col("freight").cast("double").alias("freight"),
        col("status"),
        col("approvedByOrg1").cast("boolean").alias("approvedByOrg1")
    )

    # -------- orders_items --------
    df_items = (
        df
        .select("orderId", explode(col("details.items")).alias("item"))
        .select(
            col("orderId"),
            col("item.productId").alias("productId"),
            col("item.quantity").cast("int").alias("quantity"),
            col("item.unitPrice").cast("double").alias("unitPrice"),
            col("item.discount").cast("double").alias("discount"),
            col("item.price").cast("double").alias("price")
        )
    )

    # -------- cleaning --------
    df_items = (
        df_items
        .dropna(subset=["orderId", "productId", "quantity"])
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

    print("Silver transformation done")
    return df_header, df_items


# ======================================================
# 5) SPARK SESSION
# ======================================================
def build_spark():
    return (
        SparkSession.builder
        .appName("bronze_to_silver")

        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        )

        # Nessie catalog
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

        # MinIO S3A
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

        .getOrCreate()
    )


# ======================================================
# 6) MAIN
# ======================================================
def main():
    ensure_warehouse_ready()

    spark = build_spark()

    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.silver")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.gold")

    bronze_rows = load_bronze_files()
    if not bronze_rows:
        print("No bronze data found → stop")
        spark.stop()
        return

    df_bronze = spark.createDataFrame(bronze_rows, bronze_schema)

    df_header, df_items = transform_bronze_to_silver(df_bronze)

    print("Writing Iceberg Silver tables...")
    df_header.writeTo("iceberg.silver.orders_header").createOrReplace()
    df_items.writeTo("iceberg.silver.orders_items").createOrReplace()

    print(" Bronze → Silver SUCCESS")
    spark.stop()


if __name__ == "__main__":
    main()
