from sklearn.cluster import DBSCAN
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
import matplotlib.pyplot as plt
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from minio import Minio
import io

# ======================================================
# 1. Spark Session (Iceberg + Nessie + MinIO)
# ======================================================
spark = (
    SparkSession.builder
    .appName("dbscan_advanced_clustering")

    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.iceberg.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
    .config("spark.sql.catalog.iceberg.uri", "http://nessie:19120/api/v1")
    .config("spark.sql.catalog.iceberg.ref", "main")
    .config("spark.sql.catalog.iceberg.warehouse", "s3a://warehouse/")

    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

# ======================================================
# 2. MinIO Client
# ======================================================
minio = Minio(
    "minio:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False,
)

bucket = "fabric"
output_prefix = "gold/analytics/dbscan_pca"

# ======================================================
# 3. MAIN PIPELINE
# ======================================================
def main():

    print("Loading iceberg.gold.orders ...")
    gold = spark.table("iceberg.gold.orders").toPandas()

    # --------------------------------------------------
    # Normalize column names (Iceberg → lowercase)
    # --------------------------------------------------
    gold.columns = gold.columns.str.lower()

    # --------------------------------------------------
    # Feature Engineering (Time-based)
    # --------------------------------------------------
    gold["orderdate"] = pd.to_datetime(gold["orderdate"])
    gold["requireddate"] = pd.to_datetime(gold["requireddate"])
    gold["shippeddate"] = pd.to_datetime(gold["shippeddate"], errors="coerce")

    gold["order_to_required_days"] = (
        gold["requireddate"] - gold["orderdate"]
    ).dt.days

    gold["order_to_shipped_days"] = (
        gold["shippeddate"] - gold["orderdate"]
    ).dt.days

    gold["is_not_shipped"] = gold["shippeddate"].isna().astype(int)
    gold["approvedbyorg1"] = gold["approvedbyorg1"].astype(int)

    # --------------------------------------------------
    # MULTI-DIMENSIONAL FEATURES
    # --------------------------------------------------
    feature_cols = [
        "total_amount",
        "avg_price",
        "num_items",
        "freight",
        "order_to_required_days",
        "order_to_shipped_days",
        "is_not_shipped",
        "approvedbyorg1"
    ]

    X = gold[feature_cols].fillna(0)

    # --------------------------------------------------
    # 1️⃣ Standard Scaling
    # --------------------------------------------------
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # --------------------------------------------------
    # 2️⃣ PCA (Giảm chiều)
    # --------------------------------------------------
    pca = PCA(n_components=3, random_state=42)
    X_pca = pca.fit_transform(X_scaled)

    print("PCA explained variance ratio:")
    print(pca.explained_variance_ratio_)
    print("Total explained variance:", pca.explained_variance_ratio_.sum())

    gold["pca1"] = X_pca[:, 0]
    gold["pca2"] = X_pca[:, 1]
    gold["pca3"] = X_pca[:, 2]

    # --------------------------------------------------
    # DBSCAN on PCA space
    # --------------------------------------------------
    dbscan = DBSCAN(
        eps=0.9,        # PCA space → eps ổn định hơn
        min_samples=10
    )

    gold["cluster"] = dbscan.fit_predict(X_pca)

    print("\n Cluster distribution:")
    print(gold["cluster"].value_counts())

    # --------------------------------------------------
    # Save CSV to MinIO
    # --------------------------------------------------
    csv_bytes = gold.to_csv(index=False).encode("utf-8")
    minio.put_object(
        bucket,
        f"{output_prefix}/dbscan_pca_results.csv",
        io.BytesIO(csv_bytes),
        len(csv_bytes),
        content_type="text/csv"
    )

    # --------------------------------------------------
    # Visualization (PCA1 vs PCA2)
    # --------------------------------------------------
    plt.figure(figsize=(8, 6))
    plt.scatter(
        gold["pca1"],
        gold["pca2"],
        c=gold["cluster"],
        cmap="viridis",
        s=20
    )
    plt.xlabel("PCA 1")
    plt.ylabel("PCA 2")
    plt.title("DBSCAN on PCA-reduced Feature Space")

    img_bytes = io.BytesIO()
    plt.savefig(img_bytes, format="png")
    img_bytes.seek(0)

    minio.put_object(
        bucket,
        f"{output_prefix}/dbscan_pca_plot.png",
        img_bytes,
        img_bytes.getbuffer().nbytes,
        content_type="image/png"
    )

    # --------------------------------------------------
    # Save clustering result to Iceberg
    # --------------------------------------------------
    final_cols = [
        "orderid",
        "total_amount",
        "avg_price",
        "num_items",
        "freight",
        "order_to_required_days",
        "order_to_shipped_days",
        "is_not_shipped",
        "approvedbyorg1",
        "pca1",
        "pca2",
        "pca3",
        "cluster"
    ]

    gold_final = gold[final_cols].rename(
        columns={
            "orderid": "orderId",
            "approvedbyorg1": "approvedByOrg1"
        }
    )

    spark_schema = StructType([
        StructField("orderId", StringType()),
        StructField("total_amount", DoubleType()),
        StructField("avg_price", DoubleType()),
        StructField("num_items", IntegerType()),
        StructField("freight", DoubleType()),
        StructField("order_to_required_days", IntegerType()),
        StructField("order_to_shipped_days", IntegerType()),
        StructField("is_not_shipped", IntegerType()),
        StructField("approvedByOrg1", IntegerType()),
        StructField("pca1", DoubleType()),
        StructField("pca2", DoubleType()),
        StructField("pca3", DoubleType()),
        StructField("cluster", IntegerType())
    ])

    int_cols = [
    "num_items",
    "order_to_required_days",
    "order_to_shipped_days",
    "is_not_shipped",
    "approvedByOrg1",
    "cluster"
    ]

    for c in int_cols:
        gold_final[c] = (
            gold_final[c]
            .fillna(0)              # xử lý NaN
            .replace([float("inf"), float("-inf")], 0)
            .astype("int64")
        )



    spark_df = spark.createDataFrame(gold_final, schema=spark_schema)

    spark_df.writeTo(
        "iceberg.gold.clustering_results"
    ).createOrReplace()

    print("\n DBSCAN + PCA clustering completed")
    print("MinIO  → fabric/gold/analytics/dbscan_pca/")
    print("Iceberg → iceberg.gold.clustering_results")


# ======================================================
# Run
# ======================================================
if __name__ == "__main__":
    main()