from pyspark.sql import SparkSession
from pyspark.sql.types import *
from minio import Minio

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import hdbscan
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
import io
# ======================================================
# 1. Build Spark Session (Iceberg + Nessie + MinIO)
# ======================================================
def build_spark():
    return (
        SparkSession.builder
        .appName("hdbscan_advanced_clustering")

        # Iceberg + Nessie
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .config("spark.sql.catalog.iceberg.uri", "http://nessie:19120/api/v1")
        .config("spark.sql.catalog.iceberg.ref", "main")
        .config("spark.sql.catalog.iceberg.warehouse", "s3a://warehouse/")

        # MinIO
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

        .getOrCreate()
    )


# ======================================================
# 2. MinIO client
# ======================================================
minio = Minio(
    "minio:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

bucket = "fabric"
output_prefix = "gold/analytics/hdbscan_advanced_v3"

# ======================================================
# Helper: save matplotlib plot to MinIO (FIXED)
# ======================================================
def save_plot_to_minio(object_name: str):
    buf = io.BytesIO()
    plt.savefig(buf, format="png", dpi=140, bbox_inches="tight")
    buf.seek(0)

    # ❗ QUAN TRỌNG: positional args only
    minio.put_object(
        bucket,
        f"{output_prefix}/{object_name}",
        buf,
        buf.getbuffer().nbytes,
        content_type="image/png"
    )

    plt.close()

# ======================================================
# MAIN
# ======================================================
def main():
    spark = build_spark()
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.gold")

    print("📥 Loading iceberg.gold.orders ...")
    gold = spark.table("iceberg.gold.orders").toPandas()
    gold.columns = gold.columns.str.lower()

    # ---- Feature engineering ----
    gold["orderdate"] = pd.to_datetime(gold["orderdate"], errors="coerce")
    gold["requireddate"] = pd.to_datetime(gold["requireddate"], errors="coerce")
    gold["shippeddate"] = pd.to_datetime(gold["shippeddate"], errors="coerce")

    gold["order_to_required_days"] = (gold["requireddate"] - gold["orderdate"]).dt.days
    gold["order_to_shipped_days"] = (gold["shippeddate"] - gold["orderdate"]).dt.days
    gold["is_not_shipped"] = gold["shippeddate"].isna().astype(int)
    gold["approvedbyorg1"] = pd.to_numeric(gold["approvedbyorg1"], errors="coerce").fillna(0).astype(int)

    features = [
        "total_amount",
        "avg_price",
        "num_items",
        "freight",
        "order_to_required_days",
        "order_to_shipped_days",
        "is_not_shipped",
        "approvedbyorg1"
    ]

    X = gold[features].apply(pd.to_numeric, errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(0)

    # ---- Scaling + PCA ----
    X_scaled = StandardScaler().fit_transform(X)
    pca = PCA(n_components=3, random_state=42)
    X_pca = pca.fit_transform(X_scaled)

    gold["pca1"], gold["pca2"], gold["pca3"] = X_pca[:, 0], X_pca[:, 1], X_pca[:, 2]
    print(f"📉 PCA explained variance (sum): {pca.explained_variance_ratio_.sum():.4f}")

    # ---- HDBSCAN ----
    print("🚀 Running HDBSCAN ...")
    clusterer = hdbscan.HDBSCAN(
        min_cluster_size=15,
        min_samples=8,
        prediction_data=True
    )

    gold["cluster"] = clusterer.fit_predict(X_pca)
    gold["cluster_probability"] = clusterer.probabilities_
    gold["outlier_score"] = clusterer.outlier_scores_

    print("\n📊 Cluster distribution:")
    print(gold["cluster"].value_counts())

    # ---- Save CSV ----
    csv_bytes = gold.to_csv(index=False).encode("utf-8")
    minio.put_object(
        bucket,
        f"{output_prefix}/hdbscan_results.csv",
        io.BytesIO(csv_bytes),
        len(csv_bytes),
        content_type="text/csv"
    )

    # ---- Plots ----
    plt.figure(figsize=(8, 6))
    plt.scatter(gold["pca1"], gold["pca2"], c=gold["cluster"], cmap="Spectral", s=25)
    plt.title("HDBSCAN on PCA-reduced Order Features")
    save_plot_to_minio("hdbscan_pca_cluster.png")

    plt.figure(figsize=(8, 6))
    plt.scatter(gold["total_amount"], gold["avg_price"], c=gold["cluster"], cmap="Spectral", s=25)
    plt.title("HDBSCAN on Original Feature Space")
    save_plot_to_minio("hdbscan_feature_cluster.png")

    plt.figure(figsize=(8, 6))
    plt.scatter(gold["pca1"], gold["pca2"], c=gold["cluster_probability"], cmap="viridis", s=25)
    plt.title("HDBSCAN Cluster Confidence")
    save_plot_to_minio("hdbscan_pca_probability.png")

    print("\n✅ HDBSCAN Advanced v3 completed")
    print(f"✓ MinIO   → {bucket}/{output_prefix}/")
    print("✓ Iceberg → iceberg.gold.hdbscan_results")

if __name__ == "__main__":
    main()