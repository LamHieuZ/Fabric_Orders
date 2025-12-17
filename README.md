# Blockchain Data Pipeline with Hyperledger Fabric, Trino, and Spark

## 📌 Giới thiệu
Dự án này triển khai một mạng Hyperledger Fabric để ghi nhận dữ liệu đơn hàng, sau đó sử dụng Trino để truy vấn dữ liệu và Spark để xử lý các pipeline (bronze → silver → gold). Hệ thống được container hóa bằng Docker Compose.

---
Cấu trúc dự án

test-network/          # Hyperledger Fabric network scripts
pipelines/             # Spark ETL & ML pipelines
docker-compose.yml     # Docker Compose config
README.md              # Tài liệu hướng dẫn


## 🚀 Khởi chạy mạng Fabric

```bash
cd test-network

# Tắt mạng cũ nếu có
./network.sh down

# Khởi chạy lại mạng Fabric
./run_network.sh

# Đưa dữ liệu đơn hàng mẫu lên blockchain
python3 up_orders.py

Quản lý Docker Compose

# Dừng toàn bộ container và xóa volumes
docker compose down -v

# Build lại và khởi chạy toàn bộ dịch vụ
docker compose up -d --build

# Chạy riêng listener để ghi nhận block mới
docker compose up -d --build listener

# Theo dõi log của listener
docker logs -f fabric-listener

Kiểm tra Trino

# Xem log gần nhất của Trino
docker logs trino --tail=200

# Kiểm tra trạng thái server
docker logs trino | grep "SERVER STARTED"

# Truy cập Trino CLI
docker exec -it trino trino

# Các lệnh SQL mẫu
SHOW CATALOGS;
SHOW SCHEMAS FROM iceberg;

Chạy Spark Pipelines
# Bronze → Silver
docker compose exec spark-master /opt/spark/bin/spark-submit /app/pipelines/bronze_to_silver.py

# Silver → Gold
docker compose exec spark-master /opt/spark/bin/spark-submit /app/pipelines/silver_to_gold.py

# Test DBSCAN clustering
docker compose exec spark-master /opt/spark/bin/spark-submit /app/pipelines/testdbscan.py

# Test HDBSCAN clustering
docker compose exec spark-master /opt/spark/bin/spark-submit /app/pipelines/testhdbscan.py
