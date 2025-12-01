import subprocess
import time
import os
import pandas as pd
import json
import base64

# ==== THÔNG TIN CHUỖI VÀ PEER ====
CHANNEL = "mychannel"
CHAINCODE = "mycc"
ORDERER = "localhost:7050"
ORDERER_TLS = "--ordererTLSHostnameOverride=orderer.example.com"
PEER1 = "localhost:7051"
PEER2 = "localhost:9051"
ORDERER_CA = os.environ["ORDERER_CA"]
ORG1_CA = os.environ["PEER0_ORG1_CA"]
ORG2_CA = os.environ["PEER0_ORG2_CA"]
ORG3_CA = os.environ["PEER0_ORG3_CA"]

# ==== ĐỌC FILE CSV ====
orders_df = pd.read_csv("orders.csv")
details_df = pd.read_csv("order_details.csv")

# Chuẩn hóa tên cột và kiểu dữ liệu
orders_df.columns = orders_df.columns.str.strip()
details_df.columns = details_df.columns.str.strip()
orders_df["orderID"] = orders_df["orderID"].astype(str)
details_df["orderID"] = details_df["orderID"].astype(str)

# Gom chi tiết sản phẩm theo orderID
grouped_details = details_df.groupby("orderID")

# ==== GỬI TỪNG ĐƠN HÀNG ====
for _, row in orders_df.iterrows():
    order_id = str(row["orderID"]).strip()
    customer_id = str(row["customerID"]).strip()

    # Tạo metadata đơn hàng
    order_meta = {
        "orderId": order_id,
        "employee": int(row["employeeID"]),
        "orderDate": str(row["orderDate"]),
        "requiredDate": str(row["requiredDate"]),
        "shippedDate": str(row["shippedDate"]),
        "shipperID": int(row["shipperID"]),
        "freight": float(row["freight"]),
        "status": "CREATED",
        "approvedByOrg1": False
    }

    # Tạo danh sách sản phẩm
    items = []
    if order_id in grouped_details.groups:
        for _, d in grouped_details.get_group(order_id).iterrows():
            items.append({
                "productId": str(d["productID"]),
                "quantity": int(d["quantity"]),
                "unitPrice": float(d["unitPrice"]),
                "discount": float(d["discount"]),
                "price": float(d["unitPrice"]) * (1 - float(d["discount"]))
            })

    order_details = {
        "orderId": order_id,
        "items": items,
        "total": sum(i["quantity"] * i["price"] for i in items)
    }

    # 🔑 Tạo transient map với base64 encode
    transient_map = {
        "customerId": base64.b64encode(customer_id.encode()).decode(),
        "order": base64.b64encode(json.dumps(order_meta).encode()).decode(),
        "orderDetails": base64.b64encode(json.dumps(order_details).encode()).decode()
    }

    # Gọi peer chaincode invoke
    cmd = [
        "peer", "chaincode", "invoke",
        "-o", ORDERER,
        ORDERER_TLS,
        "--tls", "--cafile", ORDERER_CA,
        "-C", CHANNEL, "-n", CHAINCODE,
        "--peerAddresses", PEER2,
        "--tlsRootCertFiles", ORG2_CA,
        "-c", json.dumps({"function": "CreateOrder", "Args": [order_id]}),
        "--transient", json.dumps(transient_map)
    ]

    print(f"Uploading order {order_id} with {len(items)} items...")
    result = subprocess.run(cmd, capture_output=True, text=True)
    print(result.stdout)
    print(result.stderr)

    time.sleep(0.5)
