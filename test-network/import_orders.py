import subprocess, json, base64, os, time
import pandas as pd


# ==== THÔNG TIN CHUỖI VÀ PEER ====
CHANNEL = "mychannel"
CHAINCODE = "mycc"
ORDERER = "localhost:7050"
ORDERER_TLS = "--ordererTLSHostnameOverride=orderer.example.com"

ORDERER_CA = os.environ["ORDERER_CA"]

PEER1 = "localhost:7051"
ORG1_CA = os.environ["PEER0_ORG1_CA"]

PEER2 = "localhost:9051"
ORG2_CA = os.environ["PEER0_ORG2_CA"]

# ==== HÀM GỌI CHAINCODE ====
def invoke_chaincode(args, peer, ca, transient=None):
    cmd = [
        "peer", "chaincode", "invoke",
        "-o", ORDERER, ORDERER_TLS,
        "--tls", "--cafile", ORDERER_CA,
        "-C", CHANNEL, "-n", CHAINCODE,
        "--peerAddresses", peer,
        "--tlsRootCertFiles", ca,
        "-c", json.dumps({"Args": args})
    ]
    if transient:
        cmd += ["--transient", json.dumps(transient)]
    return subprocess.run(cmd, capture_output=True, text=True)

# ==== TỪNG BƯỚC XỬ LÝ ====
def create_order(order_id, customer_id, meta, details):
    transient = {
        "customerId": base64.b64encode(customer_id.encode()).decode(),
        "order": base64.b64encode(json.dumps(meta).encode()).decode(),
        "orderDetails": base64.b64encode(json.dumps(details).encode()).decode()
    }
    return invoke_chaincode(["CreateOrder", order_id], PEER2, ORG2_CA, transient)

def verify_customer(order_id, customer_id):
    transient = {
        "customerId": base64.b64encode(customer_id.encode()).decode()
    }
    return invoke_chaincode(["VerifyCustomer", order_id], PEER1, ORG1_CA, transient)

def share_order_details(order_id):
    return invoke_chaincode(["ShareOrderDetailsToOrg1", order_id], PEER2, ORG2_CA)

def publish_order(order_id):
    return invoke_chaincode(["PublishOrderDetails", order_id], PEER1, ORG1_CA)

# ==== XỬ LÝ TOÀN BỘ ĐƠN HÀNG ====
def process_order(order_id, customer_id, meta, details):
    print(f"\n🛒 Đơn hàng {order_id} — Khách {customer_id}")
    print("→ Tạo đơn hàng...")
    print(create_order(order_id, customer_id, meta, details).stderr)

    time.sleep(1)
    print("→ Xác thực khách hàng...")
    print(verify_customer(order_id, customer_id).stderr)

    time.sleep(1)
    print("→ Chia sẻ chi tiết đơn hàng...")
    print(share_order_details(order_id).stderr)

    time.sleep(1)
    print("→ Công bố đơn hàng...")
    print(publish_order(order_id).stderr)

# ==== ĐỌC FILE CSV VÀ CHẠY ====
def run_batch_from_csv():
    orders = pd.read_csv("orders.csv")
    details = pd.read_csv("order_details.csv")

    orders.columns = orders.columns.str.strip()
    details.columns = details.columns.str.strip()
    orders["orderID"] = orders["orderID"].astype(str)
    details["orderID"] = details["orderID"].astype(str)

    grouped = details.groupby("orderID")

    for _, row in orders.iterrows():
        order_id = row["orderID"]
        customer_id = row["customerID"]

        meta = {
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

        items = []
        if order_id in grouped.groups:
            for _, d in grouped.get_group(order_id).iterrows():
                items.append({
                    "productId": str(d["productID"]),
                    "quantity": int(d["quantity"]),
                    "unitPrice": float(d["unitPrice"]),
                    "discount": float(d["discount"]),
                    "price": float(d["unitPrice"]) * (1 - float(d["discount"]))
                })

        details_obj = {
            "orderId": order_id,
            "items": items,
            "total": sum(i["quantity"] * i["price"] for i in items)
        }

        process_order(order_id, customer_id, meta, details_obj)
        time.sleep(2)

# ==== CHẠY ====
if __name__ == "__main__":
    run_batch_from_csv()
