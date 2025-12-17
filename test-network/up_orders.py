import pandas as pd
import json, base64, subprocess, time, os

BASE = os.getcwd() 
PEER_BIN = f"{BASE}/../bin/peer"

ORDERER = "localhost:7050"
ORDERER_TLS = "--ordererTLSHostnameOverride=orderer.example.com"
CHANNEL = "mychannel"
CHAINCODE = "mycc"

PEER1 = "localhost:7051"
PEER2 = "localhost:9051"

ORG1_CA = f"{BASE}/organizations/peerOrganizations/org1.example.com/peers/peer0.org1.example.com/tls/ca.crt"
ORG2_CA = f"{BASE}/organizations/peerOrganizations/org2.example.com/peers/peer0.org2.example.com/tls/ca.crt"
ORDERER_CA = f"{BASE}/organizations/ordererOrganizations/example.com/orderers/orderer.example.com/msp/tlscacerts/tlsca.example.com-cert.pem"

WAIT_SECONDS = 1

def org2_env():
    env = os.environ.copy()
    env.update({
        "CORE_PEER_TLS_ENABLED": "true",
        "CORE_PEER_LOCALMSPID": "Org2MSP",
        "CORE_PEER_TLS_ROOTCERT_FILE": ORG2_CA,
        "CORE_PEER_MSPCONFIGPATH": f"{BASE}/organizations/peerOrganizations/org2.example.com/users/Admin@org2.example.com/msp",
        "CORE_PEER_ADDRESS": PEER2,
        "ORDERER_CA": ORDERER_CA,
        "FABRIC_CFG_PATH": f"{BASE}/../config",
        "PATH": f"{BASE}/../bin:" + env["PATH"],
    })
    return env

def run(cmd, env):
    return subprocess.run(cmd, env=env, capture_output=True, text=True)

def create_and_publish(order_id, customer_id, order_meta, order_details):
    transient = {
        "customerId": base64.b64encode(customer_id.encode()).decode(),
        "order": base64.b64encode(json.dumps(order_meta).encode()).decode(),
        "orderDetails": base64.b64encode(json.dumps(order_details).encode()).decode()
    }

    cmd = [
        PEER_BIN, "chaincode", "invoke",
        "-o", ORDERER, ORDERER_TLS,
        "--tls", "--cafile", ORDERER_CA,
        "-C", CHANNEL, "-n", CHAINCODE,
        "--peerAddresses", PEER2, "--tlsRootCertFiles", ORG2_CA,
        "--peerAddresses", PEER1, "--tlsRootCertFiles", ORG1_CA,
        "-c", json.dumps({"function": "CreateOrderAndApprove", "Args": [order_id]}),
        "--transient", json.dumps(transient)
    ]

    res = run(cmd, org2_env())
    out = res.stdout + res.stderr
    if "status:200" in out:
        print(f" Order {order_id} đã tạo và công bố thành công")
    else:
        print(f" Order {order_id} thất bại\n{out}")

# ================== MAIN ==================
orders_df = pd.read_csv("orders.csv")
details_df = pd.read_csv("order_details.csv")

orders_df["orderID"] = orders_df["orderID"].astype(str)
details_df["orderID"] = details_df["orderID"].astype(str)

grouped_details = details_df.groupby("orderID")

for _, row in orders_df.iterrows():
    order_id = row["orderID"].strip()
    customer_id = str(row["customerID"]).strip()

    items = []
    if order_id in grouped_details.groups:
        for _, d in grouped_details.get_group(order_id).iterrows():
            items.append({
                "productId": str(d["productID"]),
                "quantity": int(d["quantity"]),
                "unitPrice": float(d["unitPrice"]),
                "discount": float(d["discount"]),
                "price": float(d["unitPrice"]) * int(d["quantity"]) * (1 - float(d["discount"]))
            })

    order_meta = {
        "orderId": order_id,
        "employee": int(row["employeeID"]),
        "orderDate": str(row["orderDate"]),
        "requiredDate": str(row["requiredDate"]),
        "shippedDate": str(row["shippedDate"]),
        "shipperID": int(row["shipperID"]),
        "freight": float(row["freight"]),
        "status": "APPROVED",        # mặc định đã duyệt
        "approvedByOrg1": True       # mặc định Org1 đã approve
    }

    order_details = {
        "orderId": order_id,
        "items": items,
        "total": sum(i["price"] for i in items)
    }

    create_and_publish(order_id, customer_id, order_meta, order_details)
    time.sleep(WAIT_SECONDS)