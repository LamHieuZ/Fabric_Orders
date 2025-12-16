import pandas as pd
import json, base64, subprocess, time, os

BASE = "/home/hieulam/go/src/github.com/hieulamZ/fabric-samples/test-network"
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

def org1_env():
    env = os.environ.copy()
    env.update({
        "CORE_PEER_TLS_ENABLED": "true",
        "CORE_PEER_LOCALMSPID": "Org1MSP",
        "CORE_PEER_TLS_ROOTCERT_FILE": ORG1_CA,
        "CORE_PEER_MSPCONFIGPATH": f"{BASE}/organizations/peerOrganizations/org1.example.com/users/Admin@org1.example.com/msp",
        "CORE_PEER_ADDRESS": PEER1,
        "ORDERER_CA": ORDERER_CA,
        "FABRIC_CFG_PATH": f"{BASE}/../config",
        "PATH": f"{BASE}/../bin:" + env["PATH"],
    })
    return env

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

def publish_with_retry(order_id, max_retries=5, delay=1):
    cmd = [
        PEER_BIN, "chaincode", "invoke",
        "-o", ORDERER, ORDERER_TLS,
        "--tls", "--cafile", ORDERER_CA,
        "-C", CHANNEL, "-n", CHAINCODE,
        "--peerAddresses", PEER1, "--tlsRootCertFiles", ORG1_CA,
        "--peerAddresses", PEER2, "--tlsRootCertFiles", ORG2_CA,
        "-c", json.dumps({"function": "PublishOrderDetails", "Args": [order_id]})
    ]

    for _ in range(max_retries):
        res = run(cmd, org1_env())
        out = res.stdout + res.stderr
        if "status:200" in out:
            print(f"✅ Publish {order_id} thành công")
            return True
        time.sleep(delay)

    print(f"❌ Publish {order_id} thất bại")
    return False

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
                "price": float(d["unitPrice"]) * int(d["quantity"]) - float(d["discount"])
            })

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

    order_details = {
        "orderId": order_id,
        "items": items,
        "total": sum(i["price"] for i in items)
    }

    transient = {
        "customerId": base64.b64encode(customer_id.encode()).decode(),
        "order": base64.b64encode(json.dumps(order_meta).encode()).decode(),
        "orderDetails": base64.b64encode(json.dumps(order_details).encode()).decode()
    }

    run([
        PEER_BIN, "chaincode", "invoke",
        "-o", ORDERER, ORDERER_TLS,
        "--tls", "--cafile", ORDERER_CA,
        "-C", CHANNEL, "-n", CHAINCODE,
        "--peerAddresses", PEER2, "--tlsRootCertFiles", ORG2_CA,
        "--peerAddresses", PEER1, "--tlsRootCertFiles", ORG1_CA,
        "-c", json.dumps({"function": "CreateOrder", "Args": [order_id]}),
        "--transient", json.dumps(transient)
    ], org2_env())

    time.sleep(WAIT_SECONDS)

    run([
        PEER_BIN, "chaincode", "invoke",
        "-o", ORDERER, ORDERER_TLS,
        "--tls", "--cafile", ORDERER_CA,
        "-C", CHANNEL, "-n", CHAINCODE,
        "--peerAddresses", PEER1, "--tlsRootCertFiles", ORG1_CA,
        "--peerAddresses", PEER2, "--tlsRootCertFiles", ORG2_CA,
        "-c", json.dumps({"function": "VerifyCustomer", "Args": [order_id]}),
        "--transient", json.dumps({
            "customerId": base64.b64encode(customer_id.encode()).decode()
        })
    ], org1_env())

    time.sleep(WAIT_SECONDS)

    run([
        PEER_BIN, "chaincode", "invoke",
        "-o", ORDERER, ORDERER_TLS,
        "--tls", "--cafile", ORDERER_CA,
        "-C", CHANNEL, "-n", CHAINCODE,
        "--peerAddresses", PEER2, "--tlsRootCertFiles", ORG2_CA,
        "-c", json.dumps({"function": "ShareOrderDetailsToOrg1", "Args": [order_id]})
    ], org2_env())

    time.sleep(WAIT_SECONDS)

    publish_with_retry(order_id, delay=WAIT_SECONDS)

    print(f"🎉 Order {order_id} hoàn tất\n")
