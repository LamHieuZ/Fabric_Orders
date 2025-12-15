from flask import Flask, request, render_template
import subprocess
import base64
import json
import os
import re

# ================== FABRIC CONFIG ==================

BASE = "/home/hieulam/go/src/github.com/hieulamZ/fabric-samples/test-network"

PEER_BIN = f"{BASE}/../bin/peer"
FABRIC_CFG_PATH = f"{BASE}/../config"

ORDERER_CA = (
    f"{BASE}/organizations/ordererOrganizations/example.com/"
    "orderers/orderer.example.com/msp/tlscacerts/tlsca.example.com-cert.pem"
)

PEER0_ORG2_CA = (
    f"{BASE}/organizations/peerOrganizations/org2.example.com/"
    "peers/peer0.org2.example.com/tls/ca.crt"
)

PEER0_ORG1_CA = (
    f"{BASE}/organizations/peerOrganizations/org1.example.com/"
    "peers/peer0.org1.example.com/tls/ca.crt"
)

CORE_PEER_ADDRESS_ORG2 = "localhost:9051"
CORE_PEER_ADDRESS_ORG1 = "localhost:7051"

ORG2_ADMIN_MSP = (
    f"{BASE}/organizations/peerOrganizations/org2.example.com/"
    "users/Admin@org2.example.com/msp"
)
CONFIGTXLATOR_BIN = f"{BASE}/../bin/configtxlator"

ORDERER_ADDR = "localhost:7050"
CHANNEL_NAME = "mychannel"
CHAINCODE_NAME = "mycc"

# ================== FLASK ==================

app = Flask(__name__)

@app.route("/")
def index():
    return render_template("index.html")

# ================== HELPER ==================

def org2_env():
    
    env = os.environ.copy()
    env.update({
        "CORE_PEER_TLS_ENABLED": "true",
        "CORE_PEER_LOCALMSPID": "Org2MSP",
        "CORE_PEER_TLS_ROOTCERT_FILE": PEER0_ORG2_CA,
        "CORE_PEER_MSPCONFIGPATH": ORG2_ADMIN_MSP,
        "CORE_PEER_ADDRESS": CORE_PEER_ADDRESS_ORG2,
        "ORDERER_CA": ORDERER_CA,
        "FABRIC_CFG_PATH": FABRIC_CFG_PATH,
        "PATH": f"{BASE}/../bin:" + env["PATH"],
    })
    return env
def org1_env():
    
    env = os.environ.copy()
    env.update({
        "CORE_PEER_TLS_ENABLED": "true",
        "CORE_PEER_LOCALMSPID": "Org1MSP",
        "CORE_PEER_TLS_ROOTCERT_FILE": PEER0_ORG1_CA,
        "CORE_PEER_MSPCONFIGPATH": (
            f"{BASE}/organizations/peerOrganizations/"
            "org1.example.com/users/Admin@org1.example.com/msp"
        ),
        "CORE_PEER_ADDRESS": CORE_PEER_ADDRESS_ORG1,
        "ORDERER_CA": ORDERER_CA,
        "FABRIC_CFG_PATH": FABRIC_CFG_PATH,
        "PATH": f"{BASE}/../bin:" + env["PATH"],
    })
    return env
def get_block_height(env):
    cmd = [
        PEER_BIN, "channel", "getinfo",
        "-C", CHANNEL_NAME
    ]
    result = subprocess.run(
        cmd, env=env, capture_output=True, text=True
    )

    info = json.loads(result.stdout)
    return info["height"]

def pretty_json(text):
    try:
        return json.dumps(json.loads(text), indent=2, ensure_ascii=False)
    except Exception:
        return text
# ================== CREATE ORDER ==================

@app.route("/create_order", methods=["POST"])
def create_order():
    env = org2_env()

    # --------- FORM DATA ---------
    order_id = request.form["orderId"]
    customer_id = request.form["customerId"]

    order_meta = {
        "employee": int(request.form["employee"]),
        "orderDate": request.form["orderDate"],
        "requiredDate": request.form["requiredDate"],
        "shipperID": int(request.form["shipperID"]),
        "freight": float(request.form["freight"]),
    }

    product_ids = request.form.getlist("productId")
    quantities = request.form.getlist("quantity")
    unit_prices = request.form.getlist("unitPrice")
    discounts = request.form.getlist("discount")

    items, total = [], 0.0
    for i in range(len(product_ids)):
        qty = int(quantities[i])
        price = float(unit_prices[i])
        disc = float(discounts[i]) if discounts[i] else 0.0
        item_total = price * qty - disc
        total += item_total

        items.append({
            "productId": product_ids[i],
            "quantity": qty,
            "unitPrice": price,
            "discount": disc,
            "price": item_total
        })

    order_details = {
        "items": items,
        "total": total
    }

    # --------- TRANSIENT ---------
    transient = {
        "customerId": base64.b64encode(customer_id.encode()).decode(),
        "order": base64.b64encode(json.dumps(order_meta).encode()).decode(),
        "orderDetails": base64.b64encode(json.dumps(order_details).encode()).decode()
    }

    # --------- FABRIC INVOKE ---------
    cmd = [
        PEER_BIN, "chaincode", "invoke",
        "-o", "localhost:7050",
        "--ordererTLSHostnameOverride", "orderer.example.com",
        "--tls", "--cafile", ORDERER_CA,
        "-C", CHANNEL_NAME,
        "-n", CHAINCODE_NAME,

        "--peerAddresses", CORE_PEER_ADDRESS_ORG2,
        "--tlsRootCertFiles", PEER0_ORG2_CA,

        "--peerAddresses", CORE_PEER_ADDRESS_ORG1,
        "--tlsRootCertFiles", PEER0_ORG1_CA,

        "-c", json.dumps({
            "function": "CreateOrder",
            "Args": [order_id]
        }),

        "--transient", json.dumps(transient)
    ]

    result = subprocess.run(cmd,env=env, capture_output=True, text=True)

    success = (
        result.returncode == 0 and
        "status:200" in (result.stdout + result.stderr)
    )

    return render_template(
        "result.html",
        action="Create Order",
        success=success,
        stdout=result.stdout,
        stderr=result.stderr
    )


# Org1 - User B: Verify Customer
# ================== ORG1 - VERIFY CUSTOMER ==================

@app.route("/verify_customer", methods=["POST"])
def verify_customer():
    env = org1_env()

    order_id = request.form["orderId"]
    customer_id = request.form["customerId"]

    cust_b64 = base64.b64encode(customer_id.encode()).decode()

    cmd = [
        PEER_BIN, "chaincode", "invoke",
        "-o", "localhost:7050",
        "--ordererTLSHostnameOverride", "orderer.example.com",
        "--tls", "--cafile", ORDERER_CA,

        "-C", CHANNEL_NAME,
        "-n", CHAINCODE_NAME,

        "--peerAddresses", CORE_PEER_ADDRESS_ORG1,
        "--tlsRootCertFiles", PEER0_ORG1_CA,

        "--peerAddresses", CORE_PEER_ADDRESS_ORG2,
        "--tlsRootCertFiles", PEER0_ORG2_CA,

        "-c", json.dumps({
            "function": "VerifyCustomer",
            "Args": [order_id]
        }),

        "--transient", json.dumps({
            "customerId": cust_b64
        })
    ]

    result = subprocess.run(
        cmd,
        env=env,
        capture_output=True,
        text=True
    )

    success = (
        result.returncode == 0 and
        "status:200" in (result.stdout + result.stderr)
    )

    return render_template(
        "result.html",
        action="Verify Customer (Org1)",
        success=success,
        stdout=result.stdout,
        stderr=result.stderr
    )

@app.route("/share_details", methods=["POST"])
def share_details():
    env = org2_env()
    order_id = request.form["orderId"]

    cmd = [
        PEER_BIN, "chaincode", "invoke",
        "-o", "localhost:7050",
        "--ordererTLSHostnameOverride", "orderer.example.com",
        "--tls", "--cafile", ORDERER_CA,
        "-C", CHANNEL_NAME,
        "-n", CHAINCODE_NAME,

        "--peerAddresses", CORE_PEER_ADDRESS_ORG2,
        "--tlsRootCertFiles", PEER0_ORG2_CA,

        "-c", json.dumps({
            "function": "ShareOrderDetailsToOrg1",
            "Args": [order_id]
        })
    ]

    result = subprocess.run(cmd, env=env, capture_output=True, text=True)

    success = result.returncode == 0 and "status:200" in (result.stdout + result.stderr)

    return render_template(
        "result.html",
        action="Share Order Details (Org2)",
        success=success,
        stdout=result.stdout,
        stderr=result.stderr
    )

@app.route("/promote_details", methods=["POST"])
def promote_details():
    env = org2_env()
    order_id = request.form["orderId"]

    cmd = [
        PEER_BIN, "chaincode", "invoke",
        "-o", "localhost:7050",
        "--ordererTLSHostnameOverride", "orderer.example.com",
        "--tls", "--cafile", ORDERER_CA,
        "-C", CHANNEL_NAME,
        "-n", CHAINCODE_NAME,

        "--peerAddresses", CORE_PEER_ADDRESS_ORG2,
        "--tlsRootCertFiles", PEER0_ORG2_CA,

        "--peerAddresses", CORE_PEER_ADDRESS_ORG1,
        "--tlsRootCertFiles", PEER0_ORG1_CA,

        "-c", json.dumps({
            "function": "PublishOrderDetails",
            "Args": [order_id]
        })
    ]

    result = subprocess.run(cmd, env=env, capture_output=True, text=True)

    success = result.returncode == 0 and "status:200" in (result.stdout + result.stderr)

    return render_template(
        "result.html",
        action="Promote Details to World State",
        success=success,
        stdout=result.stdout,
        stderr=result.stderr
    )

# ================== READ ORDER DETAILS ==================
@app.route('/readorderdetails', methods=['POST'])
def read_order_details():
    order_id = request.form['orderId']

    env = org1_env()

    cmd = [
        PEER_BIN, "chaincode", "query",
        "-C", CHANNEL_NAME,
        "-n", CHAINCODE_NAME,
        "-c", json.dumps({
            "Args": ["ReadPublicOrder", order_id]
        })
    ]

    result = subprocess.run(cmd, env=env, capture_output=True, text=True)

    data = None
    if result.stdout:
        try:
            data = json.loads(result.stdout)
        except Exception as e:
            print("JSON parse error:", e)

    return render_template(
        "read_orderdetails.html",
        action="Read Order Details",
        data=data,
        stdout=result.stdout,
        stderr=result.stderr
    )


# Org1/Org2: Query Order
# ================== READ ONE ORDER ==================

@app.route("/read_order", methods=["POST"])
def read_order():
    env = org1_env()   # hoặc org2_env()

    order_id = request.form["orderId"]

    cmd = [
        PEER_BIN, "chaincode", "query",
        "-C", CHANNEL_NAME,
        "-n", CHAINCODE_NAME,
        "-c", json.dumps({
            "Args": ["ReadOrder", order_id]
        })
    ]

    result = subprocess.run(
        cmd,
        env=env,
        capture_output=True,
        text=True
    )

    try:
        raw_json = result.stdout.strip()
        data = json.loads(raw_json) if raw_json else None
    except Exception as e:
        print(f"Error parsing JSON: {e}")
        data = None

    return render_template(
        "read_order.html",
        action=f"Read Order {order_id}",
        data=data,
        stderr=result.stderr
    )


# ================== READ ALL ORDERS ==================

@app.route("/read_all_orders", methods=["POST"])
def read_all_orders():
    env = org1_env()   # hoặc org2_env()

    cmd = [
        PEER_BIN, "chaincode", "query",
        "-C", CHANNEL_NAME,
        "-n", CHAINCODE_NAME,
        "-c", json.dumps({
            "Args": ["ReadAllOrders"]
        })
    ]

    result = subprocess.run(
        cmd,
        env=env,
        capture_output=True,
        text=True
    )

    stdout = pretty_json(result.stdout)
    stderr = result.stderr

    return render_template(
        "ReadAllOrders.html",
        action="Read All Orders",
        stdout=stdout,
        stderr=stderr
    )


# ================== READ ORDERS BY CUSTOMER ==================

import base64
import json
import subprocess
from flask import request, render_template

@app.route("/read_orders_by_customer", methods=["POST"])
def read_orders_by_customer():
    env = org1_env()   # hoặc org2_env()

    customer_id = request.form["customerId"]

    # Args phải có cả tên hàm và tham số customerId
    cmd = [
        PEER_BIN, "chaincode", "query",
        "-C", CHANNEL_NAME,
        "-n", CHAINCODE_NAME,
        "-c", json.dumps({
            "Args": ["ReadOrdersByCustomerID", customer_id]
        })
    ]

    result = subprocess.run(
        cmd,
        env=env,
        capture_output=True,
        text=True
    )

    stdout = pretty_json(result.stdout)
    stderr = result.stderr

    return render_template(
        "ReadOrdersbyCustomer.html",
        action=f"Read Orders by Customer {customer_id}",
        stdout=stdout,
        stderr=stderr
    )

# ================== SHOW BLOCKS ==================
@app.route("/show_blocks", methods=["GET"])
def show_blocks():
    env = org1_env()

    # Lấy blockchain info
    cmd_info = [PEER_BIN, "channel", "getinfo", "-c", CHANNEL_NAME]
    res_info = subprocess.run(cmd_info, env=env, capture_output=True, text=True)
    raw = res_info.stdout.strip()
    if raw.startswith("Blockchain info:"):
        raw = raw[len("Blockchain info:"):].strip()
    info = json.loads(raw)
    height = int(info["height"])

    blocks_view = []

    for i in range(height):
        block_file = f"/tmp/block_{i}.block"
        cmd_fetch = [
            PEER_BIN, "channel", "fetch", str(i),
            block_file,
            "-c", CHANNEL_NAME,
            "-o", ORDERER_ADDR,
            "--tls",
            "--cafile", ORDERER_CA
        ]
        subprocess.run(cmd_fetch, env=env, capture_output=True)

        cmd_decode = [
            CONFIGTXLATOR_BIN, "proto_decode",
            "--input", block_file,
            "--type", "common.Block"
        ]
        res_decode = subprocess.run(cmd_decode, capture_output=True, text=True)
        if not res_decode.stdout.strip():
            continue

        block_json = json.loads(res_decode.stdout)
        tx_count = len(block_json.get("data", {}).get("data", []))

        block_header = block_json.get("header", {})
        block_number = block_header.get("number", i)
        previous_hash = block_header.get("previous_hash", "")
        data_hash = block_header.get("data_hash", "")
        channel_id = CHANNEL_NAME
        tx_list = []

        if tx_count > 0:
            try:
                channel_id = block_json["data"]["data"][0]["payload"]["header"]["channel_header"]["channel_id"]
            except Exception:
                pass

            for tx in block_json["data"]["data"]:
                header = tx["payload"]["header"]["channel_header"]
                creator_raw = tx["payload"]["header"]["signature_header"]["creator"]

                # Decode creator
                creator_msp = creator_raw.get("Mspid", "")
                creator_cert = creator_raw.get("IdBytes", "")
                creator_name = ""
                try:
                    decoded_cert = base64.b64decode(creator_cert).decode("utf-8", errors="ignore")
                    match = re.search(r"CN=([^,]+)", decoded_cert)
                    if match:
                        creator_name = match.group(1)
                except Exception:
                    pass
                creator_display = f"{creator_name} ({creator_msp})" if creator_name else creator_msp

                actions = tx["payload"]["data"].get("actions", [])
                chaincode = function = args = ""
                rwset = []
                if actions:
                    try:
                        chaincode_spec = actions[0]["payload"]["chaincode_proposal_payload"]["input"]["chaincode_spec"]
                        chaincode = chaincode_spec["chaincode_id"]["name"]
                        input_args = chaincode_spec["input"]["args"]

                        if input_args:
                            try:
                                function = base64.b64decode(input_args[0]).decode("utf-8")
                            except Exception:
                                function = input_args[0]

                            args_list = []
                            for a in input_args[1:]:
                                try:
                                    args_list.append(base64.b64decode(a).decode("utf-8"))
                                except Exception:
                                    args_list.append(a)
                            args = ", ".join(args_list)

                        results = actions[0]["payload"]["action"]["proposal_response_payload"]["extension"]["results"]
                        for ns in results.get("ns_rwset", []):
                            namespace = ns.get("namespace", "")
                            for w in ns.get("rwset", {}).get("writes", []):
                                val = ""
                                try:
                                    val = base64.b64decode(w["value"]).decode("utf-8", errors="ignore")
                                except Exception:
                                    val = w["value"]
                                rwset.append({
                                    "namespace": namespace,
                                    "key": w.get("key", ""),
                                    "value": val
                                })
                    except Exception:
                        pass

                tx_list.append({
                    "tx_id": header.get("tx_id", ""),
                    "timestamp": header.get("timestamp", ""),
                    "chaincode": chaincode,
                    "function": function,
                    "args": args,
                    "creator": creator_display,
                    "rwset": rwset
                })

        blocks_view.append({
            "block_number": block_number,
            "tx_count": tx_count,
            "channel_id": channel_id,
            "transactions": tx_list,
            "previous_hash": previous_hash,
            "data_hash": data_hash
        })

    return render_template("block.html", blocks=blocks_view)


 