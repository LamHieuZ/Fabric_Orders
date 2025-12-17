import json, subprocess, io, base64, time
from minio import Minio

# ================== CONFIG ==================
CHANNEL = "mychannel"
CHAINCODE = "mycc"

PEER_BIN = "/usr/local/bin/peer"
CONFIGTXLATOR = "/usr/local/bin/configtxlator"

ORDERER = "orderer.example.com:7050"
ORDERER_CA = "/crypto/ordererOrganizations/example.com/orderers/orderer.example.com/tls/ca.crt"

TMP_DIR = "/tmp"

# ================== MINIO ==================
minio = Minio(
    "minio:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

BUCKET = "fabric"
if not minio.bucket_exists(BUCKET):
    minio.make_bucket(BUCKET)

# ================== BLOCK UTILS ==================
def fetch_block(target):
    pb = f"{TMP_DIR}/block_{target}.pb"
    js = f"{TMP_DIR}/block_{target}.json"

    subprocess.run([
        PEER_BIN, "channel", "fetch", str(target), pb,
        "-c", CHANNEL,
        "--orderer", ORDERER,
        "--tls", "--cafile", ORDERER_CA
    ], check=True, capture_output=True)

    subprocess.run([
        CONFIGTXLATOR, "proto_decode",
        "--input", pb,
        "--type", "common.Block",
        "--output", js
    ], check=True, capture_output=True)

    with open(js) as f:
        return json.load(f)

def get_block_number(block_json):
    return int(block_json["header"]["number"])

# ================== PARSE LEDGER ==================
def extract_orders(block_json):
    orders = []

    for env in block_json["data"]["data"]:
        payload = env["payload"]["data"]

        # Skip config block
        if "actions" not in payload:
            continue

        for action in payload["actions"]:
            rwsets = action["payload"]["action"] \
                ["proposal_response_payload"]["extension"]["results"]["ns_rwset"]

            for ns in rwsets:
                if ns.get("namespace") != CHAINCODE:
                    continue

                rwset = ns.get("rwset")
                if not rwset:
                    continue   

                writes = rwset.get("writes", [])
                for w in writes:
                    if not w.get("value"):
                        continue

                    try:
                        raw = base64.b64decode(w["value"])
                        val = json.loads(raw.decode())

                        if "orderId" in val:
                            orders.append(val)

                    except Exception:
                        pass

    return orders


# ================== SAVE ==================
def save_bronze(order):
    obj = f"bronze/orders/{order['orderId']}.json"
    data = json.dumps(order, indent=2).encode()
    minio.put_object(
        BUCKET, obj,
        data=io.BytesIO(data),
        length=len(data),
        content_type="application/json"
    )
    print(f"Saved {order['orderId']}")

# ================== MAIN ==================
def main():
    print("FULL LEDGER SCAN START")

    newest_block = fetch_block("newest")
    max_block = get_block_number(newest_block)

    print(f"Total blocks = {max_block + 1}")

    total_orders = 0

    for i in range(0, max_block + 1):
        print(f"Scan block {i}")
        blk = fetch_block(i)
        orders = extract_orders(blk)

        for o in orders:
            save_bronze(o)
            total_orders += 1

    print(f"DONE – Total orders stored: {total_orders}")

if __name__ == "__main__":
    main()
