import csv
import subprocess
import time
import os

# ==== THÔNG TIN CHUỖI VÀ PEER ====
CHANNEL = "mychannel"
CHAINCODE = "mycc"  # đổi thành tên chaincode của bạn, ví dụ mycc
ORDERER = "localhost:7050"
ORDERER_TLS = "--ordererTLSHostnameOverride=orderer.example.com"
PEER1 = "localhost:7051"
PEER2 = "localhost:9051"
ORDERER_CA = os.environ["ORDERER_CA"]
ORG1_CA = os.environ["PEER0_ORG1_CA"]
ORG2_CA = os.environ["PEER0_ORG2_CA"]

# ==== ĐỌC FILE CSV ====
with open('orders.csv', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        args = [
            "peer", "chaincode", "invoke",
            "-o", ORDERER,
            ORDERER_TLS,
            "--tls", "--cafile", ORDERER_CA,
            "-C", CHANNEL,
            "-n", CHAINCODE,
            "--peerAddresses", PEER1, "--tlsRootCertFiles", ORG1_CA,
            "--peerAddresses", PEER2, "--tlsRootCertFiles", ORG2_CA,
            "-c",
            (
                f'{{"function":"CreateOrder","Args":["{row["orderID"]}","{row["orderDate"]}",'
                f'"{row["requiredDate"]}","{row["shippedDate"]}","{row["companyName"]}",'
               f'"{row["employeeName"]}","{row["companyName_shipper"]}","{row["productName"]}",'
                f'"{row["quantity"]}","{row["unitPrice"]}","{row["discount"]}","{row["totalPrice"]}"]}}'
            )
        ]

        print(f"\n📦 Thêm order: {row['orderID']} ...")
        subprocess.run(args)
        time.sleep(1)  # chờ 1s giữa các lần invoke
