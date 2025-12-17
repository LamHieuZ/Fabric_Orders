package main

import (

	"encoding/base64"
	"fmt"

    "github.com/hyperledger/fabric-chaincode-go/shim"
	"github.com/hyperledger/fabric-contract-api-go/contractapi"
)


// SmartContract
// =======================
type SmartContract struct {
	contractapi.Contract
}

// =======================
// Helper functions
// =======================

// Lấy collection private của org submit transaction
func getCollectionName(ctx contractapi.TransactionContextInterface, base string) (string, error) {
    mspid, err := ctx.GetClientIdentity().GetMSPID()
    if err != nil {
        return "", err
    }
    return fmt.Sprintf("%s%sCollection", mspid, base), nil
}

// Lấy ID client (decode base64)
func submittingClientIdentity(ctx contractapi.TransactionContextInterface) (string, error) {
	b64ID, err := ctx.GetClientIdentity().GetID()
	if err != nil {
		return "", fmt.Errorf("failed to read clientID: %v", err)
	}
	decodeID, err := base64.StdEncoding.DecodeString(b64ID)
	if err != nil {
		return "", fmt.Errorf("failed base64 decode clientID: %v", err)
	}
	return string(decodeID), nil
}

// Kiểm tra client org có trùng với peer org
func verifyClientOrgMatchesPeerOrg(ctx contractapi.TransactionContextInterface) error {
	clientMSPID, err := ctx.GetClientIdentity().GetMSPID()
	if err != nil {
		return fmt.Errorf("failed getting client MSPID: %v", err)
	}
	peerMSPID, err := shim.GetMSPID()
	if err != nil {
		return fmt.Errorf("failed getting peer MSPID: %v", err)
	}
	if clientMSPID != peerMSPID {
		return fmt.Errorf("client from org %v not authorized to access peer org %v", clientMSPID, peerMSPID)
	}
	return nil
}



