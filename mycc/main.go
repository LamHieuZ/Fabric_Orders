/*
SPDX-License-Identifier: Apache-2.0
*/

package main

import (
	"fmt"

	"github.com/hyperledger/fabric-contract-api-go/contractapi"
	"mycc/chaincode"
)

func main() {
    cc, err := contractapi.NewChaincode(&chaincode.NorthwindContract{})
    if err != nil {
        fmt.Printf("Error creating Northwind chaincode: %s", err.Error())
        return
    }
    if err := cc.Start(); err != nil {
        fmt.Printf("Error starting Northwind chaincode: %s", err.Error())
    }
}
