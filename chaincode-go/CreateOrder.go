package main

import (
    "crypto/sha256"
    "encoding/hex"
    "encoding/json"
    "fmt"

    
    "github.com/hyperledger/fabric-contract-api-go/contractapi"
)

// =======================
// Structs
// =======================
type Order struct {
    OrderID        string  `json:"orderId"`
    CustomerIDHash string  `json:"customerIdHash"`
    Employee       int     `json:"employee"`
    OrderDate      string  `json:"orderDate"`
    RequiredDate   string  `json:"requiredDate"`
    ShippedDate    string  `json:"shippedDate"`
    ShipperID      int     `json:"shipperID"`
    Freight        float64 `json:"freight"`
    Status         string  `json:"status"`
    ApprovedByOrg1 bool    `json:"approvedByOrg1"`
}
type OrderDetail struct {
    OrderID string  `json:"orderId"`
    Items   []Item  `json:"items"`
    Total   float64 `json:"total"`
}
type Item struct {
    ProductID string  `json:"productId"`
    Quantity  int     `json:"quantity"`
    UnitPrice float64 `json:"unitPrice"`
    Discount  float64 `json:"discount"`
    Price     float64 `json:"price"`
}


// =======================
// SmartContract
// =======================

// =======================
// Helper functions
// =======================

func sha256Hex(b []byte) string {
    sum := sha256.Sum256(b)
    return hex.EncodeToString(sum[:])
}
// =======================
// Create functions
// =======================

// CreateOrder(orderId) — Org2
// transient: customerId, order, orderDetails
// "order" là JSON metadata (employee, dates, shipperID, freight)
func (s *SmartContract) CreateOrder(ctx contractapi.TransactionContextInterface, orderID string) error {
    msp, _ := ctx.GetClientIdentity().GetMSPID()
    if msp != "Org2MSP" {
        return fmt.Errorf("only Org2 can create order")
    }

    // Kiểm tra orderID đã tồn tại chưa trong collection chung
    existing, err := ctx.GetStub().GetPrivateData("Org1Org2OrdersCollection", orderID)
    if err != nil {
        return fmt.Errorf("failed to check existing order: %v", err)
    }
    if existing != nil {
        return fmt.Errorf("order %s already exists", orderID)
    }

    // Lấy transient fields
    t, err := ctx.GetStub().GetTransient()
    if err != nil {
        return err
    }

    cust := t["customerId"]
    ordMeta := t["order"]
    ordDet := t["orderDetails"]
    if cust == nil || ordMeta == nil || ordDet == nil {
        return fmt.Errorf("missing transient fields")
    }

    // Unmarshal order metadata
    var ord Order
    if err := json.Unmarshal(ordMeta, &ord); err != nil {
        return err
    }
    ord.OrderID = orderID
    ord.CustomerIDHash = sha256Hex(cust)
    ord.Status = "CREATED"
    ord.ApprovedByOrg1 = false

    ordJSON, err := json.Marshal(ord)
    if err != nil {
        return err
    }

    // Write shared Orders
    if err := ctx.GetStub().PutPrivateData("Org1Org2OrdersCollection", orderID, ordJSON); err != nil {
        return err
    }

    // Write private OrderDetails
    var det OrderDetail
    if err := json.Unmarshal(ordDet, &det); err != nil {
        return err
    }
    det.OrderID = orderID
    detJSON, err := json.Marshal(det)
    if err != nil {
        return err
    }

    if err := ctx.GetStub().PutPrivateData("Org2MSPOrderDetailsCollection", orderID, detJSON); err != nil {
        return err
    }

    return nil
}
// VerifyCustomer(orderId) — Org1
func (s *SmartContract) VerifyCustomer(ctx contractapi.TransactionContextInterface, orderID string) error {
    msp, _ := ctx.GetClientIdentity().GetMSPID()
    if msp != "Org1MSP" {
        return fmt.Errorf("only Org1 can verify customer")
    }

    // Lấy order từ collection chung
    sharedJSON, err := ctx.GetStub().GetPrivateData("Org1Org2OrdersCollection", orderID)
    if err != nil || sharedJSON == nil {
        return fmt.Errorf("order not found")
    }

    var ord Order
    if err := json.Unmarshal(sharedJSON, &ord); err != nil {
        return err
    }

    // Lấy customerId từ transient
    t, err := ctx.GetStub().GetTransient()
    if err != nil {
        return err
    }
    cust := t["customerId"]
    if cust == nil {
        return fmt.Errorf("missing customerId")
    }

    // Hash lại customerId từ Org1
    custHashOrg1 := sha256Hex(cust)

    // So sánh với hash Org2 đã lưu trong order
    if custHashOrg1 != ord.CustomerIDHash {
        return fmt.Errorf("customer mismatch between Org1 and Org2")
    }

    // Nếu khớp thì Org1 approve
    ord.ApprovedByOrg1 = true
    ord.Status = "APPROVED"

    updated, _ := json.Marshal(ord)
    return ctx.GetStub().PutPrivateData("Org1Org2OrdersCollection", orderID, updated)
}

// transient: customerId
func (s *SmartContract) PromoteDetails(ctx contractapi.TransactionContextInterface, orderID string) error {
    // Chỉ cho phép Org2 gọi hàm này
    msp, _ := ctx.GetClientIdentity().GetMSPID()
    if msp != "Org2MSP" {
        return fmt.Errorf("only Org2 can promote details")
    }

    // Lấy metadata Order từ collection chung Org1-Org2
    sharedJSON, err := ctx.GetStub().GetPrivateData("Org1Org2OrdersCollection", orderID)
    if err != nil || sharedJSON == nil {
        return fmt.Errorf("order not found")
    }

    var ord Order
    if err := json.Unmarshal(sharedJSON, &ord); err != nil {
        return err
    }

    // Kiểm tra Org1 đã approve chưa
    if !ord.ApprovedByOrg1 {
        return fmt.Errorf("order not approved by Org1")
    }

    // Lấy OrderDetail từ collection riêng của Org2
    detJSON, err := ctx.GetStub().GetPrivateData("Org2MSPOrderDetailsCollection", orderID)
    if err != nil || detJSON == nil {
        return fmt.Errorf("order details not found in Org2 collection")
    }

    // Ghi sang collection chung Org1-Org2
    if err := ctx.GetStub().PutPrivateData("Org1Org2OrderDetailsShared", orderID, detJSON); err != nil {
        return err
    }

    return nil
}

// ReadOrder(orderId) — Org1/Org2
func (s *SmartContract) ReadOrder(ctx contractapi.TransactionContextInterface, orderID string) (*Order, error) {
    b, err := ctx.GetStub().GetPrivateData("Org1Org2OrdersCollection", orderID)
    if err != nil || b == nil { return nil, fmt.Errorf("order not found") }
    var ord Order; json.Unmarshal(b, &ord)
    return &ord, nil
}

// ReadOrderDetailsShared(orderId) — Org1/Org2
func (s *SmartContract) ReadOrderDetailsShared(ctx contractapi.TransactionContextInterface, orderID string) (*OrderDetail, error) {
    // Đọc metadata để kiểm tra đã approve chưa
    ordJSON, err := ctx.GetStub().GetPrivateData("Org1Org2OrdersCollection", orderID)
    if err != nil || ordJSON == nil {
        return nil, fmt.Errorf("order not found")
    }
    var ord Order
    if err := json.Unmarshal(ordJSON, &ord); err != nil {
        return nil, fmt.Errorf("failed to unmarshal order: %v", err)
    }
    if !ord.ApprovedByOrg1 {
        return nil, fmt.Errorf("customer not approved yet")
    }

    // Trả chi tiết nếu đã approve
    b, err := ctx.GetStub().GetPrivateData("Org1Org2OrderDetailsShared", orderID)
    if err != nil || b == nil {
        return nil, fmt.Errorf("shared details not found")
    }
    var d OrderDetail
    if err := json.Unmarshal(b, &d); err != nil {
        return nil, err
    }
    return &d, nil
}

// ReadOrderDetailsPrivate(orderId) — chỉ Org2
func (s *SmartContract) ReadOrderDetailsPrivate(ctx contractapi.TransactionContextInterface, orderID string) (*OrderDetail, error) {
    msp, _ := ctx.GetClientIdentity().GetMSPID()
    if msp != "Org2MSP" {
        return nil, fmt.Errorf("only Org2 can read its private order details")
    }

    b, err := ctx.GetStub().GetPrivateData("Org2MSPOrderDetailsCollection", orderID)
    if err != nil {
        return nil, fmt.Errorf("failed to read private order details: %v", err)
    }
    if b == nil {
        return nil, fmt.Errorf("order details %s not found", orderID)
    }

    var d OrderDetail
    if err := json.Unmarshal(b, &d); err != nil {
        return nil, err
    }
    return &d, nil
}
// Đọc tất cả Orders trong collection chung
func (s *SmartContract) ReadAllOrders(ctx contractapi.TransactionContextInterface) ([]*Order, error) {
    // Lấy iterator cho tất cả key trong collection
    resultsIterator, err := ctx.GetStub().GetPrivateDataByRange("Org1Org2OrdersCollection", "", "")
    if err != nil {
        return nil, fmt.Errorf("failed to get orders: %v", err)
    }
    defer resultsIterator.Close()

    var orders []*Order
    for resultsIterator.HasNext() {
        kv, err := resultsIterator.Next()
        if err != nil {
            return nil, err
        }
        var ord Order
        if err := json.Unmarshal(kv.Value, &ord); err != nil {
            return nil, err
        }
        orders = append(orders, &ord)
    }
    return orders, nil
}

// Đọc chi tiết Order từ collection riêng của Org2
func (s *SmartContract) ReadOrderDetails(ctx contractapi.TransactionContextInterface, orderID string) (*OrderDetail, error) {
    detJSON, err := ctx.GetStub().GetPrivateData("Org2MSPOrderDetailsCollection", orderID)
    if err != nil {
        return nil, fmt.Errorf("failed to read order details: %v", err)
    }
    if detJSON == nil {
        return nil, fmt.Errorf("order details not found")
    }

    var det OrderDetail
    if err := json.Unmarshal(detJSON, &det); err != nil {
        return nil, fmt.Errorf("failed to unmarshal order details: %v", err)
    }
    return &det, nil
}
func (s *SmartContract) ReadOrdersByCustomerID(ctx contractapi.TransactionContextInterface) ([]*Order, error) {
    // Lấy customerId từ transient
    t, err := ctx.GetStub().GetTransient()
    if err != nil {
        return nil, err
    }
    cust := t["customerId"]
    if cust == nil {
        return nil, fmt.Errorf("missing customerId in transient")
    }
    custHash := sha256Hex(cust)

    // Duyệt tất cả orders trong collection chung
    resultsIterator, err := ctx.GetStub().GetPrivateDataByRange("Org1Org2OrdersCollection", "", "")
    if err != nil {
        return nil, fmt.Errorf("failed to get orders: %v", err)
    }
    defer resultsIterator.Close()

    var matched []*Order
    for resultsIterator.HasNext() {
        kv, err := resultsIterator.Next()
        if err != nil {
            return nil, err
        }
        var ord Order
        if err := json.Unmarshal(kv.Value, &ord); err != nil {
            return nil, err
        }
        // So sánh hash
        if ord.CustomerIDHash == custHash {
            matched = append(matched, &ord)
        }
    }

    return matched, nil
}

