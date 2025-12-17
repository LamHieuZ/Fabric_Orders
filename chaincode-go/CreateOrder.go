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
type PublicOrder struct {
    Order
    OrderDetail *OrderDetail 
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
    // Chỉ Org2 mới được tạo đơn hàng
    msp, _ := ctx.GetClientIdentity().GetMSPID()
    if msp != "Org2MSP" {
        return fmt.Errorf("only Org2 can create order")
    }

    // Kiểm tra orderID đã tồn tại chưa trong world state
    existing, err := ctx.GetStub().GetState(orderID)
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


    // Ghi Order vào world state
    ordJSON, err := json.Marshal(ord)
    if err != nil {
        return err
    }
    if err := ctx.GetStub().PutState(orderID, ordJSON); err != nil {
        return err
    }

    // Ghi OrderDetail vào private data Org2
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
    // Chỉ Org1 mới được verify
    msp, _ := ctx.GetClientIdentity().GetMSPID()
    if msp != "Org1MSP" {
        return fmt.Errorf("only Org1 can verify customer")
    }

    // Lấy order từ world state
    orderJSON, err := ctx.GetStub().GetState(orderID)
    if err != nil || orderJSON == nil {
        return fmt.Errorf("order not found in world state")
    }

    var ord Order
    if err := json.Unmarshal(orderJSON, &ord); err != nil {
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

    // Nếu khớp thì Org1 approve và cập nhật trạng thái
    ord.ApprovedByOrg1 = true
    ord.Status = "APPROVED"

    updated, err := json.Marshal(ord)
    if err != nil {
        return err
    }

    // Ghi lại vào world state
    return ctx.GetStub().PutState(orderID, updated)
}


// transient: customerId
func (s *SmartContract) ShareOrderDetailsToOrg1(
    ctx contractapi.TransactionContextInterface,
    orderID string,
) error {

    if msp, _ := ctx.GetClientIdentity().GetMSPID(); msp != "Org2MSP" {
        return fmt.Errorf("only Org2 can share order details")
    }

    // Lấy private details của Org2
    detJSON, err := ctx.GetStub().
        GetPrivateData("Org2MSPOrderDetailsCollection", orderID)
    if err != nil || detJSON == nil {
        return fmt.Errorf("order details not found in Org2 private collection")
    }

    // Ghi sang collection chung Org1–Org2
    err = ctx.GetStub().PutPrivateData(
        "Org1Org2OrderDetailsShared",
        orderID,
        detJSON,
    )
    if err != nil {
        return fmt.Errorf("failed to share order details: %v", err)
    }

    return nil
}

func (s *SmartContract) PublishOrderDetails(
    ctx contractapi.TransactionContextInterface,
    orderID string,
) error {

    // Cho phép Org1 hoặc Org2
    msp, _ := ctx.GetClientIdentity().GetMSPID()
    if msp != "Org1MSP" && msp != "Org2MSP" {
        return fmt.Errorf("only Org1 or Org2 can publish order")
    }

    // 1. Read public order
    orderJSON, err := ctx.GetStub().GetState(orderID)
    if err != nil || orderJSON == nil {
        return fmt.Errorf("order not found")
    }

    var ord Order
    json.Unmarshal(orderJSON, &ord)



    if !ord.ApprovedByOrg1 {
        return fmt.Errorf("order not approved by Org1")
    }

    // 2. Read shared details
    detJSON, err := ctx.GetStub().
        GetPrivateData("Org1Org2OrderDetailsShared", orderID)
    if err != nil || detJSON == nil {
        return fmt.Errorf("shared order details not found")
    }

    var det OrderDetail
    json.Unmarshal(detJSON, &det)

    // 3. Merge & publish
    result := PublicOrder{
        Order:       ord,
        OrderDetail: &det,
    }

    finalJSON, _ := json.Marshal(result)
    return ctx.GetStub().PutState(orderID, finalJSON)
}



// ReadOrder(orderId) — Org1/Org2
func (s *SmartContract) ReadOrder(ctx contractapi.TransactionContextInterface, orderID string) (*Order, error) {
    // Lấy dữ liệu order từ world state
    b, err := ctx.GetStub().GetState(orderID)
    if err != nil {
        return nil, fmt.Errorf("failed to read order: %v", err)
    }
    if b == nil {
        return nil, fmt.Errorf("order %s not found", orderID)
    }

    var ord Order
    if err := json.Unmarshal(b, &ord); err != nil {
        return nil, fmt.Errorf("failed to unmarshal order: %v", err)
    }

    return &ord, nil
}

// ReadOrderDetailsShared(orderId) — Org1/Org2
func (s *SmartContract) ReadOrderDetailsShared(ctx contractapi.TransactionContextInterface, orderID string) (*OrderDetail, error) {
    // Đọc order từ world state
    ordJSON, err := ctx.GetStub().GetState(orderID)
    if err != nil || ordJSON == nil {
        return nil, fmt.Errorf("order not found")
    }

    var ord Order
    if err := json.Unmarshal(ordJSON, &ord); err != nil {
        return nil, fmt.Errorf("failed to unmarshal order: %v", err)
    }

    // Kiểm tra Org1 đã approve chưa
    if !ord.ApprovedByOrg1 {
        return nil, fmt.Errorf("customer not approved yet")
    }

    // Sau khi Org2 promote, OrderDetails đã được ghi vào world state
    // Ta đọc lại toàn bộ struct fullOrder (Order + OrderDetail)
    var pub PublicOrder
    if err := json.Unmarshal(ordJSON, &pub); err != nil {
        return nil, fmt.Errorf("failed to unmarshal public order: %v", err)
    }
    return pub.OrderDetail, nil

}

// Đọc tất cả Orders trong collection chung
func (s *SmartContract) ReadAllOrders(ctx contractapi.TransactionContextInterface) ([]*Order, error) {
    // Lấy iterator cho tất cả key trong world state
    resultsIterator, err := ctx.GetStub().GetStateByRange("", "")
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


func (s *SmartContract) ReadOrdersByCustomerID(
    ctx contractapi.TransactionContextInterface,
    customerId string,
) ([]*Order, error) {

    // Băm customerId bằng SHA256
    custHash := sha256Hex([]byte(customerId))

    // Duyệt toàn bộ world state
    iterator, err := ctx.GetStub().GetStateByRange("", "")
    if err != nil {
        return nil, fmt.Errorf("failed to get state by range: %v", err)
    }
    defer iterator.Close()

    var matched []*Order
    for iterator.HasNext() {
        kv, err := iterator.Next()
        if err != nil {
            return nil, fmt.Errorf("iterator error: %v", err)
        }

        var ord Order
        if err := json.Unmarshal(kv.Value, &ord); err != nil {
            continue
        }

        // So sánh hash
        if ord.CustomerIDHash == custHash {
            matched = append(matched, &ord)
        }
    }

    return matched, nil
}


func (s *SmartContract) ReadPublicOrder(
    ctx contractapi.TransactionContextInterface,
    orderID string,
) (*PublicOrder, error) {

    // Đọc dữ liệu từ world state (thống nhất key)
    b, err := ctx.GetStub().GetState(orderID)
    if err != nil {
        return nil, fmt.Errorf("failed to read order: %v", err)
    }
    if b == nil {
        return nil, fmt.Errorf("order %s not found", orderID)
    }

    var pub PublicOrder
    if err := json.Unmarshal(b, &pub); err != nil {
        return nil, fmt.Errorf("failed to unmarshal public order: %v", err)
    }

    if pub.OrderDetail == nil {
        return nil, fmt.Errorf("order %s has no details (not promoted yet)", orderID)
    }

    return &pub, nil
}

func (s *SmartContract) CreateOrderAndApprove(
    ctx contractapi.TransactionContextInterface,
    orderID string,
) error {
    msp, _ := ctx.GetClientIdentity().GetMSPID()
    if msp != "Org2MSP" {
        return fmt.Errorf("only Org2 can create and approve order")
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

    // Parse Order
    var ord Order
    if err := json.Unmarshal(ordMeta, &ord); err != nil {
        return err
    }
    ord.OrderID = orderID
    ord.CustomerIDHash = sha256Hex(cust)
    ord.Status = "APPROVED"
    ord.ApprovedByOrg1 = true

    // Parse OrderDetail
    var det OrderDetail
    if err := json.Unmarshal(ordDet, &det); err != nil {
        return err
    }
    det.OrderID = orderID

    // Ghi OrderDetail vào private collections
    detJSON, _ := json.Marshal(det)
    if err := ctx.GetStub().PutPrivateData("Org2MSPOrderDetailsCollection", orderID, detJSON); err != nil {
        return err
    }
    if err := ctx.GetStub().PutPrivateData("Org1Org2OrderDetailsShared", orderID, detJSON); err != nil {
        return err
    }

    // Ghi đè trực tiếp PublicOrder lên key orderID
    pub := PublicOrder{
        Order:       ord,
        OrderDetail: &det,
    }
    pubJSON, _ := json.Marshal(pub)
    if err := ctx.GetStub().PutState(orderID, pubJSON); err != nil {
        return err
    }

    return nil
}





