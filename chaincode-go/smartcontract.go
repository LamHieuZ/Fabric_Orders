package main

import (

	"encoding/base64"
	"encoding/json"
	"fmt"
    "strconv"

	"github.com/hyperledger/fabric-chaincode-go/shim"
	"github.com/hyperledger/fabric-contract-api-go/contractapi"
)

// =======================
// Structs
// =======================

// Public Data
type Product struct {
    ProductID       string  `json:"productID"`
    ProductName     string  `json:"productName"`
    QuantityPerUnit string  `json:"quantityPerUnit"`
    UnitPrice       float64 `json:"unitPrice"`
    Discontinued    bool    `json:"discontinued"`
    CategoryID      string  `json:"categoryID"`
}


type Shipper struct {
    ShipperID   string `json:"shipperID"`
    CompanyName string `json:"companyName"`
}

// Private Data
type CustomerPrivate struct {
    CustomerID    string `json:"customerID"`
    CompanyName   string `json:"companyName"`
    ContactName   string `json:"contactName"`
    ContactTitle  string `json:"contactTitle"`
    City          string `json:"city"`
    Country       string `json:"country"`
}


type Order struct {
    OrderID      string `json:"orderID"`
    CustomerID   string `json:"customerID"`
    Employee     string `json:"employee"`
    OrderDate    string `json:"orderDate"`
    RequiredDate string `json:"requiredDate"`
    ShippedDate  string `json:"shippedDate"`
    ShipperID    string `json:"shipperID"`
    Freight      float64 `json:"freight"`
}

type OrderDetail struct {
    OrderID   string  `json:"orderID"`
    ProductID string  `json:"productID"`
    UnitPrice float64 `json:"unitPrice"`
    Quantity  int     `json:"quantity"`
    Discount  float64 `json:"discount"`
}

type EmployeePrivate struct {
    EmployeeID   string `json:"employeeID"`
    EmployeeName string `json:"employeeName"`
    Title        string `json:"title"`
    City         string `json:"city"`
    Country      string `json:"country"`
    ReportsTo    string `json:"reportsTo"`
}


// =======================
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

// =======================
// Create functions
// =======================

// Org1: Create Customer
func (s *SmartContract) CreateCustomer(ctx contractapi.TransactionContextInterface) error {
    transientMap, err := ctx.GetStub().GetTransient()
    if err != nil {
        return fmt.Errorf("failed to get transient map: %v", err)
    }

    customerJSON, ok := transientMap["customer"]
    if !ok {
        return fmt.Errorf("transient map missing 'customer' key")
    }

    var customer CustomerPrivate
    if err := json.Unmarshal(customerJSON, &customer); err != nil {
        return fmt.Errorf("failed to unmarshal customer JSON: %v", err)
    }

    // Validate dữ liệu
    if customer.CustomerID == "" {
        return fmt.Errorf("customerID is empty")
    }
    if customer.CompanyName == "" || customer.ContactName == "" {
        return fmt.Errorf("companyName or contactName is missing")
    }

    // Verify org
    if err := verifyClientOrgMatchesPeerOrg(ctx); err != nil {
        return fmt.Errorf("org verification failed: %v", err)
    }

    // Lấy collection riêng cho Customer
    collection, err := getCollectionName(ctx, "Customer")
    if err != nil {
        return fmt.Errorf("failed to get collection name: %v", err)
    }

    // Kiểm tra trùng ID
    existing, err := ctx.GetStub().GetPrivateData(collection, customer.CustomerID)
    if err != nil {
        return fmt.Errorf("failed to check existing customer: %v", err)
    }
    if existing != nil {
        return fmt.Errorf("customer with ID %s already exists", customer.CustomerID)
    }

    // Ghi vào private data
    return ctx.GetStub().PutPrivateData(collection, customer.CustomerID, customerJSON)
}

// Org2: Create Order

func (s *SmartContract) CreateOrder(ctx contractapi.TransactionContextInterface) error {
    transientMap, err := ctx.GetStub().GetTransient()
    if err != nil {
        return err
    }

    orderJSON, ok := transientMap["order"]
    if !ok {
        return fmt.Errorf("order key not found in transient map")
    }

    var order Order
    err = json.Unmarshal(orderJSON, &order)
    if err != nil {
        return err
    }

    collection, err := getCollectionName(ctx, "Orders")
    if err != nil {
        return err
    }

    return ctx.GetStub().PutPrivateData(collection, order.OrderID, orderJSON)
}

func (s *SmartContract) CreateOrderDetail(ctx contractapi.TransactionContextInterface) error {
    transientMap, err := ctx.GetStub().GetTransient()
    if err != nil {
        return fmt.Errorf("failed to get transient: %v", err)
    }

    detailJSON, ok := transientMap["detail"]
    if !ok {
        return fmt.Errorf("detail key not found in transient map")
    }

    var detail OrderDetail
    if err := json.Unmarshal(detailJSON, &detail); err != nil {
        return fmt.Errorf("failed to unmarshal order detail: %v", err)
    }
    if detail.OrderID == "" || detail.ProductID == "" {
        return fmt.Errorf("orderID or productID is empty")
    }

    collection, err := getCollectionName(ctx, "OrderDetails")
    if err != nil {
        return err
    }

    compositeKey, err := ctx.GetStub().CreateCompositeKey("orderDetail", []string{detail.OrderID, detail.ProductID})
    if err != nil {
        return fmt.Errorf("failed to create composite key: %v", err)
    }

    return ctx.GetStub().PutPrivateData(collection, compositeKey, detailJSON)
}


// Public Product (shared)
func (s *SmartContract) CreateProduct(ctx contractapi.TransactionContextInterface, id, name, qty, priceStr, discontinuedStr, categoryID string) error {
    price, err := strconv.ParseFloat(priceStr, 64)
    if err != nil {
        return fmt.Errorf("invalid price: %v", err)
    }
    discontinued := discontinuedStr == "true"

    product := Product{
        ProductID:       id,
        ProductName:     name,
        QuantityPerUnit: qty,
        UnitPrice:       price,
        Discontinued:    discontinued,
        CategoryID:      categoryID,
    }

    productJSON, err := json.Marshal(product)
    if err != nil {
        return fmt.Errorf("failed to marshal product: %v", err)
    }

    return ctx.GetStub().PutState(id, productJSON)
}



// Org3: Create Employee
func (s *SmartContract) CreateEmployee(ctx contractapi.TransactionContextInterface) error {
    transientMap, err := ctx.GetStub().GetTransient()
    if err != nil {
        return err
    }
    employeeJSON, ok := transientMap["employee"]
    if !ok {
        return fmt.Errorf("employee key not found in transient map")
    }

    var emp EmployeePrivate
    err = json.Unmarshal(employeeJSON, &emp)
    if err != nil {
        return fmt.Errorf("failed to unmarshal employee: %v", err)
    }
    if emp.EmployeeID == "" {
        return fmt.Errorf("employeeID is empty")
    }

    err = verifyClientOrgMatchesPeerOrg(ctx)
    if err != nil {
        return err
    }

    collection, err := getCollectionName(ctx, "Employee")
    if err != nil {
        return err
    }

    return ctx.GetStub().PutPrivateData(collection, emp.EmployeeID, employeeJSON)
}


// Public Shipper
func (s *SmartContract) CreateShipper(ctx contractapi.TransactionContextInterface, shipperID string, companyName string) error {
    // Kiểm tra shipper đã tồn tại chưa
    existing, err := ctx.GetStub().GetState(shipperID)
    if err != nil {
        return fmt.Errorf("failed to read shipper %s: %v", shipperID, err)
    }
    if existing != nil {
        return fmt.Errorf("shipper %s already exists", shipperID)
    }

    // Tạo struct
    shipper := Shipper{
        ShipperID:   shipperID,
        CompanyName: companyName,
    }

    // Convert sang JSON
    shipperJSON, err := json.Marshal(shipper)
    if err != nil {
        return fmt.Errorf("failed to marshal shipper: %v", err)
    }

    // Ghi vào ledger
    return ctx.GetStub().PutState(shipperID, shipperJSON)
}
// =======================
// Read functions
// =======================

// Read private customer
func (s *SmartContract) ReadCustomer(ctx contractapi.TransactionContextInterface, id string) (*CustomerPrivate, error) {
	collection, err := getCollectionName(ctx, "Customer")
	if err != nil {
		return nil, err
	}
	data, err := ctx.GetStub().GetPrivateData(collection, id)
	if err != nil {
		return nil, err
	}
	if data == nil {
		return nil, fmt.Errorf("customer %v not found", id)
	}
	var cust CustomerPrivate
	json.Unmarshal(data, &cust)
	return &cust, nil
}

// Read private order
func (s *SmartContract) ReadOrder(ctx contractapi.TransactionContextInterface, orderID string) (*Order, error) {
    collection, err := getCollectionName(ctx, "Orders")
    if err != nil {
        return nil, err
    }

    data, err := ctx.GetStub().GetPrivateData(collection, orderID)
    if err != nil {
        return nil, err
    }
    if data == nil {
        return nil, fmt.Errorf("order %s not found", orderID)
    }

    var order Order
    err = json.Unmarshal(data, &order)
    if err != nil {
        return nil, err
    }
    return &order, nil
}
// Read private order detail
func (s *SmartContract) ReadOrderDetail(ctx contractapi.TransactionContextInterface, orderID string, productID string) (*OrderDetail, error) {
    collection, err := getCollectionName(ctx, "OrderDetails")
    if err != nil {
        return nil, err
    }

    compositeKey, err := ctx.GetStub().CreateCompositeKey("orderDetail", []string{orderID, productID})
    if err != nil {
        return nil, fmt.Errorf("failed to create composite key: %v", err)
    }

    data, err := ctx.GetStub().GetPrivateData(collection, compositeKey)
    if err != nil {
        return nil, err
    }
    if data == nil {
        return nil, fmt.Errorf("order detail %s not found", compositeKey)
    }

    var detail OrderDetail
    if err := json.Unmarshal(data, &detail); err != nil {
        return nil, err
    }
    return &detail, nil
}

// Read all order details by OrderID
func (s *SmartContract) ReadOrderDetailsByOrderID(ctx contractapi.TransactionContextInterface, orderID string) ([]*OrderDetail, error) {
    collection, err := getCollectionName(ctx, "OrderDetails")
    if err != nil {
        return nil, err
    }

    resultsIterator, err := ctx.GetStub().GetPrivateDataByPartialCompositeKey(collection, "orderDetail", []string{orderID})
    if err != nil {
        return nil, err
    }
    defer resultsIterator.Close()

    var details []*OrderDetail
    for resultsIterator.HasNext() {
        queryResponse, err := resultsIterator.Next()
        if err != nil {
            return nil, err
        }

        _, compositeKeyParts, err := ctx.GetStub().SplitCompositeKey(queryResponse.Key)
        if err != nil {
            return nil, err
        }

        var detail OrderDetail
        if err := json.Unmarshal(queryResponse.Value, &detail); err != nil {
            return nil, err
        }

        // Gán lại OrderID và ProductID từ composite key để chắc chắn
        detail.OrderID = compositeKeyParts[0]
        detail.ProductID = compositeKeyParts[1]

        details = append(details, &detail)
    }

    return details, nil
}



// Read public product
func (s *SmartContract) ReadProduct(ctx contractapi.TransactionContextInterface, id string) (*Product, error) {
    data, err := ctx.GetStub().GetState(id)
    if err != nil {
        return nil, fmt.Errorf("failed to read product %s: %v", id, err)
    }
    if data == nil {
        return nil, fmt.Errorf("product %s not found", id)
    }

    var p Product
    if err := json.Unmarshal(data, &p); err != nil {
        return nil, fmt.Errorf("failed to unmarshal product %s: %v", id, err)
    }

    return &p, nil
}

// Read private employee
func (s *SmartContract) ReadEmployee(ctx contractapi.TransactionContextInterface, id string) (*EmployeePrivate, error) {
    collection, err := getCollectionName(ctx, "Employee")
    if err != nil {
        return nil, err
    }

    data, err := ctx.GetStub().GetPrivateData(collection, id)
    if err != nil {
        return nil, err
    }
    if data == nil {
        return nil, fmt.Errorf("employee %s not found", id)
    }

    var emp EmployeePrivate
    if err := json.Unmarshal(data, &emp); err != nil {
        return nil, fmt.Errorf("failed to unmarshal employee %s: %v", id, err)
    }

    return &emp, nil
}

// Read public shipper
func (s *SmartContract) ReadShipper(ctx contractapi.TransactionContextInterface, id string) (*Shipper, error) {
    data, err := ctx.GetStub().GetState(id)
    if err != nil {
        return nil, fmt.Errorf("failed to read shipper %s: %v", id, err)
    }
    if data == nil {
        return nil, fmt.Errorf("shipper %s not found", id)
    }

    var shipper Shipper
    if err := json.Unmarshal(data, &shipper); err != nil {
        return nil, fmt.Errorf("failed to unmarshal shipper %s: %v", id, err)
    }

    return &shipper, nil
}
// Read all shippers`
func (s *SmartContract) GetAllShippers(ctx contractapi.TransactionContextInterface) ([]*Shipper, error) {
    resultsIterator, err := ctx.GetStub().GetStateByRange("1", "999")
    if err != nil {
        return nil, err
    }
    defer resultsIterator.Close()

    var shippers []*Shipper
    for resultsIterator.HasNext() {
        queryResponse, err := resultsIterator.Next()
        if err != nil {
            return nil, err
        }

        var shipper Shipper
        if err := json.Unmarshal(queryResponse.Value, &shipper); err != nil {
            continue // skip invalid entries
        }
        shippers = append(shippers, &shipper)
    }

    return shippers, nil
}

