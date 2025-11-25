package chaincode

import (
    "encoding/json"
    "fmt"
    "strconv"
    "strings"

    "github.com/hyperledger/fabric-contract-api-go/contractapi"
)

type NorthwindContract struct {
    contractapi.Contract
}

// ====== Models ======

type Customer struct {
    CustomerID   string `json:"customerID"`
    CompanyName  string `json:"companyName"`
    ContactName  string `json:"contactName"`
    ContactTitle string `json:"contactTitle"`
    City         string `json:"city"`
    Country      string `json:"country"`
}

type Category struct {
    CategoryID   string `json:"categoryID"`
    CategoryName string `json:"categoryName"`
    Description  string `json:"description"`
}

type Employee struct {
    EmployeeID    string `json:"employeeID"`
    EmployeeTitle string `json:"employeeTitle"`
    City          string `json:"city"`
    Country       string `json:"country"`
    ReportsTo     string `json:"reportsTo"`
}

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

type Order struct {
    OrderID      string  `json:"orderID"`
    CustomerID   string  `json:"customerID"`
    EmployeeID   string  `json:"employeeID"`
    OrderDate    string  `json:"orderDate"`    // yyyy-mm-dd hoặc mm/dd/yyyy
    RequiredDate string  `json:"requiredDate"` // yyyy-mm-dd hoặc mm/dd/yyyy
    ShippedDate  string  `json:"shippedDate"`  // yyyy-mm-dd hoặc mm/dd/yyyy
    ShipperID    string  `json:"shipperID"`
    Freight      float64 `json:"freight"`
}

type OrderDetail struct {
    OrderID   string  `json:"orderID"`
    ProductID string  `json:"productID"`
    UnitPrice float64 `json:"unitPrice"`
    Quantity  int     `json:"quantity"`
    Discount  float64 `json:"discount"` // 0..1
}

// ====== Helpers ======

func putJSON(ctx contractapi.TransactionContextInterface, key string, v interface{}) error {
    b, err := json.Marshal(v)
    if err != nil {
        return err
    }
    return ctx.GetStub().PutState(key, b)
}

func getJSON[T any](ctx contractapi.TransactionContextInterface, key string, out *T) (bool, error) {
    b, err := ctx.GetStub().GetState(key)
    if err != nil {
        return false, fmt.Errorf("read state error: %v", err)
    }
    if b == nil {
        return false, nil
    }
    if err := json.Unmarshal(b, out); err != nil {
        return false, err
    }
    return true, nil
}

func k(prefix, id string) string { return prefix + ":" + id }

// ====== Customers ======

func (c *NorthwindContract) AddCustomer(ctx contractapi.TransactionContextInterface,
    customerID, companyName, contactName, contactTitle, city, country string) error {

    key := k("customer", customerID)
    if v, err := ctx.GetStub().GetPrivateData("CustomerPrivateCollection", key); err != nil {
        return err
    } else if v != nil {
        return fmt.Errorf("customer %s already exists", customerID)
    }

    customer := Customer{
        CustomerID:   customerID,
        CompanyName:  companyName,
        ContactName:  contactName,
        ContactTitle: contactTitle,
        City:         city,
        Country:      country,
    }

    bytes, err := json.Marshal(customer)
    if err != nil {
        return err
    }

    return ctx.GetStub().PutPrivateData("CustomerPrivateCollection", key, bytes)
}

func (c *NorthwindContract) GetCustomer(ctx contractapi.TransactionContextInterface, customerID string) (*Customer, error) {
    key := k("customer", customerID)
    bytes, err := ctx.GetStub().GetPrivateData("CustomerPrivateCollection", key)
    if err != nil {
        return nil, err
    }
    if bytes == nil {
        return nil, fmt.Errorf("Customer %s not found", customerID)
    }

    var customer Customer
    if err := json.Unmarshal(bytes, &customer); err != nil {
        return nil, err
    }
    return &customer, nil
}


// ====== Categories ======

func (c *NorthwindContract) AddCategory(ctx contractapi.TransactionContextInterface,
    categoryID, categoryName, description string) error {

    key := k("category", categoryID)
    if v, err := ctx.GetStub().GetState(key); err != nil {
        return err
    } else if v != nil {
        return fmt.Errorf("category %s already exists", categoryID)
    }
    return putJSON(ctx, key, Category{
        CategoryID:   categoryID,
        CategoryName: categoryName,
        Description:  description,
    })
}

func (c *NorthwindContract) GetCategory(ctx contractapi.TransactionContextInterface, categoryID string) (*Category, error) {
    var x Category
    found, err := getJSON(ctx, k("category", categoryID), &x)
    if err != nil {
        return nil, err
    }
    if !found {
        return nil, fmt.Errorf("category %s not found", categoryID)
    }
    return &x, nil
}

// ====== Employees ======

func (c *NorthwindContract) AddEmployee(ctx contractapi.TransactionContextInterface,
    employeeID, employeeTitle, city, country, reportsTo string) error {

    // Kiểm tra quyền Org
    mspid, _ := ctx.GetClientIdentity().GetMSPID()
    if mspid != "Org3MSP" {
        return fmt.Errorf("Only Org3 can add employees")
    }

    // Tạo key và kiểm tra trùng
    key := k("employee", employeeID)
    if v, err := ctx.GetStub().GetPrivateData("EmployeePrivateCollection", key); err != nil {
        return err
    } else if v != nil {
        return fmt.Errorf("employee %s already exists", employeeID)
    }

    // Tạo struct Employee đúng định dạng
    employee := Employee{
        EmployeeID:    employeeID,
        EmployeeTitle: employeeTitle,
        City:          city,
        Country:       country,
        ReportsTo:     reportsTo,
    }

    // Mã hóa JSON và lưu vào private data
    bytes, err := json.Marshal(employee)
    if err != nil {
        return err
    }

    return ctx.GetStub().PutPrivateData("EmployeePrivateCollection", key, bytes)
}


func (c *NorthwindContract) GetEmployee(ctx contractapi.TransactionContextInterface, employeeID string) (*Employee, error) {
    key := k("employee", employeeID)
    bytes, err := ctx.GetStub().GetPrivateData("EmployeePrivateCollection", key)
    if err != nil {
        return nil, err
    }
    if bytes == nil {
        return nil, fmt.Errorf("Employee %s not found", employeeID)
    }

    var employee Employee
    if err := json.Unmarshal(bytes, &employee); err != nil {
        return nil, err
    }
    return &employee, nil
}

// ====== Products ======

func (c *NorthwindContract) AddProduct(ctx contractapi.TransactionContextInterface,
    productID, productName, quantityPerUnit, unitPriceStr, discontinuedStr, categoryID string) error {

    unitPrice, err := strconv.ParseFloat(unitPriceStr, 64)
    if err != nil {
        return fmt.Errorf("invalid unitPrice: %v", err)
    }
    disc := strings.EqualFold(discontinuedStr, "1") || strings.EqualFold(discontinuedStr, "true")

    // optional referential check
    if v, _ := ctx.GetStub().GetState(k("category", categoryID)); v == nil {
        return fmt.Errorf("category %s not found", categoryID)
    }

    key := k("product", productID)
    if v, err := ctx.GetStub().GetState(key); err != nil {
        return err
    } else if v != nil {
        return fmt.Errorf("product %s already exists", productID)
    }
    return putJSON(ctx, key, Product{
        ProductID:       productID,
        ProductName:     productName,
        QuantityPerUnit: quantityPerUnit,
        UnitPrice:       unitPrice,
        Discontinued:    disc,
        CategoryID:      categoryID,
    })
}

func (c *NorthwindContract) GetProduct(ctx contractapi.TransactionContextInterface, productID string) (*Product, error) {
    var x Product
    found, err := getJSON(ctx, k("product", productID), &x)
    if err != nil {
        return nil, err
    }
    if !found {
        return nil, fmt.Errorf("product %s not found", productID)
    }
    return &x, nil
}

// ====== Shippers ======

func (c *NorthwindContract) AddShipper(ctx contractapi.TransactionContextInterface, shipperID, companyName string) error {
    key := k("shipper", shipperID)
    if v, err := ctx.GetStub().GetState(key); err != nil {
        return err
    } else if v != nil {
        return fmt.Errorf("shipper %s already exists", shipperID)
    }
    return putJSON(ctx, key, Shipper{
        ShipperID:   shipperID,
        CompanyName: companyName,
    })
}

func (c *NorthwindContract) GetShipper(ctx contractapi.TransactionContextInterface, shipperID string) (*Shipper, error) {
    var x Shipper
    found, err := getJSON(ctx, k("shipper", shipperID), &x)
    if err != nil {
        return nil, err
    }
    if !found {
        return nil, fmt.Errorf("shipper %s not found", shipperID)
    }
    return &x, nil
}

// ====== Orders ======

func (c *NorthwindContract) AddOrder(ctx contractapi.TransactionContextInterface,
    orderID, customerID, employeeID, orderDate, requiredDate, shippedDate, shipperID, freightStr string) error {

    // referential checks (optional)
    if v, _ := ctx.GetStub().GetState(k("customer", customerID)); v == nil {
        return fmt.Errorf("customer %s not found", customerID)
    }
    if v, _ := ctx.GetStub().GetState(k("employee", employeeID)); v == nil {
        return fmt.Errorf("employee %s not found", employeeID)
    }
    if v, _ := ctx.GetStub().GetState(k("shipper", shipperID)); v == nil {
        return fmt.Errorf("shipper %s not found", shipperID)
    }

    freight, err := strconv.ParseFloat(freightStr, 64)
    if err != nil {
        return fmt.Errorf("invalid freight: %v", err)
    }

    kkey := k("order", orderID)
    if v, err := ctx.GetStub().GetPrivateData("OrderPrivateCollection", key); err != nil {
        return err
    } else if v != nil {
        return fmt.Errorf("order %s already exists", orderID)
    }

    order := Order{
        OrderID:    orderID,
        CustomerID: customerID,
        OrderDate:  orderDate,
        ShipperID:  shipperID,
        Freight:    freight,
    }

    bytes, err := json.Marshal(order)
    if err != nil {
        return err
    }

    return ctx.GetStub().PutPrivateData("OrderPrivateCollection", key, bytes)
    // index: orders by customer
    idxKey, err := ctx.GetStub().CreateCompositeKey("order~customer", []string{customerID, orderID})
    if err != nil {
        return err
    }
    return ctx.GetStub().PutState(idxKey, []byte{0})
}

func (c *NorthwindContract) GetOrder(ctx contractapi.TransactionContextInterface, orderID string) (*Order, error) {
    key := k("order", orderID)
    bytes, err := ctx.GetStub().GetPrivateData("OrderPrivateCollection", key)
    if err != nil {
        return nil, err
    }
    if bytes == nil {
        return nil, fmt.Errorf("Order %s not found", orderID)
    }

    var order Order
    if err := json.Unmarshal(bytes, &order); err != nil {
        return nil, err
    }
    return &order, nil
}

func (c *NorthwindContract) GetOrdersByCustomer(ctx contractapi.TransactionContextInterface, customerID string) ([]*Order, error) {
    iter, err := ctx.GetStub().GetStateByPartialCompositeKey("order~customer", []string{customerID})
    if err != nil {
        return nil, err
    }
    defer iter.Close()

    var res []*Order
    for iter.HasNext() {
        kv, err := iter.Next()
        if err != nil {
            return nil, err
        }
        _, parts, err := ctx.GetStub().SplitCompositeKey(kv.Key)
        if err != nil || len(parts) != 2 {
            return nil, fmt.Errorf("invalid composite key")
        }
        orderID := parts[1]
        var o Order
        found, err := getJSON(ctx, k("order", orderID), &o)
        if err != nil {
            return nil, err
        }
        if found {
            res = append(res, &o)
        }
    }
    return res, nil
}

// ====== OrderDetails ======

func (c *NorthwindContract) AddOrderDetail(ctx contractapi.TransactionContextInterface,
    orderID, productID, unitPriceStr, quantityStr, discountStr string) error {

    // referential checks
    if v, _ := ctx.GetStub().GetState(k("order", orderID)); v == nil {
        return fmt.Errorf("order %s not found", orderID)
    }
    if v, _ := ctx.GetStub().GetState(k("product", productID)); v == nil {
        return fmt.Errorf("product %s not found", productID)
    }

    unitPrice, err := strconv.ParseFloat(unitPriceStr, 64)
    if err != nil {
        return fmt.Errorf("invalid unitPrice: %v", err)
    }
    quantity, err := strconv.Atoi(quantityStr)
    if err != nil {
        return fmt.Errorf("invalid quantity: %v", err)
    }
    discount, err := strconv.ParseFloat(discountStr, 64)
    if err != nil || discount < 0 || discount > 1 {
        return fmt.Errorf("invalid discount (0..1): %v", err)
    }

    odKey, err := ctx.GetStub().CreateCompositeKey("orderDetail", []string{orderID, productID})
    if err != nil {
        return err
    }
    if v, err := ctx.GetStub().GetState(odKey); err != nil {
        return err
    } else if v != nil {
        return fmt.Errorf("order detail (%s,%s) already exists", orderID, productID)
    }

    return putJSON(ctx, odKey, OrderDetail{
        OrderID:   orderID,
        ProductID: productID,
        UnitPrice: unitPrice,
        Quantity:  quantity,
        Discount:  discount,
    })
}

func (c *NorthwindContract) GetOrderDetailsByOrder(ctx contractapi.TransactionContextInterface, orderID string) ([]*OrderDetail, error) {
    iter, err := ctx.GetStub().GetStateByPartialCompositeKey("orderDetail", []string{orderID})
    if err != nil {
        return nil, err
    }
    defer iter.Close()

    var res []*OrderDetail
    for iter.HasNext() {
        kv, err := iter.Next()
        if err != nil {
            return nil, err
        }
        var od OrderDetail
        if err := json.Unmarshal(kv.Value, &od); err != nil {
            return nil, err
        }
        res = append(res, &od)
    }
    return res, nil
}

func (c *NorthwindContract) GetOrderTotal(ctx contractapi.TransactionContextInterface, orderID string) (string, error) {
    details, err := c.GetOrderDetailsByOrder(ctx, orderID)
    if err != nil {
        return "", err
    }
    total := 0.0
    for _, d := range details {
        line := d.UnitPrice * float64(d.Quantity) * (1.0 - d.Discount)
        total += line
    }
    return fmt.Sprintf("%.2f", total), nil
}
