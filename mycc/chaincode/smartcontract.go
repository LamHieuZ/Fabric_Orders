package chaincode

import (
	"encoding/json"
	"fmt"

	"github.com/hyperledger/fabric-contract-api-go/contractapi"
)

// SmartContract provides functions for managing Orders
type SmartContract struct {
	contractapi.Contract
}

// Order describes details of a purchase order
type Order struct {
	OrderID            string  `json:"orderID"`
	OrderDate          string  `json:"orderDate"`
	RequiredDate       string  `json:"requiredDate"`
	ShippedDate        string  `json:"shippedDate"`
	CompanyName        string  `json:"companyName"`
	EmployeeName       string  `json:"employeeName"`
	CompanyNameShipper string  `json:"companyName_shipper"`
	ProductName        string  `json:"productName"`
	Quantity           int     `json:"quantity"`
	UnitPrice          float64 `json:"unitPrice"`
	Discount           float64 `json:"discount"`
	TotalPrice         float64 `json:"totalPrice"`
}

// InitLedger adds a base set of orders to the ledger
func (s *SmartContract) InitLedger(ctx contractapi.TransactionContextInterface) error {
	orders := []Order{
		{OrderID: "10000", OrderDate: "7/4/2013", RequiredDate: "8/1/2013", ShippedDate: "7/16/2013",
			CompanyName: "Vins et alcools Chevalier", EmployeeName: "Steven Buchanan", CompanyNameShipper: "Federal Shipping",
			ProductName: "Queso Cabrales", Quantity: 12, UnitPrice: 14, Discount: 0, TotalPrice: 168},
	}

	for _, order := range orders {
		orderJSON, err := json.Marshal(order)
		if err != nil {
			return err
		}

		err = ctx.GetStub().PutState(order.OrderID, orderJSON)
		if err != nil {
			return fmt.Errorf("failed to put to world state. %v", err)
		}
	}
	return nil
}

// CreateOrder issues a new order to the world state with given details.
func (s *SmartContract) CreateOrder(ctx contractapi.TransactionContextInterface,
	orderID string, orderDate string, requiredDate string, shippedDate string,
	companyName string, employeeName string, companyNameShipper string,
	productName string, quantity int, unitPrice float64, discount float64, totalPrice float64) error {

	exists, err := s.OrderExists(ctx, orderID)
	if err != nil {
		return err
	}
	if exists {
		return fmt.Errorf("the order %s already exists", orderID)
	}

	order := Order{
		OrderID:            orderID,
		OrderDate:          orderDate,
		RequiredDate:       requiredDate,
		ShippedDate:        shippedDate,
		CompanyName:        companyName,
		EmployeeName:       employeeName,
		CompanyNameShipper: companyNameShipper,
		ProductName:        productName,
		Quantity:           quantity,
		UnitPrice:          unitPrice,
		Discount:           discount,
		TotalPrice:         totalPrice,
	}
	orderJSON, err := json.Marshal(order)
	if err != nil {
		return err
	}

	return ctx.GetStub().PutState(orderID, orderJSON)
}

// ReadOrder returns the order stored in the world state with given id.
func (s *SmartContract) ReadOrder(ctx contractapi.TransactionContextInterface, orderID string) (*Order, error) {
	orderJSON, err := ctx.GetStub().GetState(orderID)
	if err != nil {
		return nil, fmt.Errorf("failed to read from world state: %v", err)
	}
	if orderJSON == nil {
		return nil, fmt.Errorf("the order %s does not exist", orderID)
	}

	var order Order
	err = json.Unmarshal(orderJSON, &order)
	if err != nil {
		return nil, err
	}

	return &order, nil
}

// UpdateOrder updates an existing order in the world state.
func (s *SmartContract) UpdateOrder(ctx contractapi.TransactionContextInterface,
	orderID string, orderDate string, requiredDate string, shippedDate string,
	companyName string, employeeName string, companyNameShipper string,
	productName string, quantity int, unitPrice float64, discount float64, totalPrice float64) error {

	exists, err := s.OrderExists(ctx, orderID)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("the order %s does not exist", orderID)
	}

	order := Order{
		OrderID:            orderID,
		OrderDate:          orderDate,
		RequiredDate:       requiredDate,
		ShippedDate:        shippedDate,
		CompanyName:        companyName,
		EmployeeName:       employeeName,
		CompanyNameShipper: companyNameShipper,
		ProductName:        productName,
		Quantity:           quantity,
		UnitPrice:          unitPrice,
		Discount:           discount,
		TotalPrice:         totalPrice,
	}

	orderJSON, err := json.Marshal(order)
	if err != nil {
		return err
	}

	return ctx.GetStub().PutState(orderID, orderJSON)
}

// DeleteOrder deletes an order from the world state.
func (s *SmartContract) DeleteOrder(ctx contractapi.TransactionContextInterface, orderID string) error {
	exists, err := s.OrderExists(ctx, orderID)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("the order %s does not exist", orderID)
	}

	return ctx.GetStub().DelState(orderID)
}

// OrderExists returns true when order with given ID exists in world state
func (s *SmartContract) OrderExists(ctx contractapi.TransactionContextInterface, orderID string) (bool, error) {
	orderJSON, err := ctx.GetStub().GetState(orderID)
	if err != nil {
		return false, fmt.Errorf("failed to read from world state: %v", err)
	}

	return orderJSON != nil, nil
}

// GetAllOrders returns all orders found in world state
func (s *SmartContract) GetAllOrders(ctx contractapi.TransactionContextInterface) ([]*Order, error) {
	resultsIterator, err := ctx.GetStub().GetStateByRange("", "")
	if err != nil {
		return nil, err
	}
	defer resultsIterator.Close()

	var orders []*Order
	for resultsIterator.HasNext() {
		queryResponse, err := resultsIterator.Next()
		if err != nil {
			return nil, err
		}

		var order Order
		err = json.Unmarshal(queryResponse.Value, &order)
		if err != nil {
			return nil, err
		}
		orders = append(orders, &order)
	}

	return orders, nil
}
