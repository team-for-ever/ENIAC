package contracts

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/hyperledger/fabric-contract-api-go/contractapi"
)

type ProcessingStep struct {
	ResourceType     string    `json:"resourceType"`    
	ID               string    `json:"id"`              
	LinkedEventKey   string    `json:"linked_event_key"` 
	ProcessorID      string    `json:"processor_id"`   
	StepType         string    `json:"step_type"`       
	StepDate         time.Time `json:"step_date"`       
	Conditions       string    `json:"conditions"`      
	QuantityAfter    float64   `json:"quantity_after"`  
	PlantName        string    `json:"plant_name"`      
	BlockchainTxID   string    `json:"blockchain_tx_id"` 
}
type ProcessingStepHistory struct {
	TxID      string            `json:"tx_id"`
	Timestamp time.Time         `json:"timestamp"`
	IsDelete  bool              `json:"is_delete"`
	Event     *ProcessingStep   `json:"event,omitempty"`
}
type ProcessingStepContract struct {
	contractapi.Contract
}
func (c *ProcessingStepContract) authorizeMSP(ctx contractapi.TransactionContextInterface, allowed []string) error {
	mspID, err := ctx.GetClientIdentity().GetMSPID()
	if err != nil {
		return fmt.Errorf("failed to get MSP ID: %v", err)
	}
	for _, a := range allowed {
		if mspID == a {
			return nil
		}
	}
	return fmt.Errorf("unauthorized MSP: %s", mspID)
}

func (c *ProcessingStepContract) queryRangeWithPagination(
	ctx contractapi.TransactionContextInterface,
	startKey, endKey string,
	pageSize int,
	bookmark string,
) ([]*ProcessingStep, *contractapi.QueryResultMetadata, error) {
	if pageSize <= 0 {
		pageSize = 10
	}
	iter, meta, err := ctx.GetStub().GetStateByRangeWithPagination(startKey, endKey, int32(pageSize), bookmark)
	if err != nil {
		return nil, nil, err
	}
	defer iter.Close()

	var steps []*ProcessingStep
	for iter.HasNext() {
		resp, err := iter.Next()
		if err != nil {
			return nil, nil, err
		}
		var step ProcessingStep
		if err := json.Unmarshal(resp.Value, &step); err != nil { 
			continue
		}
		steps = append(steps, &step)
	}
	return steps, meta, nil
}
func (c *ProcessingStepContract) CreateProcessingStep(
	ctx contractapi.TransactionContextInterface,
	linkedEventKey, processorID, stepType, stepDate, conditions string,
	quantityAfter float64,
) (string, error) {
	if err := c.authorizeMSP(ctx, []string{"ProcessorMSP"}); err != nil {
		return "", err
	}
	if linkedEventKey == "" || processorID == "" || stepType == "" || stepDate == "" {
		return "", fmt.Errorf("missing required fields: linkedEventKey, processorID, stepType, stepDate")
	}
	if quantityAfter <= 0.0 {
		return "", fmt.Errorf("quantity_after must be positive")
	}
	t, err := time.Parse(time.RFC3339, stepDate)
	if err != nil {
		return "", fmt.Errorf("invalid step date (RFC3339): %v", err)
	}
	if err := c.validateProcessingStep(ctx, linkedEventKey, t, stepType, conditions, quantityAfter); err != nil {
		return "", err
	}
	stepID := fmt.Sprintf("PROCESSING_%s_%d", ctx.GetStub().GetTxID(), time.Now().UnixNano())
	linkedBytes, err := ctx.GetStub().GetState(linkedEventKey)
	if err != nil || linkedBytes == nil {
		return "", fmt.Errorf("linked event not found or invalid: %s", linkedEventKey)
	}
    	var plantName string
	var linkedDate time.Time
	var ce CollectionEvent
	if err := json.Unmarshal(linkedBytes, &ce); err == nil && ce.PlantName != "" {
		plantName = ce.PlantName
		linkedDate = ce.CollectionDate
	} else {
		var ps ProcessingStep
		if err := json.Unmarshal(linkedBytes, &ps); err == nil && ps.PlantName != "" {
			plantName = ps.PlantName
			linkedDate = ps.StepDate
		} else {
			return "", fmt.Errorf("invalid linked event type: %s", linkedEventKey)
		}
	}
	attrs := []string{linkedEventKey, stepType}
	key, err := ctx.GetStub().CreateCompositeKey("PROCESSINGSTEP", attrs)
	if err != nil {
		return "", err
	}
	if existing, _ := ctx.GetStub().GetState(key); existing != nil {
		return "", fmt.Errorf("processing step already exists for linked event and type: %s", key)
	}

	step := ProcessingStep{
		ResourceType:     "ProcessingStep",
		ID:               stepID,
		LinkedEventKey:   linkedEventKey,
		ProcessorID:      processorID,
		StepType:         stepType,
		StepDate:         t,
		Conditions:       conditions, 
		QuantityAfter:    quantityAfter,
		PlantName:        plantName,
		BlockchainTxID:   ctx.GetStub().GetTxID(),
	}

	stepJSON, err := json.Marshal(step)
	if err != nil {
		return "", err
	}
	if err := ctx.GetStub().PutState(key, stepJSON); err != nil {
		return "", err
	}
	ctx.GetStub().SetEvent("ProcessingStepCreated", stepJSON)
	return key, nil
}
func (c *ProcessingStepContract) validateProcessingStep(
	ctx contractapi.TransactionContextInterface,
	linkedEventKey string,
	t time.Time,
	stepType, conditions string,
	quantityAfter float64,
) error {
    	linkedBytes, err := ctx.GetStub().GetState(linkedEventKey)
	if err != nil {
		return fmt.Errorf("failed to fetch linked event: %v", err)
	}
	if linkedBytes == nil {
		return fmt.Errorf("linked event not found: %s", linkedEventKey)
	}

	var linkedDate time.Time
	var linkedQuantity float64
	var ce CollectionEvent
	if err := json.Unmarshal(linkedBytes, &ce); err == nil {
		linkedDate = ce.CollectionDate
		linkedQuantity = ce.Quantity
	} else {
		var ps ProcessingStep
		if err := json.Unmarshal(linkedBytes, &ps); err == nil {
			linkedDate = ps.StepDate
			linkedQuantity = ps.QuantityAfter
		} else {
			return fmt.Errorf("invalid linked event data: %v", err)
		}
	}
    	if t.Before(linkedDate) {
		return fmt.Errorf("step date %s before linked event date %s", t.Format(time.RFC3339), linkedDate.Format(time.RFC3339))
	}
	if quantityAfter > linkedQuantity {
		return fmt.Errorf("quantity_after %.2fkg cannot exceed previous quantity %.2fkg", quantityAfter, linkedQuantity)
	}	var condMap map[string]interface{}
	if err := json.Unmarshal([]byte(conditions), &condMap); err != nil {
		return fmt.Errorf("invalid conditions JSON: %v", err)
	}
    return nil
}
func (c *ProcessingStepContract) GetProcessingStep(ctx contractapi.TransactionContextInterface, key string) (*ProcessingStep, error) {
	if err := c.authorizeMSP(ctx, []string{"ProcessorMSP", "ManufacturerMSP", "RegulatorMSP"}); err != nil {
		return nil, err
	}
	bytes, err := ctx.GetStub().GetState(key)
	if err != nil {
		return nil, err
	}
	if bytes == nil {
		return nil, fmt.Errorf("not found: %s", key)
	}
	var step ProcessingStep
	if err := json.Unmarshal(bytes, &step); err != nil {
		return nil, err
	}
	return &step, nil
}
func (c *ProcessingStepContract) GetAllProcessingSteps(
	ctx contractapi.TransactionContextInterface,
	pageSize int,
	bookmark string,
) ([]*ProcessingStep, *contractapi.QueryResultMetadata, error) {
	if err := c.authorizeMSP(ctx, []string{"ProcessorMSP", "ManufacturerMSP", "RegulatorMSP"}); err != nil {
		return nil, nil, err
	}
	startKey, _ := ctx.GetStub().CreateCompositeKey("PROCESSINGSTEP", []string{})
	endKey, _ := ctx.GetStub().CreateCompositeKey("PROCESSINGSTEP", []string{string(0xff)})
	return c.queryRangeWithPagination(ctx, startKey, endKey, pageSize, bookmark)
}
func (c *ProcessingStepContract) GetProcessingStepsByPlant(
	ctx contractapi.TransactionContextInterface,
	plantName string,
	pageSize int,
	bookmark string,
) ([]*ProcessingStep, *contractapi.QueryResultMetadata, error) {
	if err := c.authorizeMSP(ctx, []string{"ProcessorMSP", "ManufacturerMSP", "RegulatorMSP"}); err != nil {
		return nil, nil, err
	}
	if plantName == "" {
		return nil, nil, fmt.Errorf("plantName required")
	}
	query := fmt.Sprintf(`{"selector":{"plant_name":"%s","resourceType":"ProcessingStep"}}`, plantName)
	iter, meta, err := ctx.GetStub().GetQueryResultWithPagination(query, int32(pageSize), bookmark)
	if err != nil {
		fmt.Printf("Rich query failed (%v); fallback to scan", err)
		all, _, ferr := c.GetAllProcessingSteps(ctx, pageSize, bookmark)
		if ferr != nil {
			return nil, nil, ferr
		}
		var filtered []*ProcessingStep
		for _, s := range all {
			if strings.EqualFold(s.PlantName, plantName) {
				filtered = append(filtered, s)
			}
		}
		return filtered, &contractapi.QueryResultMetadata{Bookmark: bookmark}, nil
	}
	defer iter.Close()
	var steps []*ProcessingStep
	for iter.HasNext() {
		resp, err := iter.Next()
		if err != nil {
			return nil, nil, err
		}
		var step ProcessingStep
		if err := json.Unmarshal(resp.Value, &step); err != nil {
			continue
		}
		steps = append(steps, &step)
	}
	return steps, meta, nil
}
func (c *ProcessingStepContract) GetProcessingStepsByProcessor(
	ctx contractapi.TransactionContextInterface,
	processorID string,
	pageSize int,
	bookmark string,
) ([]*ProcessingStep, *contractapi.QueryResultMetadata, error) {
	if err := c.authorizeMSP(ctx, []string{"ProcessorMSP", "ManufacturerMSP", "RegulatorMSP"}); err != nil {
		return nil, nil, err
	}
	if processorID == "" {
		return nil, nil, fmt.Errorf("processorID required")
	}
	query := fmt.Sprintf(`{"selector":{"processor_id":"%s","resourceType":"ProcessingStep"}}`, processorID)
	iter, meta, err := ctx.GetStub().GetQueryResultWithPagination(query, int32(pageSize), bookmark)
	if err != nil {
		fmt.Printf("Rich query failed (%v); fallback to scan", err)
		all, _, ferr := c.GetAllProcessingSteps(ctx, pageSize, bookmark)
		if ferr != nil {
			return nil, nil, ferr
		}
		var filtered []*ProcessingStep
		for _, s := range all {
			if strings.EqualFold(s.ProcessorID, processorID) {
				filtered = append(filtered, s)
			}
		}
		return filtered, &contractapi.QueryResultMetadata{Bookmark: bookmark}, nil
	}
	defer iter.Close()
	var steps []*ProcessingStep
	for iter.HasNext() {
		resp, err := iter.Next()
		if err != nil {
			return nil, nil, err
		}
		var step ProcessingStep
		if err := json.Unmarshal(resp.Value, &step); err != nil {
			continue
		}
		steps = append(steps, &step)
	}
	return steps, meta, nil
}
func (c *ProcessingStepContract) GetProcessingStepHistory(ctx contractapi.TransactionContextInterface, key string) ([]*ProcessingStepHistory, error) {
	if err := c.authorizeMSP(ctx, []string{"ProcessorMSP", "ManufacturerMSP", "RegulatorMSP"}); err != nil {
		return nil, err
	}
	iter, err := ctx.GetStub().GetHistoryForKey(key)
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	var history []*ProcessingStepHistory
	for iter.HasNext() {
		resp, err := iter.Next()
		if err != nil {
			return nil, err
		}
		h := &ProcessingStepHistory{
			TxID:      resp.TxId,
			Timestamp: time.Unix(int64(resp.Timestamp.Seconds), int64(resp.Timestamp.Nanos)),
			IsDelete:  resp.Value == nil,
		}
		if !h.IsDelete {
			var step ProcessingStep
			if err := json.Unmarshal(resp.Value, &step); err != nil {
				continue
			}
			h.Event = &step
		}
		history = append(history, h)
	}
	return history, nil
}
