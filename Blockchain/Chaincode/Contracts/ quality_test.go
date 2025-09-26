package contracts

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/hyperledger/fabric-contract-api-go/contractapi"
)

type QualityTest struct {
	ResourceType    string    `json:"resourceType"`   
	ID              string    `json:"id"`             
	LinkedEventKey  string    `json:"linked_event_key"` 
	LabID           string    `json:"lab_id"`       
	TestDate        time.Time `json:"test_date"`    
	PlantName       string    `json:"plant_name"`    
	Moisture        float64   `json:"moisture"`     
	Pesticide       float64   `json:"pesticide"`     
	DNAResult       bool      `json:"dna_result"`     
	Passed          bool      `json:"passed"`        
	BlockchainTxID  string    `json:"blockchain_tx_id"` 
}

type QualityEventHistory struct {
	TxID      string        `json:"tx_id"`
	Timestamp time.Time     `json:"timestamp"`
	IsDelete  bool          `json:"is_delete"`
	Event     *QualityTest  `json:"event,omitempty"`
}

type QualityTestContract struct {
	contractapi.Contract
}

func (c *QualityTestContract) authorizeMSP(ctx contractapi.TransactionContextInterface, allowed []string) error {
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

func (c *QualityTestContract) queryRangeWithPagination(
	ctx contractapi.TransactionContextInterface,
	startKey, endKey string,
	pageSize int,
	bookmark string,
) ([]*QualityTest, *contractapi.QueryResultMetadata, error) {
	if pageSize <= 0 {
		pageSize = 10
	}
	iter, meta, err := ctx.GetStub().GetStateByRangeWithPagination(startKey, endKey, int32(pageSize), bookmark)
	if err != nil {
		return nil, nil, err
	}
	defer iter.Close()

	var tests []*QualityTest
	for iter.HasNext() {
		resp, err := iter.Next()
		if err != nil {
			return nil, nil, err
		}
		var test QualityTest
		if err := json.Unmarshal(resp.Value, &test); err != nil { 
			continue
		}
		tests = append(tests, &test)
	}
	return tests, meta, nil
}

func (c *QualityTestContract) GetPlantRules(ctx contractapi.TransactionContextInterface, plantName string) (*PlantRules, error) {
	if plantName == "" {
		return nil, fmt.Errorf("plantName required")
	}
	key := fmt.Sprintf("PLANT_RULES_%s", plantName)
	bytes, err := ctx.GetStub().GetState(key)
	if err != nil {
		return nil, err
	}
	if bytes == nil {
		return &PlantRules{
			PlantName:     plantName,
			Seasons:       []int{10, 11, 12, 1, 2, 3}, 
			Zones:         []GeoZone{{26.0, 30.0, 70.0, 78.0}}, 
			MaxQuantity:   100.0,
			MaxMoisture:   12.0, 
			MaxPesticide:  0.05,  
		}, nil
	}
	var rules PlantRules
	if err := json.Unmarshal(bytes, &rules); err != nil {
		return nil, err
	}
	return &rules, nil
}

func (c *QualityTestContract) CreateQualityTest(
	ctx contractapi.TransactionContextInterface,
	linkedEventKey, labID string,
	testDate string,
	moisture, pesticide float64,
	dnaResult bool,
) (string, error) {
	if err := c.authorizeMSP(ctx, []string{"LabMSP"}); err != nil {
		return "", err
	}
	if linkedEventKey == "" || labID == "" || testDate == "" {
		return "", fmt.Errorf("missing required fields: linkedEventKey, labID, testDate")
	}
	if moisture < 0.0 || moisture > 100.0 {
		return "", fmt.Errorf("invalid moisture: %.2f%% (must be 0-100)", moisture)
	}
	if pesticide < 0.0 {
		return "", fmt.Errorf("invalid pesticide: %.4f ppm (must be non-negative)", pesticide)
	}
	t, err := time.Parse(time.RFC3339, testDate)
	if err != nil {
		return "", fmt.Errorf("invalid test date (RFC3339): %v", err)
	}
	if err := c.validateQualityTest(ctx, linkedEventKey, t, moisture, pesticide, dnaResult); err != nil {
		return "", err
	}

	testID := fmt.Sprintf("QUALITY_%s_%d", ctx.GetStub().GetTxID(), time.Now().UnixNano())

	linkedBytes, err := ctx.GetStub().GetState(linkedEventKey)
	if err != nil || linkedBytes == nil {
		return "", fmt.Errorf("linked event not found or invalid: %s", linkedEventKey)
	}
	var ce CollectionEvent
	if err := json.Unmarshal(linkedBytes, &ce); err != nil {
		return "", fmt.Errorf("failed to parse linked event: %v", err)
	}
	plantName := ce.PlantName

	attrs := []string{linkedEventKey}
	key, err := ctx.GetStub().CreateCompositeKey("QUALITYTEST", attrs)
	if err != nil {
		return "", err
	}
	if existing, _ := ctx.GetStub().GetState(key); existing != nil {
		return "", fmt.Errorf("quality test already exists for linked event: %s", linkedEventKey)
	}

	passed := true 

	test := QualityTest{
		ResourceType:   "QualityTest",
		ID:             testID,
		LinkedEventKey: linkedEventKey,
		LabID:          labID,
		TestDate:       t,
		PlantName:      plantName,
		Moisture:       moisture,
		Pesticide:      pesticide,
		DNAResult:      dnaResult,
		Passed:         passed,
		BlockchainTxID: ctx.GetStub().GetTxID(),
	}

	testJSON, err := json.Marshal(test)
	if err != nil {
		return "", fmt.Errorf("failed to marshal quality test: %v", err)
	}
	if err := ctx.GetStub().PutState(key, testJSON); err != nil {
		return "", fmt.Errorf("failed to put quality test in world state: %v", err)
	}

	ctx.GetStub().SetEvent("QualityTestCreated", testJSON) 
	return key, nil
}

func (c *QualityTestContract) validateQualityTest(
	ctx contractapi.TransactionContextInterface,
	linkedEventKey string,
	t time.Time,
	moisture, pesticide float64,
	dnaResult bool,
) error {
	linkedBytes, err := ctx.GetStub().GetState(linkedEventKey)
	if err != nil {
		return fmt.Errorf("failed to fetch linked event: %v", err)
	}
	if linkedBytes == nil {
		return fmt.Errorf("linked event not found: %s", linkedEventKey)
	}
	var ce CollectionEvent
	if err := json.Unmarshal(linkedBytes, &ce); err != nil {
		return fmt.Errorf("invalid linked event data: %v", err)
	}
	if t.Before(ce.CollectionDate) {
		return fmt.Errorf("test date %s before collection date %s", t.Format(time.RFC3339), ce.CollectionDate.Format(time.RFC3339))
	}

	rules, err := c.GetPlantRules(ctx, ce.PlantName)
	if err != nil {
		return fmt.Errorf("failed to load rules for %s: %v", ce.PlantName, err)
	}
	if moisture > rules.MaxMoisture {
		return fmt.Errorf("%s moisture violation: max %.2f%%, measured %.2f%%", ce.PlantName, rules.MaxMoisture, moisture)
	}
	if pesticide > rules.MaxPesticide {
		return fmt.Errorf("%s pesticide violation: max %.4f ppm, measured %.4f ppm", ce.PlantName, rules.MaxPesticide, pesticide)
	}
	if !dnaResult {
		return fmt.Errorf("%s DNA barcoding failed: species authentication required", ce.PlantName)
	}
	return nil
}

func (c *QualityTestContract) GetQualityTest(ctx contractapi.TransactionContextInterface, key string) (*QualityTest, error) {
	if err := c.authorizeMSP(ctx, []string{"LabMSP", "ProcessorMSP", "RegulatorMSP"}); err != nil {
		return nil, err
	}
	bytes, err := ctx.GetStub().GetState(key)
	if err != nil {
		return nil, fmt.Errorf("failed to read quality test from world state: %v", err)
	}
	if bytes == nil {
		return nil, fmt.Errorf("quality test not found: %s", key)
	}
	var test QualityTest
	if err := json.Unmarshal(bytes, &test); err != nil {
		return nil, fmt.Errorf("failed to unmarshal quality test JSON: %v", err)
	}
	return &test, nil
}

func (c *QualityTestContract) GetAllQualityTests(
	ctx contractapi.TransactionContextInterface,
	pageSize int,
	bookmark string,
) ([]*QualityTest, *contractapi.QueryResultMetadata, error) {
	if err := c.authorizeMSP(ctx, []string{"LabMSP", "ProcessorMSP", "RegulatorMSP"}); err != nil {
		return nil, nil, err
	}
	startKey, _ := ctx.GetStub().CreateCompositeKey("QUALITYTEST", []string{})
	endKey, _ := ctx.GetStub().CreateCompositeKey("QUALITYTEST", []string{string(0xff)})
	return c.queryRangeWithPagination(ctx, startKey, endKey, pageSize, bookmark)
}

func (c *QualityTestContract) GetTestsByCollection(
	ctx contractapi.TransactionContextInterface,
	collectionKey string, 
	pageSize int,
	bookmark string,
) ([]*QualityTest, *contractapi.QueryResultMetadata, error) {
	if err := c.authorizeMSP(ctx, []string{"LabMSP", "ProcessorMSP", "RegulatorMSP"}); err != nil {
		return nil, nil, err
	}
	if collectionKey == "" {
		return nil, nil, fmt.Errorf("collectionKey required")
	}

	query := fmt.Sprintf(`{"selector":{"linked_event_key":"%s","resourceType":"QualityTest"}}`, collectionKey)
	iter, meta, err := ctx.GetStub().GetQueryResultWithPagination(query, int32(pageSize), bookmark)
	if err != nil {
		fmt.Printf("Rich query failed (%v); fallback to scan", err)
		all, _, ferr := c.GetAllQualityTests(ctx, pageSize, bookmark)
		if ferr != nil {
			return nil, nil, ferr
		}
		var filtered []*QualityTest
		for _, test := range all {
			if test.LinkedEventKey == collectionKey {
				filtered = append(filtered, test)
			}
		}
		return filtered, &contractapi.QueryResultMetadata{Bookmark: bookmark}, nil
	}
	defer iter.Close()

	var tests []*QualityTest
	for iter.HasNext() {
		resp, err := iter.Next()
		if err != nil {
			return nil, nil, err
		}
		var test QualityTest
		if err := json.Unmarshal(resp.Value, &test); err != nil {
			continue 
		}
		tests = append(tests, &test)
	}
	return tests, meta, nil
}

func (c *QualityTestContract) GetQualityTestsByPlant(
	ctx contractapi.TransactionContextInterface,
	plantName string,
	pageSize int,
	bookmark string,
) ([]*QualityTest, *contractapi.QueryResultMetadata, error) {
	if err := c.authorizeMSP(ctx, []string{"LabMSP", "ProcessorMSP", "RegulatorMSP"}); err != nil {
		return nil, nil, err
	}
	if plantName == "" {
		return nil, nil, fmt.Errorf("plantName required")
	}
	query := fmt.Sprintf(`{"selector":{"plant_name":"%s","resourceType":"QualityTest"}}`, plantName)
	iter, meta, err := ctx.GetStub().GetQueryResultWithPagination(query, int32(pageSize), bookmark)
	if err != nil {
		fmt.Printf("Rich query failed (%v); fallback to scan", err)
		all, _, ferr := c.GetAllQualityTests(ctx, pageSize, bookmark)
		if ferr != nil {
			return nil, nil, ferr
		}
		var filtered []*QualityTest
		for _, test := range all {
			if strings.EqualFold(test.PlantName, plantName) {
				filtered = append(filtered, test)
			}
		}
		return filtered, &contractapi.QueryResultMetadata{Bookmark: bookmark}, nil
	}
	defer iter.Close()
	var tests []*QualityTest
	for iter.HasNext() {
		resp, err := iter.Next()
		if err != nil {
			return nil, nil, err
		}
		var test QualityTest
		if err := json.Unmarshal(resp.Value, &test); err != nil {
			continue
		}
		tests = append(tests, &test)
	}
	return tests, meta, nil
}

func (c *QualityTestContract) GetQualityTestsByLab(
	ctx contractapi.TransactionContextInterface,
	labID string,
	pageSize int,
	bookmark string,
) ([]*QualityTest, *contractapi.QueryResultMetadata, error) {
	if err := c.authorizeMSP(ctx, []string{"LabMSP", "ProcessorMSP", "RegulatorMSP"}); err != nil {
		return nil, nil, err
	}
	if labID == "" {
		return nil, nil, fmt.Errorf("labID required")
	}
	query := fmt.Sprintf(`{"selector":{"lab_id":"%s","resourceType":"QualityTest"}}`, labID)
	iter, meta, err := ctx.GetStub().GetQueryResultWithPagination(query, int32(pageSize), bookmark)
	if err != nil {
		fmt.Printf("Rich query failed (%v); fallback to scan", err)
		all, _, ferr := c.GetAllQualityTests(ctx, pageSize, bookmark)
		if ferr != nil {
			return nil, nil, ferr
		}
		var filtered []*QualityTest
		for _, test := range all {
			if strings.EqualFold(test.LabID, labID) {
				filtered = append(filtered, test)
			}
		}
		return filtered, &contractapi.QueryResultMetadata{Bookmark: bookmark}, nil
	}
	defer iter.Close()
	var tests []*QualityTest
	for iter.HasNext() {
		resp, err := iter.Next()
		if err != nil {
			return nil, nil, err
		}
		var test QualityTest
		if err := json.Unmarshal(resp.Value, &test); err != nil {
			continue
		}
		tests = append(tests, &test)
	}
	return tests, meta, nil
}

func (c *QualityTestContract) GetQualityTestHistory(ctx contractapi.TransactionContextInterface, key string) ([]*QualityEventHistory, error) {
	if err := c.authorizeMSP(ctx, []string{"LabMSP", "ProcessorMSP", "RegulatorMSP"}); err != nil {
		return nil, err
	}
	iter, err := ctx.GetStub().GetHistoryForKey(key)
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	var history []*QualityEventHistory
	for iter.HasNext() {
		resp, err := iter.Next()
		if err != nil {
			return nil, err
		}
		h := &QualityEventHistory{
			TxID:      resp.TxId,
			Timestamp: time.Unix(int64(resp.Timestamp.Seconds), int64(resp.Timestamp.Nanos)),
			IsDelete:  resp.Value == nil,
		}
		if !h.IsDelete {
			var test QualityTest
			if err := json.Unmarshal(resp.Value, &test); err != nil {
				continue
			}
			h.Event = &test
		}
		history = append(history, h)
	}
	return history, nil
}
