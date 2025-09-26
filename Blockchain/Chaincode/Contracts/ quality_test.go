package contracts

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
w
	"github.com/hyperledger/fabric-contract-api-go/contractapi"
)

// QualityTest represents a laboratory quality assurance event in FHIR-style
type QualityTest struct {
	ResourceType    string    `json:"resourceType"`   // "QualityTest"
	ID              string    `json:"id"`
	LinkedEventKey  string    `json:"linked_event_key"` // Key to CollectionEvent or ProcessingStep
	LinkedTxID      string    `json:"linked_tx_id"`  // TxID of linked event
	LabID           string    `json:"lab_id"`
	TestDate        time.Time `json:"test_date"`
	PlantName       string    `json:"plant_name"`
	Moisture        float64   `json:"moisture"`       // %
	Pesticide       float64   `json:"pesticide"`      // ppm
	DNAResult       bool      `json:"dna_result"`
	Passed          bool      `json:"passed"`         // Based on rules
	BlockchainTxID  string    `json:"blockchain_tx_id"`
}

// QualityEventHistory for audit trails
type QualityEventHistory struct {
	TxID      string        `json:"tx_id"`
	Timestamp time.Time     `json:"timestamp"`
	IsDelete  bool          `json:"is_delete"`
	Event     *QualityTest  `json:"event,omitempty"`
}

// QualityTestContract manages QualityTest records
type QualityTestContract struct {
	contractapi.Contract
}

// authorizeMSP checks MSP access
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

// queryRangeWithPagination for paginated range queries
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

// GetPlantRules retrieves or defaults plant rules
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

// CreateQualityTest creates test with validation and linkage
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
		return "", fmt.Errorf("missing required fields")
	}
	if moisture < 0.0 || moisture > 100.0 || pesticide < 0.0 {
		return "", fmt.Errorf("invalid metrics")
	}
	t, err := time.Parse(time.RFC3339, testDate)
	if err != nil {
		return "", fmt.Errorf("invalid test date: %v", err)
	}
	if err := c.validateQualityTest(ctx, linkedEventKey, t); err != nil {
		return "", err
	}

	testID := fmt.Sprintf("QUALITY_%s_%d", ctx.GetStub().GetTxID(), time.Now().UnixNano())

	linkedBytes, err := ctx.GetStub().GetState(linkedEventKey)
	if err != nil || linkedBytes == nil {
		return "", fmt.Errorf("linked event not found: %s", linkedEventKey)
	}
	var plantName, linkedTxID string
	var linkedDate time.Time
	var ce CollectionEvent
	if err := json.Unmarshal(linkedBytes, &ce); err == nil && ce.PlantName != "" {
		plantName = ce.PlantName
		linkedTxID = ce.BlockchainTxID
		linkedDate = ce.CollectionDate
	} else {
		var ps ProcessingStep
		if err := json.Unmarshal(linkedBytes, &ps); err == nil && ps.PlantName != "" {
			plantName = ps.PlantName
			linkedTxID = ps.BlockchainTxID
			linkedDate = ps.StepDate
		} else {
			return "", fmt.Errorf("invalid linked event type")
		}
	}

	passed := c.computePassed(ctx, plantName, moisture, pesticide, dnaResult)

	attrs := []string{linkedEventKey, testID}
	key, err := ctx.GetStub().CreateCompositeKey("QUALITYTEST", attrs)
	if err != nil {
		return "", err
	}
	if existing, _ := ctx.GetStub().GetState(key); existing != nil {
		return "", fmt.Errorf("test already exists: %s", key)
	}

	test := QualityTest{
		ResourceType:   "QualityTest",
		ID:             testID,
		LinkedEventKey: linkedEventKey,
		LinkedTxID:     linkedTxID,
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
		return "", err
	}
	if err := ctx.GetStub().PutState(key, testJSON); err != nil {
		return "", err
	}
	ctx.GetStub().SetEvent("QualityTestCreated", testJSON)
	return key, nil
}

// validateQualityTest checks basic rules (existence, chronology)
func (c *QualityTestContract) validateQualityTest(
	ctx contractapi.TransactionContextInterface,
	linkedEventKey string,
	t time.Time,
) error {
	linkedBytes, err := ctx.GetStub().GetState(linkedEventKey)
	if err != nil || linkedBytes == nil {
		return fmt.Errorf("linked event invalid: %v", err)
	}
	var linkedDate time.Time
	var ce CollectionEvent
	if err := json.Unmarshal(linkedBytes, &ce); err == nil {
		linkedDate = ce.CollectionDate
	} else {
		var ps ProcessingStep
		if err := json.Unmarshal(linkedBytes, &ps); err == nil {
			linkedDate = ps.StepDate
		} else {
			return fmt.Errorf("invalid linked event: %v", err)
		}
	}
	if t.Before(linkedDate) {
		return fmt.Errorf("test date before linked date")
	}
	return nil
}

// computePassed calculates passed status (allows failed records)
func (c *QualityTestContract) computePassed(
	ctx contractapi.TransactionContextInterface,
	plantName string,
	moisture, pesticide float64,
	dnaResult bool,
) bool {
	rules, err := c.GetPlantRules(ctx, plantName)
	if err != nil {
		return false
	}
	return moisture <= rules.MaxMoisture && pesticide <= rules.MaxPesticide && dnaResult
}

// GetQualityTest retrieves single test
func (c *QualityTestContract) GetQualityTest(ctx contractapi.TransactionContextInterface, key string) (*QualityTest, error) {
	if err := c.authorizeMSP(ctx, []string{"LabMSP", "ProcessorMSP", "RegulatorMSP"}); err != nil {
		return nil, err
	}
	bytes, err := ctx.GetStub().GetState(key)
	if err != nil || bytes == nil {
		return nil, fmt.Errorf("not found: %s", key)
	}
	var test QualityTest
	if err := json.Unmarshal(bytes, &test); err != nil {
		return nil, err
	}
	return &test, nil
}

// GetAllQualityTests retrieves all with pagination
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

// GetTestsByCollection retrieves tests for event (rich query with fallback)
func (c *QualityTestContract) GetTestsByCollection(
	ctx contractapi.TransactionContextInterface,
	eventKey string,
	pageSize int,
	bookmark string,
) ([]*QualityTest, *contractapi.QueryResultMetadata, error) {
	if err := c.authorizeMSP(ctx, []string{"LabMSP", "ProcessorMSP", "RegulatorMSP"}); err != nil {
		return nil, nil, err
	}
	if eventKey == "" {
		return nil, nil, fmt.Errorf("eventKey required")
	}
	query := fmt.Sprintf(`{"selector":{"linked_event_key":"%s","resourceType":"QualityTest"}}`, eventKey)
	iter, meta, err := ctx.GetStub().GetQueryResultWithPagination(query, int32(pageSize), bookmark)
	if err != nil {
		all, _, ferr := c.GetAllQualityTests(ctx, pageSize, bookmark)
		if ferr != nil {
			return nil, nil, ferr
		}
		var filtered []*QualityTest
		for _, test := range all {
			if test.LinkedEventKey == eventKey {
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

// GetQualityTestsByPlant retrieves by plant (rich query with fallback)
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
