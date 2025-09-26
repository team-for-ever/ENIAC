package contracts

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/hyperledger/fabric-contract-api-go/contractapi"
)

type QRCode struct {
	ResourceType    string    `json:"resourceType"`
	ID              string    `json:"id"`           
	LinkedEventKey  string    `json:"linked_event_key"` 
	LinkedTxID      string    `json:"linked_tx_id"`  
	ProductID       string    `json:"product_id"`    
	BatchSize       float64   `json:"batch_size"`   
	ExpiryDate      time.Time `json:"expiry_date"`   
	Hash            string    `json:"hash"`          
	CustodianMSP    string    `json:"custodian_msp"`  
	CreatedAt       time.Time `json:"created_at"`     
	BlockchainTxID  string    `json:"blockchain_tx_id"`
}

type ProvenanceBundle struct {
	ResourceType     string          `json:"resourceType"`    
	QRCodeID         string          `json:"qr_code_id"`
	CollectionEvent  *CollectionEvent `json:"collection_event,omitempty"`
	QualityTests     []*QualityTest  `json:"quality_tests,omitempty"`
	ProcessingSteps  []*ProcessingStep `json:"processing_steps,omitempty"`
	PlantName        string          `json:"plant_name"`
	BatchSize        float64         `json:"batch_size"`
	ExpiryDate       time.Time       `json:"expiry_date"`
}
type QRCodeContract struct {
	contractapi.Contract
}
func (c *QRCodeContract) authorizeMSP(ctx contractapi.TransactionContextInterface, allowed []string) error {
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
func (c *QRCodeContract) CreateQRCode(
	ctx contractapi.TransactionContextInterface,
	linkedEventKey, productID string,
	batchSize float64,
	expiryDate string,
) (string, error) {
	if err := c.authorizeMSP(ctx, []string{"ManufacturerMSP"}); err != nil {
		return "", err
	}
	if linkedEventKey == "" || productID == "" || expiryDate == "" {
		return "", fmt.Errorf("missing required fields")
	}
	if batchSize <= 0.0 {
		return "", fmt.Errorf("batch_size must be positive")
	}
	expiry, err := time.Parse(time.RFC3339, expiryDate)
	if err != nil {
		return "", fmt.Errorf("invalid expiry date: %v", err)
	}
	linkedBytes, err := ctx.GetStub().GetState(linkedEventKey)
	if err != nil || linkedBytes == nil {
		return "", fmt.Errorf("linked event not found: %s", linkedEventKey)
	}
	var ps ProcessingStep
	var qt QualityTest
	if err := json.Unmarshal(linkedBytes, &ps); err != nil {
		if err := json.Unmarshal(linkedBytes, &qt); err != nil {
			return "", fmt.Errorf("invalid linked event type")
		}
	}
	hashBytes := sha256.Sum256(linkedBytes)
	hash := hex.EncodeToString(hashBytes[:])
	custodianMSP, err := ctx.GetClientIdentity().GetMSPID()
	if err != nil {
		return "", fmt.Errorf("failed to get custodian MSP: %v", err)
	}

	qrID := fmt.Sprintf("QR_%s_%d", ctx.GetStub().GetTxID(), time.Now().UnixNano())
	attrs := []string{qrID}
	key, err := ctx.GetStub().CreateCompositeKey("QRCODE", attrs)
	if err != nil {
		return "", err
	}
	if existing, _ := ctx.GetStub().GetState(key); existing != nil {
		return "", fmt.Errorf("QR already exists: %s", key)
	}
    	var linkedTxID string
	if err := json.Unmarshal(linkedBytes, &ps); err == nil {
		linkedTxID = ps.BlockchainTxID
	} else if err := json.Unmarshal(linkedBytes, &qt); err == nil {
		linkedTxID = qt.BlockchainTxID
	}

	qr := QRCode{
		ResourceType:   "QRCode",
		ID:             qrID,
		LinkedEventKey: linkedEventKey,
		LinkedTxID:     linkedTxID,
		ProductID:      productID,
		BatchSize:      batchSize,
		ExpiryDate:     expiry,
		Hash:           hash,
		CustodianMSP:   custodianMSP,
		CreatedAt:      time.Now().UTC(),
		BlockchainTxID: ctx.GetStub().GetTxID(),
	}

	qrJSON, err := json.Marshal(qr)
	if err != nil {
		return "", err
	}
	if err := ctx.GetStub().PutState(key, qrJSON); err != nil {
		return "", err
	}
	ctx.GetStub().SetEvent("QRCodeCreated", qrJSON)
	return key, nil
}

func (c *QRCodeContract) GetQRCode(ctx contractapi.TransactionContextInterface, qrKey string) (*QRCode, error) {
	bytes, err := ctx.GetStub().GetState(qrKey)
	if err != nil || bytes == nil {
		return nil, fmt.Errorf("not found: %s", qrKey)
	}
	var qr QRCode
	if err := json.Unmarshal(bytes, &qr); err != nil {
		return nil, err
	}
	return &qr, nil
}

func (c *QRCodeContract) GetProvenanceByQR(ctx contractapi.TransactionContextInterface, qrKey string) (*ProvenanceBundle, error) {
	qrBytes, err := ctx.GetStub().GetState(qrKey)
	if err != nil || qrBytes == nil {
		return nil, fmt.Errorf("QR not found: %s", qrKey)
	}
	var qr QRCode
	if err := json.Unmarshal(qrBytes, &qr); err != nil {
		return nil, fmt.Errorf("failed to parse QR: %v", err)
	}

	linkedBytes, err := ctx.GetStub().GetState(qr.LinkedEventKey)
	if err != nil || linkedBytes == nil {
		return nil, fmt.Errorf("linked event tampered or missing")
	}
	hashBytes := sha256.Sum256(linkedBytes)
	currentHash := hex.EncodeToString(hashBytes[:])
	if currentHash != qr.Hash {
		return nil, fmt.Errorf("integrity check failed: hash mismatch")
	}
    	bundle := &ProvenanceBundle{
		ResourceType: "ProvenanceBundle",
		QRCodeID:     qr.ID,
		BatchSize:    qr.BatchSize,
		ExpiryDate:   qr.ExpiryDate,
	}
	var qualityTests []*QualityTest
	var processingSteps []*ProcessingStep
	visited := make(map[string]bool)
	currentKey := qr.LinkedEventKey
	maxSteps := 100 
	stepCount := 0

	for currentKey != "" && stepCount < maxSteps {
		if visited[currentKey] {
			return nil, fmt.Errorf("cycle detected in provenance chain")
		}
		visited[currentKey] = true
		stepCount++

		currentBytes, err := ctx.GetStub().GetState(currentKey)
		if err != nil || currentBytes == nil {
			break 
		}

		var ce CollectionEvent
		if err := json.Unmarshal(currentBytes, &ce); err == nil && ce.ResourceType == "CollectionEvent" {
			bundle.CollectionEvent = &ce
			bundle.PlantName = ce.PlantName
			break 
		}

	
		var ps ProcessingStep
		if err := json.Unmarshal(currentBytes, &ps); err == nil && ps.ResourceType == "ProcessingStep" {
			processingSteps = append([]*ProcessingStep{&ps}, processingSteps...) 
			currentKey = ps.LinkedEventKey
			continue
		}
		var qt QualityTest
		if err := json.Unmarshal(currentBytes, &qt); err == nil && qt.ResourceType == "QualityTest" {
			qualityTests = append([]*QualityTest{&qt}, qualityTests...) 
			currentKey = qt.LinkedEventKey 
			continue
		}
		break
	}

	if stepCount >= maxSteps {
		return nil, fmt.Errorf("provenance chain too deep (possible error)")
	}

	for i, j := 0, len(processingSteps)-1; i < j; i, j = i+1, j-1 {
		processingSteps[i], processingSteps[j] = processingSteps[j], processingSteps[i]
	}

	for i, j := 0, len(qualityTests)-1; i < j; i, j = i+1, j-1 {
		qualityTests[i], qualityTests[j] = qualityTests[j], qualityTests[i]
	}
	sort.Slice(qualityTests, func(i, j int) bool {
		return qualityTests[i].TestDate.Before(qualityTests[j].TestDate)
	})

	bundle.QualityTests = qualityTests
	bundle.ProcessingSteps = processingSteps
	return bundle, nil
}

func (c *QRCodeContract) GetAllQRCodes(
	ctx contractapi.TransactionContextInterface,
	pageSize int,
	bookmark string,
) ([]*QRCode, *contractapi.QueryResultMetadata, error) {
	if err := c.authorizeMSP(ctx, []string{"ManufacturerMSP", "RegulatorMSP"}); err != nil {
		return nil, nil, err
	}
	startKey, _ := ctx.GetStub().CreateCompositeKey("QRCODE", []string{})
	endKey, _ := ctx.GetStub().CreateCompositeKey("QRCODE", []string{string(0xff)})

	iter, meta, err := ctx.GetStub().GetStateByRangeWithPagination(startKey, endKey, int32(pageSize), bookmark)
	if err != nil {
		return nil, nil, err
	}
	defer iter.Close()

	var qrs []*QRCode
	for iter.HasNext() {
		resp, err := iter.Next()
		if err != nil {
			return nil, nil, err
		}
		var qr QRCode
		if err := json.Unmarshal(resp.Value, &qr); err != nil {
			continue
		}
		qrs = append(qrs, &qr)
	}
	return qrs, meta, nil
}
