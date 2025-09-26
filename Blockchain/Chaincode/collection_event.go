package contracts

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/hyperledger/fabric-contract-api-go/contractapi"
)
type GeoZone struct {
	LatMin float64 `json:"lat_min"`
	LatMax float64 `json:"lat_max"`
	LonMin float64 `json:"lon_min"`
	LonMax float64 `json:"lon_max"`
}

type PlantRules struct {
	PlantName     string    `json:"plant_name"`
	Seasons       []int     `json:"seasons"`
	Zones         []GeoZone `json:"zones"`
	MaxQuantity   float64   `json:"max_quantity"`
 	MaxMoisture   float64   `json:"max_moisture"`  
	MaxPesticide  float64   `json:"max_pesticide"` 
}

type EventHistory struct {
	TxID      string          `json:"tx_id"`
	Timestamp time.Time       `json:"timestamp"`
	IsDelete  bool            `json:"is_delete"`
	Event     *CollectionEvent `json:"event,omitempty"`
}

type CollectionEvent struct {
	ResourceType   string    `json:"resourceType"`   
	ID             string    `json:"id"`             
	FarmerID       string    `json:"farmer_id"`
	Name           string    `json:"name"`
	Latitude       float64   `json:"latitude"`
	Longitude      float64   `json:"longitude"`
	CollectionDate time.Time `json:"collection_date"`
	PlantName      string    `json:"plant_name"`
	Quantity       float64   `json:"quantity"`    
	BlockchainTxID string    `json:"blockchain_tx_id"`
}
type CollectionEventContract struct {
	contractapi.Contract
}

func (c *CollectionEventContract) authorizeMSP(ctx contractapi.TransactionContextInterface, allowed []string) error {
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

func (c *CollectionEventContract) queryRangeWithPagination(
	ctx contractapi.TransactionContextInterface,
	startKey, endKey string,
	pageSize int,
	bookmark string,
) ([]*CollectionEvent, *contractapi.QueryResultMetadata, error) {
	if pageSize <= 0 {
		pageSize = 10
	}
	iter, meta, err := ctx.GetStub().GetStateByRangeWithPagination(startKey, endKey, int32(pageSize), bookmark)
	if err != nil {
		return nil, nil, err
	}
	defer iter.Close()

	var events []*CollectionEvent
	for iter.HasNext() {
		resp, err := iter.Next()
		if err != nil {
			return nil, nil, err
		}
		var event CollectionEvent
		if err := json.Unmarshal(resp.Value, &event); err != nil { //Invalid JSON skip karo
			continue 
		}
		events = append(events, &event)
	}
	return events, meta, nil
}

func (c *CollectionEventContract) InitPlantRules(ctx contractapi.TransactionContextInterface, rulesJSON string) error {
	if err := c.authorizeMSP(ctx, []string{"AdminMSP", "RegulatorMSP"}); err != nil {
		return err
	}
	var rules PlantRules
	if err := json.Unmarshal([]byte(rulesJSON), &rules); err != nil || rules.PlantName == "" {
		return fmt.Errorf("invalid rules JSON or missing plant_name")
	}
	rulesBytes, _ := json.Marshal(rules)
	key := fmt.Sprintf("PLANT_RULES_%s", rules.PlantName)
	return ctx.GetStub().PutState(key, rulesBytes)
}
func (c *CollectionEventContract) GetPlantRules(ctx contractapi.TransactionContextInterface, plantName string) (*PlantRules, error) {
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
			PlantName:   plantName,
			Seasons:     []int{10, 11, 12, 1, 2, 3}, /
			Zones:       []GeoZone{{26.0, 30.0, 70.0, 78.0}},
			MaxQuantity: 100.0,
		}, nil
	}
	var rules PlantRules
	if err := json.Unmarshal(bytes, &rules); err != nil {
		return nil, err
	}
	return &rules, nil
}

func (c *CollectionEventContract) CreateCollectionEvent(
	ctx contractapi.TransactionContextInterface,
	farmerID, name string,
	latitude, longitude float64,
	collectionDate, plantName string,
	quantity float64,
) (string, error) {
	if err := c.authorizeMSP(ctx, []string{"FarmerMSP"}); err != nil {
		return "", err
	}
	if farmerID == "" || name == "" || plantName == "" {
		return "", fmt.Errorf("missing required fields")
	}
	if latitude < -90.0 || latitude > 90.0 || longitude < -180.0 || longitude > 180.0 {
		return "", fmt.Errorf("invalid coordinates: lat=%.2f, lon=%.2f", latitude, longitude)
	}
	if quantity <= 0.0 {
		return "", fmt.Errorf("quantity must be positive")
	}
	t, err := time.Parse(time.RFC3339, collectionDate)
	if err != nil {
		return "", fmt.Errorf("invalid date (RFC3339): %v", err)
	}
	if err := c.validateCollectionEvent(ctx, t, latitude, longitude, plantName, quantity); err != nil {
		return "", err
	}
	eventID := fmt.Sprintf("COLLECTION_%s_%d", ctx.GetStub().GetTxID(), time.Now().UnixNano())
	attrs := []string{farmerID, plantName, eventID}
	key, err := ctx.GetStub().CreateCompositeKey("COLLECTIONEVENT", attrs)
	if err != nil {
		return "", err
	}
	if existing, _ := ctx.GetStub().GetState(key); existing != nil {
		return "", fmt.Errorf("event exists: %s", key)
	}
	event := CollectionEvent{
		ResourceType:   "CollectionEvent",
		ID:             eventID,
		FarmerID:       farmerID,
		Name:           name,
		Latitude:       latitude,
		Longitude:      longitude,
		CollectionDate: t,
		PlantName:      plantName,
		Quantity:       quantity,
		BlockchainTxID: ctx.GetStub().GetTxID(),
	}
	eventJSON, err := json.Marshal(event)
	if err != nil {
		return "", err
	}
	if err := ctx.GetStub().PutState(key, eventJSON); err != nil {
		return "", err
	}
	ctx.GetStub().SetEvent("CollectionEventCreated", eventJSON)
	return key, nil
}

func (c *CollectionEventContract) validateCollectionEvent(
	ctx contractapi.TransactionContextInterface,
	t time.Time,
	latitude, longitude float64,
	plantName string,
	quantity float64,
) error {
	rules, err := c.GetPlantRules(ctx, plantName)
	if err != nil {
		return fmt.Errorf("load rules failed for %s: %v", plantName, err)
	}
	month := int(t.Month())
	isSeasonAllowed := false
	for _, m := range rules.Seasons {
		if month == m {
			isSeasonAllowed = true
			break
		}
	}
	if !isSeasonAllowed {
		return fmt.Errorf("%s seasonal violation: allowed %v, current %d", plantName, rules.Seasons, month)
	}
	inZone := false
	for _, zone := range rules.Zones {
		if latitude >= zone.LatMin && latitude <= zone.LatMax && longitude >= zone.LonMin && longitude <= zone.LonMax {
			inZone = true
			break
		}
	}
	if !inZone {
		return fmt.Errorf("%s geo violation: (%.2f, %.2f) outside zones", plantName, latitude, longitude)
	}
	if quantity > rules.MaxQuantity {
		return fmt.Errorf("%s limit exceeded: max %.2fkg, got %.2fkg", plantName, rules.MaxQuantity, quantity)
	}
	return nil
}

func (c *CollectionEventContract) GetCollectionEvent(ctx contractapi.TransactionContextInterface, key string) (*CollectionEvent, error) {
	if err := c.authorizeMSP(ctx, []string{"FarmerMSP", "ProcessorMSP", "LabMSP", "RegulatorMSP"}); err != nil {
		return nil, err
	}
	bytes, err := ctx.GetStub().GetState(key)
	if err != nil {
		return nil, err
	}
	if bytes == nil {
		return nil, fmt.Errorf("not found: %s", key)
	}
	var event CollectionEvent
	if err := json.Unmarshal(bytes, &event); err != nil {
		return nil, err
	}
	return &event, nil
}

func (c *CollectionEventContract) GetAllCollectionEvents(
	ctx contractapi.TransactionContextInterface,
	pageSize int,
	bookmark string,
) ([]*CollectionEvent, *contractapi.QueryResultMetadata, error) {
	if err := c.authorizeMSP(ctx, []string{"FarmerMSP", "ProcessorMSP", "LabMSP", "RegulatorMSP"}); err != nil {
		return nil, nil, err
	}
	startKey, _ := ctx.GetStub().CreateCompositeKey("COLLECTIONEVENT", []string{})
	endKey, _ := ctx.GetStub().CreateCompositeKey("COLLECTIONEVENT", []string{string(0xff)})
	return c.queryRangeWithPagination(ctx, startKey, endKey, pageSize, bookmark)
}

func (c *CollectionEventContract) GetEventsByFarmer(
	ctx contractapi.TransactionContextInterface,
	farmerID string,
	pageSize int,
	bookmark string,
) ([]*CollectionEvent, *contractapi.QueryResultMetadata, error) {
	if err := c.authorizeMSP(ctx, []string{"FarmerMSP", "ProcessorMSP", "LabMSP", "RegulatorMSP"}); err != nil {
		return nil, nil, err
	}
	if farmerID == "" {
		return nil, nil, fmt.Errorf("farmerID required")
	}
	startKey, _ := ctx.GetStub().CreateCompositeKey("COLLECTIONEVENT", []string{farmerID})
	endKey, _ := ctx.GetStub().CreateCompositeKey("COLLECTIONEVENT", []string{farmerID + string(0xff)})
	return c.queryRangeWithPagination(ctx, startKey, endKey, pageSize, bookmark)
}

func (c *CollectionEventContract) GetEventsByPlant(
	ctx contractapi.TransactionContextInterface,
	plantName string,
	pageSize int,
	bookmark string,
) ([]*CollectionEvent, *contractapi.QueryResultMetadata, error) {
	if err := c.authorizeMSP(ctx, []string{"FarmerMSP", "ProcessorMSP", "LabMSP", "RegulatorMSP"}); err != nil {
		return nil, nil, err
	}
	if plantName == "" {
		return nil, nil, fmt.Errorf("plantName required")
	}
	query := fmt.Sprintf(`{"selector":{"plant_name":"%s","resourceType":"CollectionEvent"}}`, plantName)
	iter, meta, err := ctx.GetStub().GetQueryResultWithPagination(query, int32(pageSize), bookmark)
	if err != nil {
		fmt.Printf("Rich query failed (%v); fallback to scan", err)
		all, _, ferr := c.GetAllCollectionEvents(ctx, pageSize, bookmark)
		if ferr != nil {
			return nil, nil, ferr
		}
		var filtered []*CollectionEvent
		for _, e := range all {
			if strings.EqualFold(e.PlantName, plantName) {
				filtered = append(filtered, e)
			}
		}
		return filtered, &contractapi.QueryResultMetadata{Bookmark: bookmark}, nil
	}
	defer iter.Close()
	var events []*CollectionEvent
	for iter.HasNext() {
		resp, err := iter.Next()
		if err != nil {
			return nil, nil, err
		}
		var event CollectionEvent
		if err := json.Unmarshal(resp.Value, &event); err != nil {
			continue
		}
		events = append(events, &event)
	}
	return events, meta, nil
}

func (c *CollectionEventContract) GetEventHistory(ctx contractapi.TransactionContextInterface, key string) ([]*EventHistory, error) {
	if err := c.authorizeMSP(ctx, []string{"FarmerMSP", "ProcessorMSP", "LabMSP", "RegulatorMSP"}); err != nil {
		return nil, err
	}
	iter, err := ctx.GetStub().GetHistoryForKey(key)
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	var history []*EventHistory
	for iter.HasNext() {
		resp, err := iter.Next()
		if err != nil {
			return nil, err
		}
		h := &EventHistory{TxID: resp.TxId, Timestamp: time.Unix(int64(resp.Timestamp.Seconds), int64(resp.Timestamp.Nanos)), IsDelete: resp.Value == nil}
		if !h.IsDelete {
			var event CollectionEvent
			if err := json.Unmarshal(resp.Value, &event); err != nil {
				continue
			}
			h.Event = &event
		}
		history = append(history, h)
	}
	return history, nil
}
