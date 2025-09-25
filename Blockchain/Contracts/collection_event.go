package contracts

import (
    "encoding/json"
    "fmt"
    "time"

    "github.com/hyperledger/fabric-contract-api-go/contractapi"
)

// CollectionEvent blockchain pe  geotagged collection event dikhayega
type CollectionEvent struct {
    FarmerID      string    `json:"farmer_id"`
    Name        string    `json:"name"`
    Latitude     float64   `json:"latitude"`
    Longitude    float64   `json:"longitude"`
    CollectionDate     time.Time `json:"collectiondate"`
    PlantName   string    `json:"plantname"`
    BlockchainTx string    `json:"blockchain_tx"`
}
type CollectionEventContract struct {
    contractapi.Contract
}

// CreateCollectionEvent naye collection event banayega  blockchain pe 
func( c*CollectionEventContract) CreateCollectionEvent(ctx contractapi.TransactionContextInterface, farmerID string, name string, latitude float64, longitude float64, collectionDate string, plantName string) (string, error {
    // Parse the collecition date 
    t, err := time.Parse(time.RFC3339, collectionDate)
    if err != nil {
        return "", fmt.Errorf("invalid collection date: %v", err)
    }

    // naya event struct bana rahe hai yaha 
    event:=CollectionEvent{
        FarmerID:     farmerID,
        Name:         name,
        Latitude:     latitude,
        Longitude:    longitude,
        CollectionDate:    t,
        PlantName:    plantName,
        BlockchainTx: ctx.GetStub().GetTxID(),
    }

    eventJSON,err := json.Marshal(event)
    if err != nil {
        return "", fmt.Errorf("failed to marshal event: %v", err)  //Code ko yaha pe dekhna hai
    }
key :=fmt.Sprintf("COLLECTIONEVENT_%s_%s", farmerID, ctx.GetStub().GetTxID())
err = ctx.GetStub().PutState(key, eventJSON)
if err != nil {
    return "", fmt.Errorf("failed to put event in world state: %v", err)
}
return key, nil
}

// GetCollectionEvent ek collection event ko retreive karega blockchain se
func( c*CollectionEventContract) GetCollectionEvent(ctx contractapi.TransactionContextInterface, key string) (*CollectionEvent, error) {
    eventJSON, err := ctx.GetStub().GetState(key)
    if err != nil {
        return nil, fmt.Errorf("failed to read event from world state: %v", err)
    }
    if eventJSON == nil {
        return nil, fmt.Errorf("event not found: %s", key)
    }

    var event CollectionEvent
    err = json.Unmarshal(eventJSON, &event)
    if err != nil {
        return nil, fmt.Errorf("failed to unmarshal event JSON: %v", err)
    }

    return &event, nil
}

// GetAllCollectionEvents saare collection events ko retreive karega blockchain se
func( c*CollectionEventContract) GetAllCollectionEvents(ctx contractapi.TransactionContextInterface) ([]*CollectionEvent, error) {
    resultsIterator, err := ctx.GetStub().GetStateByRange("COLLECTIONEVENT_", "COLLECTIONEVENT_~")
    if err != nil {
        return nil, fmt.Errorf("failed to get events from world state: %v", err)
    }
    defer resultsIterator.Close()

    var events []*CollectionEvent
    for resultsIterator.HasNext() {
        queryResponse, err := resultsIterator.Next()
        if err != nil {
            return nil, fmt.Errorf("failed to iterate over events: %v",err)   
        }
        var event CollectionEvent
        err = json.Unmarshal(queryResponse.Value, &event)
        if err != nil {
            return nil, fmt.Errorf("failed to unmarshal event JSON: %v", err)
        }
        events = append(events, &event)
    }   
    return events,nil
}