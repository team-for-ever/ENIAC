// chaincode.go - Main Chaincode Entry Point for Multi-Contract Supply Chain
// This file registers all four separate contracts (CollectionEventContract, QualityTestContract,
// ProcessingStepContract, QRCodeContract) into a single chaincode package for Hyperledger Fabric.

package main

import (
	"log"

	"github.com/hyperledger/fabric-contract-api-go/contractapi"

	"supplychain-chaincode/contracts" 
)

func main() {
	chaincode, err := contractapi.NewChaincode(
		&contracts.CollectionEventContract{}, 
		&contracts.QualityTestContract{},     
		&contracts.ProcessingStepContract{},  
		&contracts.QRCodeContract{},        
	)

	if err != nil {
		log.Panicf("Error creating supplychain chaincode: %v", err)
	}

	if err := chaincode.Start(); err != nil {
		log.Panicf("Error starting supplychain chaincode: %v", err)
	}
}
