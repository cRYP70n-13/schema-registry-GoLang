package main

import (
	"fmt"
	"log"

	"schema-registry/internal/kafka"
	payload_v1 "schema-registry/pkg/payload.v1"
)

const (
	topic             = "protobuf-topic-schema-registry"
	kafkaURL          = "localhost:9092"
	schemaRegistryURL = "http://localhost:8081"
)

func main() {
	producer, err := kafka.NewProducer(kafkaURL, schemaRegistryURL)
	if err != nil {
		log.Fatalf("error with producer: %v", err)
	}
	defer producer.Close()

	testMsg := payload_v1.PayloadMessage{
		Value:       1,
		FirstName:   "Otmane",
		Lastname:    "Kimdil",
		Email:       "otmane.kimdil@gmail.com",
		Description: "A software engineer trying to play with Kafka schema registry",
	}
	offset, err := producer.ProduceMessage(&testMsg, topic)
	if err != nil {
		log.Fatalf("error while producing the test message: %v", err)
	}

	fmt.Println(offset)
}
