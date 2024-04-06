package main

import (
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
	consumer, err := kafka.NewConsumer(kafkaURL, schemaRegistryURL)
	if err != nil {
		log.Fatalf("error with producer: %v", err)
	}
	defer consumer.Close()

    messageType := (&payload_v1.PayloadMessage{}).ProtoReflect().Type()
    if err := consumer.Run(messageType, topic); err != nil {
        log.Fatal(err)
    }
}
