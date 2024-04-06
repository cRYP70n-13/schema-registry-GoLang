package kafka

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry/serde/protobuf"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const (
    consumerGroupID = "test-consumer"
    defaultSessionTimeout = 6000
    noTimeout = -1
)

// SchemaRegistryConsumer defines the methods for SchemaRegistry consumer
type SchemaRegistryConsumer interface {
    Run(messageType protoreflect.MessageType, topic string) error
    Close()
}

type schemaRegistryConsumer struct {
    consumer *kafka.Consumer
    deserializer *protobuf.Deserializer
}

// NewConsumer returns new schemaRegistryConsumer for Kafka and SR
func NewConsumer(kafkaURL, schemaRegistryURL string) (SchemaRegistryConsumer, error) {
    c, err := kafka.NewConsumer(&kafka.ConfigMap{
        "bootstrap.servers": kafkaURL,
        "group.id": consumerGroupID,
        "session.timeout.ms": defaultSessionTimeout,
        "enable.auto.commit": false,
    })
    if err != nil {
        return nil, err
    }

    sr, err := schemaregistry.NewClient(schemaregistry.NewConfig(schemaRegistryURL))
    if err != nil {
        return nil, err
    }

    d, err := protobuf.NewDeserializer(sr, serde.ValueSerde, protobuf.NewDeserializerConfig())
    if err != nil {
        return nil, err
    }

    return &schemaRegistryConsumer{
        consumer: c,
        deserializer: d,
    }, nil
}

// Run starts the consumption process
func (c *schemaRegistryConsumer) Run(messageType protoreflect.MessageType, topic string) error {
    if err := c.consumer.SubscribeTopics([]string{topic}, nil); err != nil {
        return err
    }
    if err := c.deserializer.ProtoRegistry.RegisterMessage(messageType); err != nil {
        return err
    }

    for {
        kafkaMsg, err := c.consumer.ReadMessage(noTimeout)
        if err != nil {
            return err
        }

        msg, err := c.deserializer.Deserialize(topic, kafkaMsg.Value)
        if err != nil {
            return err
        }

        c.handleMessage(msg, int64(kafkaMsg.TopicPartition.Offset))
        if _, err := c.consumer.CommitMessage(kafkaMsg); err != nil {
            return err
        }
    }
}

func (c *schemaRegistryConsumer) handleMessage(message any, offset int64) {
    fmt.Printf("message %v with offset %d\n", message, offset)
}

// Close closes the consumer along with the deserializer
func (c *schemaRegistryConsumer) Close() {
    c.deserializer.Close()
    c.consumer.Close()
}
