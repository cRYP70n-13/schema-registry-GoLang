package kafka

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry/serde/protobuf"
	"google.golang.org/protobuf/proto"
)

const (
	nullOffset = -1
)

type SchemaRegistryProducer interface {
	ProduceMessage(msg proto.Message, topic string) (int64, error)
	Close()
}

type schemaRegistryProducer struct {
	producer   *kafka.Producer
	serializer serde.Serializer
}

// NewProducer returns a Kafka producer with schema registry
func NewProducer(kafkaURL, srURL string) (SchemaRegistryProducer, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": kafkaURL})
	if err != nil {
		return nil, err
	}

	c, err := schemaregistry.NewClient(schemaregistry.NewConfig(srURL))
	if err != nil {
		return nil, err
	}

	s, err := protobuf.NewSerializer(c, serde.ValueSerde, protobuf.NewSerializerConfig())
	if err != nil {
		return nil, err
	}

	return &schemaRegistryProducer{
		producer:   p,
		serializer: s,
	}, nil
}

// ProduceMessage sends a proto.Message message to a specific topic.
func (p *schemaRegistryProducer) ProduceMessage(msg proto.Message, topic string) (int64, error) {
	kafkaChan := make(chan kafka.Event)
	defer close(kafkaChan)

	payload, err := p.serializer.Serialize(topic, msg)
	if err != nil {
		return nullOffset, err
	}

	if err = p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic},
		Value:          payload,
	}, kafkaChan); err != nil {
		return nullOffset, err
	}

	e := <-kafkaChan
	switch ev := e.(type) {
	case *kafka.Message:
		return int64(ev.TopicPartition.Offset), nil
	case kafka.Error:
		return nullOffset, err
	}

	return nullOffset, nil
}

// Close closes the connection with the schema registry and kafka.
func (p *schemaRegistryProducer) Close() {
	p.serializer.Close()
	p.producer.Close()
}
