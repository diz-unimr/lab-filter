package kafka

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	log "github.com/sirupsen/logrus"
	"lab-filter/pkg/config"
	"os"
)

type LabConsumer struct {
	Consumer *kafka.Consumer
	Topic    string
	ClientId string
	IsClosed bool
}

func NewConsumer(config config.AppConfig, clientId string) *LabConsumer {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":        config.Kafka.BootstrapServers,
		"security.protocol":        config.Kafka.SecurityProtocol,
		"ssl.ca.location":          config.Kafka.Ssl.CaLocation,
		"ssl.key.location":         config.Kafka.Ssl.KeyLocation,
		"ssl.certificate.location": config.Kafka.Ssl.CertificateLocation,
		"ssl.key.password":         config.Kafka.Ssl.KeyPassword,
		"broker.address.family":    "v4",
		"group.id":                 config.App.Name,
		"client.id":                clientId,
		"enable.auto.commit":       true,
		"enable.auto.offset.store": false,
		"auto.commit.interval.ms":  5000,
		"auto.offset.reset":        "earliest",
	})

	if err != nil {
		panic(err)
	}

	err = c.SubscribeTopics([]string{config.Kafka.InputTopic}, nil)
	check(err)

	return &LabConsumer{
		Consumer: c,
		Topic:    config.Kafka.InputTopic,
		ClientId: clientId,
	}
}

func (c *LabConsumer) Close() {
	err := c.Consumer.Close()
	if err != nil {
		log.Error("Failed to close consumer properly")
	}
	c.IsClosed = true
}

func (c *LabConsumer) Unsubscribe() {
	err := c.Consumer.Unsubscribe()
	if err != nil {
		log.Error("Failed to unsubscribe consumer from the current subscription")
	}
}

func (c *LabConsumer) StoreOffset(msg *kafka.Message) {
	if c.IsClosed {
		return
	}
	_, err := c.Consumer.StoreMessage(msg)
	if err != nil {
		log.WithFields(log.Fields{
			"key":    string(msg.Key),
			"topic":  *msg.TopicPartition.Topic,
			"offset": msg.TopicPartition.Offset.String()}).
			Warn("Failed to commit offset for message")
	} else {
		log.WithFields(log.Fields{
			"key":    string(msg.Key),
			"topic":  *msg.TopicPartition.Topic,
			"offset": msg.TopicPartition.Offset.String()}).
			Trace("Offset for message committed")
	}
}

func check(err error) {
	if err == nil {
		return
	}

	log.WithError(err).Error("Terminating")
	os.Exit(1)
}
