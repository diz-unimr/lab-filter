package kafka

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/samply/golang-fhir-models/fhir-models/fhir"
	log "github.com/sirupsen/logrus"
	"lab-filter/pkg/config"
	"os"
	"time"
)

type LabProducer struct {
	Producer *kafka.Producer
	Topic    string
}

func NewProducer(config config.Kafka) *LabProducer {

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":        config.BootstrapServers,
		"security.protocol":        config.SecurityProtocol,
		"ssl.ca.location":          config.Ssl.CaLocation,
		"ssl.key.location":         config.Ssl.KeyLocation,
		"ssl.certificate.location": config.Ssl.CertificateLocation,
		"ssl.key.password":         config.Ssl.KeyPassword,
	})
	if err != nil {
		log.WithError(err).Error("Failed to create Kafka producer. Terminating")
		os.Exit(1)
	}

	// produced message handler
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					log.WithError(ev.TopicPartition.Error).
						Error("Delivery failed")
				} else {
					log.WithFields(log.Fields{
						"key":    string(ev.Key),
						"offset": ev.TopicPartition.Offset,
						"topic":  *ev.TopicPartition.Topic,
					}).
						Debug("Delivered message")
				}
			}
		}
	}()

	return &LabProducer{
		Producer: p,
		Topic:    config.OutputTopic,
	}
}

func (p *LabProducer) SendBundle(key []byte, bundle *fhir.Bundle) (deliveryChan chan kafka.Event) {
	if bundle != nil {
		byteVal, err := bundle.MarshalJSON()
		if err != nil {
			fmt.Println(err)
			return
		}
		return p.Send(key, byteVal)
	}

	return nil
}

func (p *LabProducer) Send(key []byte, msg []byte) (deliveryChan chan kafka.Event) {

	delChan := make(chan kafka.Event, 10000)

	go func() {
		err := p.Producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &p.Topic, Partition: kafka.PartitionAny},
			Key:            key,
			Value:          msg,
		}, delChan)
		if err != nil {
			if err.(kafka.Error).Code() == kafka.ErrQueueFull {
				// Producer queue is full, wait 1s for messages
				// to be delivered then try again.
				close(delChan)
				time.Sleep(time.Second)
				p.Send(key, msg)
			}
		}
	}()

	return delChan
}
