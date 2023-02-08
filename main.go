package main

import (
	cKafka "github.com/confluentinc/confluent-kafka-go/kafka"
	log "github.com/sirupsen/logrus"
	"lab-filter/pkg/config"
	"lab-filter/pkg/fhir"
	"lab-filter/pkg/kafka"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	appConfig := loadConfig()
	configureLogger(appConfig.App)

	// create consumer and subscribe to input topics
	consumer := kafka.Subscribe(appConfig)

	// create producer
	producer := kafka.NewProducer(appConfig.Kafka)

	// signal handler to break the loop
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	for {
		select {
		case sig := <-sigchan:
			log.WithField("signal", sig).Info("Caught signal. Gracefully terminating...")
			for producer.Producer.Flush(10000) > 0 {
				log.Debug("Still waiting to flush outstanding messages")
			}
			return

		default:
			msg, err := consumer.ReadMessage(-1)
			go processMessages(consumer, producer, msg, err)
		}
	}
}

func processMessages(consumer *cKafka.Consumer, producer *kafka.LabProducer, msg *cKafka.Message, err error) {
	if err == nil {
		log.WithFields(log.Fields{"key": string(msg.Key), "topic": *msg.TopicPartition.Topic, "offset": msg.TopicPartition.Offset.String()}).Debug("Message received")
		filtered := fhir.FilterBundle(msg.Value)
		if filtered == nil {
			commitMessage(consumer, msg)
			return
		}

		<-producer.SendBundle(msg.Key, msg.Timestamp, filtered)
		commitMessage(consumer, msg)

	} else {
		// The producer will automatically try to recover from all errors.
		log.WithError(err).Error("Consumer error")
	}
}

func commitMessage(consumer *cKafka.Consumer, msg *cKafka.Message) {
	_, err := consumer.CommitMessage(msg)
	if err != nil {
		log.WithError(err).Error("Failed to commit offset")
	}
	log.WithFields(log.Fields{
		"offset": msg.TopicPartition.Offset.String(),
		"topic":  *msg.TopicPartition.Topic}).
		Debug("Offset committed")
}

func loadConfig() config.AppConfig {
	c, err := config.LoadConfig(".")
	if err != nil {
		log.WithError(err).Fatal("Unable to load config file")
	}
	return c
}

func configureLogger(config config.App) {
	//log.SetFormatter(&log.JSONFormatter{})
	log.SetOutput(os.Stdout)
	level, err := log.ParseLevel(config.LogLevel)
	if err != nil {
		level = log.InfoLevel
	}
	log.SetLevel(level)
}
