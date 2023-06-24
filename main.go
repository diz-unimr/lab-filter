package main

import (
	cKafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	log "github.com/sirupsen/logrus"
	"lab-filter/pkg/config"
	"lab-filter/pkg/fhir"
	"lab-filter/pkg/kafka"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"
)

func main() {
	appConfig := loadConfig()
	configureLogger(appConfig.App)

	// signal handler to break the loop
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// create producer
	producer := kafka.NewProducer(appConfig.Kafka)
	var wg sync.WaitGroup

	for i := 1; i <= appConfig.Kafka.NumConsumers; i++ {

		wg.Add(1)

		go func(clientId string) {

			// create consumer with subscription to input topic
			c := kafka.NewConsumer(appConfig, clientId)
			log.WithFields(log.Fields{
				"group-id":  appConfig.App.Name,
				"client-id": clientId,
			}).
				Info("Consumer created")

			for {
				select {
				case <-sigchan:
					log.WithField("client-id", c.ClientId).Info("Consumer shutting down gracefully")
					syncConsumerCommits(c)
					wg.Done()
					return

				default:
					msg, err := c.Consumer.ReadMessage(1 * time.Second)
					if err == nil {
						log.WithFields(log.Fields{
							"client-id": clientId,
							"key":       string(msg.Key),
							"topic":     *msg.TopicPartition.Topic,
							"offset":    msg.TopicPartition.Offset.String()}).
							Debug("Message received")

						deliveryChan := createListener(sigchan, c, msg)
						processMessages(producer, msg, deliveryChan, sigchan)

					} else {
						if err.(cKafka.Error).Code() != cKafka.ErrTimedOut {
							// The producer will automatically try to recover from all errors.
							log.WithError(err).Error("Consumer error")
						}
					}
				}
			}
		}(strconv.Itoa(i))
	}
	<-sigchan
	close(sigchan)
	wg.Wait()
	log.Info("All consumers stopped. Flushing outstanding producer messages...")

	for producer.Producer.Flush(10000) > 0 {
		log.Debug("Still waiting to flush outstanding messages")
	}
	log.Info("Done")
	producer.Producer.Close()
}

func createListener(sigchan chan os.Signal, c *kafka.LabConsumer, msg *cKafka.Message) chan cKafka.Event {
	listener := make(chan cKafka.Event, 1)

	go func(msg *cKafka.Message) {
		defer close(listener)

		e := <-listener
		switch ev := e.(type) {
		case nil:
			return
		case *cKafka.Error:
			log.WithError(ev).
				Error("Processing failed")
			sigchan <- syscall.SIGINT
		case *cKafka.Message:
			if ev.TopicPartition.Error != nil {
				log.WithError(ev.TopicPartition.Error).
					Error("Delivery failed")
				sigchan <- syscall.SIGINT
			} else {
				log.WithFields(log.Fields{
					"key":    string(ev.Key),
					"offset": ev.TopicPartition.Offset,
					"topic":  *ev.TopicPartition.Topic,
				}).
					Debug("Delivered message")
				c.StoreOffset(msg)
			}
		}
	}(msg)

	return listener
}

func processMessages(producer *kafka.LabProducer, msg *cKafka.Message,
	deliveryChan chan cKafka.Event, sigchan chan os.Signal) {

	filtered := fhir.FilterBundle(msg.Value)
	if filtered == nil {

		deliveryChan <- nil
		return
	}

	producer.SendBundle(msg.Key, msg.Timestamp, filtered, deliveryChan, sigchan)
}

func loadConfig() config.AppConfig {
	c, err := config.LoadConfig(".")
	if err != nil {
		log.WithError(err).Fatal("Unable to load config file")
	}
	return c
}

func syncConsumerCommits(c *kafka.LabConsumer) {
	c.Unsubscribe()
	parts, err := c.Consumer.Commit()
	if err != nil {
		if err.(cKafka.Error).Code() == cKafka.ErrNoOffset {
			return
		}
		log.WithError(err).Error("Failed to commit offsets")
	} else {

		for _, tp := range parts {
			log.WithFields(log.Fields{
				"topic":     *tp.Topic,
				"partition": tp.Partition,
				"offset":    tp.Offset.String()}).
				Info("Stored offsets committed")
		}
	}
	c.Close()
}

func configureLogger(config config.App) {
	log.SetOutput(os.Stdout)
	level, err := log.ParseLevel(config.LogLevel)
	if err != nil {
		level = log.InfoLevel
	}
	log.SetLevel(level)
}
