package main

import (
	"log"
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"
)

// New takes a set of options and returns a new kafka instance or an error
func New(opts ...Option) (*Kafka, error) {
	k := Kafka{
		writeSig: make(chan struct{}),
		stream:   make(chan *sarama.ConsumerMessage),
		quitCh:   make(chan struct{}),
	}

	for _, opt := range opts {
		if err := opt.Apply(&k); err != nil {
			return nil, err
		}
	}

	go k.timer()

	return &k, nil
}

// Kafka is a wrapper around a sarama.Consumer
type Kafka struct {
	consumer       sarama.Consumer
	writeSig       chan struct{}
	funcs          map[string]func(*sarama.ConsumerMessage) error
	offsetWriter   OffsetWriter
	stream         chan *sarama.ConsumerMessage
	totalConsumers int64
	quitCh         chan struct{}
}

// Start starts the connections to a kafka instance
func (k *Kafka) Start() error {
	topics, err := k.consumer.Topics()
	if err != nil {
		return err
	}

	for _, topic := range topics {
		if fn, ok := k.funcs[topic]; ok {
			partitions, err := k.consumer.Partitions(topic)
			if err != nil {
				return err
			}

			for _, partition := range partitions {

				offset, err := k.offsetWriter.ReadOffset(topic, partition)
				if err != nil {
					offset = sarama.OffsetNewest
				}

				input, err := k.consumer.ConsumePartition(topic, partition, offset)
				if err != nil {
					return err
				}

				log.Printf("Starting %s - %d @ offset %d", topic, partition, offset)
				k.totalConsumers++
				go func(input sarama.PartitionConsumer, fn func(*sarama.ConsumerMessage) error) {
					var rm *sarama.ConsumerMessage

					for {
						select {
						case <-k.writeSig:
							// log.Printf("::: Attempting Offset Write :::")
							if err := k.offsetWriter.WriteOffset(rm.Topic, rm.Partition, rm.Offset); err != nil {
								log.Printf("Error writing to offest writer: %s", err.Error())
							}
						case message := <-input.Messages():
							rm = message
							if err := fn(message); err != nil {
								log.Printf("Error writing message: %s", err.Error())
							}
						case err := <-input.Errors():
							// TODO: this should be handled better -- but this is just a demo, so we just log the error.
							log.Println(err)
						case <-k.quitCh:
							atomic.AddInt64(&k.totalConsumers, -1)

							if err := input.Close(); err != nil {
								// TODO: handle this later
								// c.errCh <- err
							}
						}
					}
				}(input, fn)
			}
		} else {
			// log.Printf("no func provided for topic: %s", topic)
		}
	}

	return nil
}

// Stop gracefully stops the kafka subscriptions
func (k *Kafka) Stop() error {
	k.quitCh <- struct{}{}

	return nil
}

func (k *Kafka) timer() {

	ticker := time.NewTicker(time.Second * 5)

	for {
		select {
		case <-ticker.C:
			for i := 0; i < int(k.totalConsumers); i++ {
				k.writeSig <- struct{}{}
			}
		case <-k.quitCh:
			ticker.Stop()
			return
		}
	}
}
