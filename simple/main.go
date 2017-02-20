package main

import (
	"log"

	"github.com/Shopify/sarama"
)

func main() {
	// Setup Functions
	funcs := make(map[string]func(*sarama.ConsumerMessage) error)

	funcs["experiment.goal"] = func(msg *sarama.ConsumerMessage) error {
		log.Printf("Got an experiment:goal event!")

		return nil
	}

	funcs["experiment.participation"] = func(msg *sarama.ConsumerMessage) error {
		log.Printf("Got an experiment:participation event!")

		return nil
	}

	// Setup Kafka
	kafka, err := New(
		WithDefaults(),
		WithFuncMap(funcs),
	)

	if err != nil {
		panic(err)
	}

	if err := kafka.Start(); err != nil {
		panic(err)
	}

	// Don't end program
	select {}
}
