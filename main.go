package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/ut0mt8/kalag/lag"
	"strings"
)

func main() {

	config := sarama.NewConfig()
	config.Version = sarama.V2_1_0_0
	config.Consumer.Offsets.AutoCommit.Enable = false
	config.Consumer.Return.Errors = true

	brokers := []string{"localhost:9092"}

	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		panic(err)
	}

	topics, _ := consumer.Topics()

	for _, topic := range topics {
		if strings.Contains(topic, "__consumer_offsets") {
			continue
		}

		partitions, _ := consumer.Partitions(topic)
		ts, err := lag.GetOffsetTimestamp(consumer, topic, partitions[0], sarama.OffsetOldest)
		if err != nil {
			fmt.Printf("cannot get oldest message in topic %s : %v\n", topic, err)
		} else {
			fmt.Printf("oldest message in topic %s at %v\n", topic, ts)
		}
	}

	consumer.Close()
}
