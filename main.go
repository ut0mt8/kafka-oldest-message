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

	brokers := []string{"localhost:9092"}

	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		panic(err)
	}

	topics, _ := client.Topics()

	for _, topic := range topics {
		if strings.Contains(topic, "__consumer_offsets") {
			continue
		}

		partitions, _ := client.Partitions(topic)
		for _, part := range partitions {
			old, _ := client.GetOffset(topic, part, sarama.OffsetOldest)

			leader, _ := client.Leader(topic, part)
			if ok, _ := leader.Connected(); !ok {
				leader.Open(client.Config())
			}

			ts, err := lag.GetTimestamp(leader, topic, part, old)
			if err == nil {
				fmt.Printf("oldest message in topic %s partition %d at %v\n", topic, part, ts)
			}
		}
	}
	client.Close()
}
