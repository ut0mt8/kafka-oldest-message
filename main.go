package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"strings"
        "time"
)

func main() {

	config := sarama.NewConfig()
        config.Version = sarama.V2_1_0_0
	config.Consumer.Offsets.CommitInterval = 60
	config.Consumer.Return.Errors = true

	brokers := []string{"localhost:9092"}

	client, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		panic(err)
	}

	topics, _ := client.Topics()

	for _, topic := range topics {
		if strings.Contains(topic, "__consumer_offsets") {
			continue
		}

		partitions, _ := client.Partitions(topic)
		consumer, err := client.ConsumePartition(topic, partitions[0], sarama.OffsetOldest)
		if nil != err {
			panic(err)
		}

		select {
		case consumerError := <-consumer.Errors():
			fmt.Println("consumerError: ", consumerError.Err)
		case msg := <-consumer.Messages():
			fmt.Printf("oldest message in topic %s at %v\n", topic, msg.Timestamp)
                case <-time.After(500 * time.Millisecond):
			fmt.Printf("no message in topic %s\n", topic)
		}
	}

	client.Close()
}
