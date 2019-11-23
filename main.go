package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"strings"
	"time"
)

func getOffsetTimestamp(consumer sarama.Consumer, topic string, partition int32, offset int64) (time.Time, error) {
	cp, err := consumer.ConsumePartition(topic, partition, offset)
	if nil != err {
		return time.Time{}, fmt.Errorf("cannot consumme partition: ", err)
	}

	select {
	case <-time.After(500 * time.Millisecond):
		return time.Time{}, fmt.Errorf("no message in topic")
	case consumerError := <-cp.Errors():
		return time.Time{}, fmt.Errorf("consumer error: ", consumerError.Err)
	case msg := <-cp.Messages():
		return msg.Timestamp, nil
	}

	return time.Time{}, fmt.Errorf("unknow error")
}

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
		ts, err := getOffsetTimestamp(consumer, topic, partitions[0], sarama.OffsetOldest)
		if err != nil {
			fmt.Printf("cannot get oldest message in topic %s : %v\n", topic, err)
		} else {
			fmt.Printf("oldest message in topic %s at %v\n", topic, ts)
		}
	}

	consumer.Close()
}
