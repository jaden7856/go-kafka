package main

import (
	"github.com/Shopify/sarama"
)

var kafkaClient sarama.AsyncProducer

func main() {
	brokers := []string{"broker:9092"}
	kafkaClient, _ = sarama.NewAsyncProducer(brokers, nil)
}

func SendMessage(topic string, message string) {
	kafkaClient.Input() <- &sarama.ProducerMessage{
		Topic:     topic,
		Partition: -1,
		Value:     sarama.StringEncoder(message),
	}
}
