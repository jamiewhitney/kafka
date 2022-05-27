package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	log "github.com/sirupsen/logrus"
	"math/rand"
	"os"
	"strconv"
	"time"
)

type Consumer struct {
	reader *kafka.Consumer
}

type Producer struct {
	writer *kafka.Producer
}

func main() {
	topic := "test-topic"
	hosts := "localhost:9092"
	group := "test-group"

	writer := NewProducer(hosts)

	writer.Write(topic)

	reader := NewConsumer(hosts, []string{topic}, group)

	reader.Listen()
}

func NewConsumer(hosts string, topic []string, group string) *Consumer {
	reader, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": hosts,
		"group.id":          group,
	})
	if err != nil {
		log.Println(err)
	}

	err = reader.SubscribeTopics(topic, nil)
	if err != nil {
		log.Error(err)
	}

	return &Consumer{reader: reader}
}

func (c *Consumer) Listen() {
	for {
		select {
		default:
			ev := c.reader.Poll(100)
			if ev == nil {
				continue
			}
			switch e := ev.(type) {
			case *kafka.Message:
				fmt.Printf("message at topic/partition/offset/time %v/%v: %s = %s\n", e.TopicPartition, e.Timestamp, string(e.Key), string(e.Value))
			case kafka.Error:
				fmt.Fprintf(os.Stderr, "%% Error: %v: %v\n", e.Code(), e)
			default:
				fmt.Printf("Ignored %v\n", e)
			}
		}
	}
}

func NewProducer(hosts string) *Producer {
	writer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": hosts,
	})
	if err != nil {
		log.Error(err)
	}
	log.Info("created producer")
	return &Producer{writer: writer}
}

func (p *Producer) Write(topic string) {
	i := 0
	for {
		err := p.writer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Key:            []byte(strconv.Itoa(i)),
			Value:          []byte("this is message" + strconv.Itoa(i)),
		}, nil)
		if err != nil {
			log.Error("couldn't write message", err)
		}

		fmt.Println("writes:", i)
		i++
		n := rand.Intn(10)
		time.Sleep(time.Duration(n) * time.Second)

	}
}
