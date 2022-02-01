package main

import (
	"context"
	"fmt"
	"strconv"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

var (
	brokers = []string{"192.168.1.171:9092", "192.168.1.172:9092", "192.168.1.173:9092"}
	topic = "test-topic"
	GroupID = "test-group"
)

func main() {
	go Producer()
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		Topic:  topic,
		GroupID: GroupID,
	})

	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			break
		}
		fmt.Printf("message at topic/partition/offset/time %v/%v/%v: %s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
	}

	if err := r.Close(); err != nil {
		fmt.Println("failed to close reader:", err)
	}
}

func Producer() {
	i := 0

	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: brokers,
		Topic:   topic,
	})
	ctx := context.Background()

	for {
		err := w.WriteMessages(ctx, kafka.Message{
			Key:   []byte(strconv.Itoa(i)),
			Value: []byte("this is message" + strconv.Itoa(i)),
		})
		if err != nil {
			panic("could not write message " + err.Error())
		}

		fmt.Println("writes:", i)
		i++
		time.Sleep(time.Second)
	}
}
