package main

import (
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
	"net/smtp"
)

func main() {
	log.Println("starting..")
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "pkc-l6wr6.europe-west2.gcp.confluent.cloud:9092",
		"security.protocol": "SASL_SSL",
		"sasl.username":     "VF4ELHMVULALFWAA",
		"sasl.password":     "6uW09Sxq4cTSjWHJd48BellhYTdGm1PpbJ9BJzvtGjdun5zCOfbyObPTErftaQk8",
		"sasl.mechanism":    "PLAIN",
		"group.id":          "myGroup",
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		panic(err)
	}

	consumer.SubscribeTopics([]string{"default"}, nil)

	for {
		msg, err := consumer.ReadMessage(-1)
		if err != nil {
			// The client will automatically try to recover from all errors.
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
			return
		}

		fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))

		var message map[string]interface{}

		err = json.Unmarshal(msg.Value, &message)
		if err != nil {
			log.Fatalf("err0: %v\n", err)
		}

		ambassadorMessage := []byte(fmt.Sprintf("You earned $%f from the link #%s", message["ambassador_revenue"].(float64), message["code"]))

		err = smtp.SendMail("host.docker.internal:1025", nil, "no-reply@email.com", []string{message["ambassador_email"].(string)}, ambassadorMessage)
		if err != nil {
			log.Fatalf("err1: %v\n", err)
		}

		adminMessage := []byte(fmt.Sprintf("Order #%f with a total of $%f has been completed", message["id"].(float64), message["admin_revenue"].(float64)))

		err = smtp.SendMail("host.docker.internal:1025", nil, "no-reply@email.com", []string{"admin@admin.com"}, adminMessage)
		if err != nil {
			log.Fatalf("err2: %v\n", err)
		}
	}

	consumer.Close()

}
