package kafka

import (
	"fmt"
	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
)

// Kafka producer client
var Client sarama.SyncProducer

const TOPIC_DEFAULT_LOG_COLLECT = "log_collect"

func Connect(address []string) error {
	// producer configuration
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll          // ack
	config.Producer.Partitioner = sarama.NewRandomPartitioner //partition
	config.Producer.Return.Successes = true                   //确认

	producer, err := sarama.NewSyncProducer(address, config)
	if err != nil {
		fmt.Println("error of build producer:", err)
		return err
	}
	Client = producer
	log.Info("connect kafka success")
	return nil
}

// Send msg to kafka
func SendString(msg, topic string) error {
	message := &sarama.ProducerMessage{}
	message.Topic = topic
	message.Value = sarama.StringEncoder(msg)

	pid, offset, err := Client.SendMessage(message)
	if err != nil {
		fmt.Println("error of send msg: ", err)
		return err
	}
	//fmt.Printf("%v, %v\n", pid, offset)
	log.Debugf(">>> send success, topic: %v, msg: %v, offset: %v, pid: %v\n", topic, msg, offset, pid)
	return nil
}

// Send string msg to kafka with default topic(log_collect)
func SendStringDefaultTopic(msg string) error {
	return SendString(msg, TOPIC_DEFAULT_LOG_COLLECT)
}

func sendToKafka(msg, topic string) {
	// producer configuration
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll          // ack
	config.Producer.Partitioner = sarama.NewRandomPartitioner //partition
	config.Producer.Return.Successes = true                   //确认

	producer, err := sarama.NewSyncProducer([]string{"127.0.0.1:9092"}, config)
	if err != nil {
		fmt.Println("error of build producer:", err)
		return
	}
	defer producer.Close()

	message := &sarama.ProducerMessage{}
	message.Topic = topic
	message.Value = sarama.StringEncoder(msg)

	pid, offset, err := producer.SendMessage(message)
	if err != nil {
		fmt.Println("error of send msg: ", err)
		return
	}
	fmt.Printf("%v, %v\n", pid, offset)
}
