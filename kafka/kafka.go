package kafka

import (
	"fmt"
	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
	"time"
)

// Kafka producer client
var client sarama.SyncProducer

// Buf channel for incoming log
var MsgChan chan *sarama.ProducerMessage

const TopicDefaultLogCollect = "log_collect"

func Connect(address []string, chanSize int) error {
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
	client = producer
	//defer func() {
	//	err := client.Close()
	//	if err != nil {
	//		log.Errorf(">>> error of close kafka producer: %v\n", err)
	//	}
	//}()
	log.Info("connect kafka success")

	MsgChan = make(chan *sarama.ProducerMessage, chanSize)

	// Read from msg chan and send
	go func() {
		for {
			select {
			case msg := <-MsgChan:
				err := sendMsg(msg)
				if err != nil {
					log.Errorf(">>> error of send msg to kafka: %v\n", err)
				}
				return
			default:
			}
			time.Sleep(time.Second)
			log.Debug(">>> no msg exist in msg chan ,sleep 1s")
		}
	}()
	return nil
}

func BuildMsg(msg string) *sarama.ProducerMessage {
	return BuildMsgWithTopic(msg, TopicDefaultLogCollect)
}

func BuildMsgWithTopic(msg, topic string) *sarama.ProducerMessage {
	message := sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(msg),
	}
	return &message
}

func sendMsg(msg *sarama.ProducerMessage) error {
	pid, offset, err := client.SendMessage(msg)
	if err != nil {
		fmt.Println("error of send msg: ", err)
		return err
	}
	//fmt.Printf("%v, %v\n", pid, offset)
	log.Debugf(">>> send success, topic: %v, msg: %v, offset: %v, pid: %v\n", msg.Topic, msg.Value, offset, pid)
	return nil
}

// Send msg to kafka
func SendString(msg, topic string) error {
	return sendMsg(BuildMsgWithTopic(msg, topic))
}

// Send string msg to kafka with default topic(log_collect)
func SendStringDefaultTopic(msg string) error {
	return SendString(msg, TopicDefaultLogCollect)
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
