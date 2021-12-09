package kafka

import (
	"github.com/Shopify/sarama"
	perrors "github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
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
		return perrors.Wrap(err, "error of build sync producer")
	}
	client = producer
	//defer func() {
	//	err := client.Close()
	//	if err != nil {
	//		log.Errorf(">>> error of close kafka producer: %v\n", err)
	//	}
	//}()
	log.Info(">>> connect kafka ok")

	MsgChan = make(chan *sarama.ProducerMessage, chanSize)
	log.Debugf(">>> make msg chan ok, size: %d\n", chanSize)

	// Read from msg chan and send
	go func() {
		for {
			select {
			case msg := <-MsgChan:
				log.Debugf(">>> receive msg from chan ok: %v\n", msg)

				err := sendMsg(msg)
				if err != nil {
					log.Errorf("%v\n", err)
					_ = client.Close()
					return
				}
				//default:
			}
			//time.Sleep(time.Second)
			//log.Debug(">>> no msg exist in msg chan ,sleep 1s")
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
		return perrors.Wrap(err, "error of send msg")
	}
	log.Debugf(">>> send msg ok, offset: %v, pid: %v,  msg: %v\n", offset, pid, msg)
	return nil
}
