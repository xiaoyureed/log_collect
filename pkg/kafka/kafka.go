package kafka

import (
	"encoding/json"
	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
)

const TopicDefaultLogCollect = "log_collect"

type Service struct {
	// client is a Kafka producer client
	client sarama.SyncProducer

	// msgChan is a Buf channel for incoming log
	msgChan chan *sarama.ProducerMessage
}

// Put put msg into msgChan, then the msg will be sent to kafka
func (s Service) PutWithDefaultTopic(msg string) {
	s.msgChan <- buildMsgWithDefaultTopic(msg)

}
func (s Service) Put(msg, topic string) {
	s.msgChan <- buildMsg(msg, topic)
}

func NewService(address []string, chanSize int) *Service {
	// producer configuration
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll          // ack
	config.Producer.Partitioner = sarama.NewRandomPartitioner //partition
	config.Producer.Return.Successes = true                   //确认

	producer, err := sarama.NewSyncProducer(address, config)
	if err != nil {
		log.Fatalf(">>>kafka, error of create sync producer, err: %v", err)
	}

	log.Info(">>> connect kafka ok")

	msgChan := make(chan *sarama.ProducerMessage, chanSize)
	log.Debugf(">>> make msg chan ok, size: %d\n", chanSize)

	// Read from msg chan and send to kafka
	go func() {
		for {
			select {
			case msg := <-msgChan:
				log.Debugf(">>> receive msg from chan ok: %v\n", msg)

				partitionId, offset, err := producer.SendMessage(msg)
				if err != nil {
					log.Errorf(">>>kafka, error of send msg to kafka %v\n", err)
					_ = producer.Close()
					return
				}
				log.Debugf(">>>kafka, send msg ok, offset: %v, pid: %v,  msg: %v\n", offset, partitionId, msg)

				//default:
			}
			//time.Sleep(time.Second)
			//log.Debug(">>> no msg exist in msg chan ,sleep 1s")
		}
	}()

	return &Service{client: producer, msgChan: msgChan}
}

func buildMsgWithDefaultTopic(msg string) *sarama.ProducerMessage {
	log.Debugf(">>> the default topic (%v) is used.\n", TopicDefaultLogCollect)
	return buildMsg(msg, TopicDefaultLogCollect)
}

func buildMsg(msg, topic string) *sarama.ProducerMessage {
	message := sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(msg),
	}
	log.Debugf(">>> build msg ok, msg: %v, topic: %v\n", msg, topic)
	return &message
}

//为日志 transfer 准备的 service
type TransferService struct {
	consumer sarama.Consumer
}

func NewTransferService(addrs []string) *TransferService {
	consumer, err := sarama.NewConsumer(addrs, nil)
	if err != nil {
		log.Fatalf("error of create consumer, err: %v\n", err)
	}
	//partitions, err := consumer.Partitions()
	//if err != nil {
	//	log.Fatalf("error of get partitons by consumer with topic %v, err: %v\n", )
	//}
	return &TransferService{consumer: consumer}
}

//从 kafka 中消费, 消息发往返回的 channel 中
func (t *TransferService) Consume(topic string, chanSize int) <-chan map[string]interface{} {
	ret := make(chan map[string]interface{}, chanSize)

	partitions, err := t.consumer.Partitions(topic)
	if err != nil {
		log.Fatalf("error of get partitons by consumer with topic %v, err: %v\n", topic, err)
	}
	for _, partition := range partitions {
		partitionConsumer, err := t.consumer.ConsumePartition(topic, partition, sarama.OffsetOldest)
		if err != nil {
			log.Fatalf("error of get partition consumer, topic: %v, err: %v\n", topic, err)
		}

		messages := partitionConsumer.Messages()
		go func() {
			defer partitionConsumer.AsyncClose()

			for msg := range messages {
				m := make(map[string]interface{})
				err := json.Unmarshal(msg.Value, &m)
				if err != nil {
					log.Warnf("error of unmarshal to a map, this msg will be ignored, target: %s, err: %v\n", msg.Value, err)
					continue
				}

				ret <- m
			}
		}()

	}

	return ret
}
