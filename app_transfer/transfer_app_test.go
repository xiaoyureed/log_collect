package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
	"sync"
	"testing"
	"xiaoyureed.github.io/log_collection/etcd"
	"xiaoyureed.github.io/log_collection/global"
	"xiaoyureed.github.io/log_collection/kafka"
)

func TestKafkaConsume(t *testing.T) {
	conf := global.Config("../config.ini")
	kafkaService := kafka.NewTransferService([]string{conf.KafkaConfig.Address})
	etcdService := etcd.NewService([]string{conf.EtcdConfig.Address})
	entries, _ := etcdService.GetCollectEntries(conf.EtcdConfig.ConfigKeyLogCollect)
	for _, entry := range entries {
		messages := kafkaService.Consume(entry.Topic, conf.KafkaConfig.MsgChanSize)
		for msg := range messages {
			fmt.Println(msg)
		}
	}

}

func TestConsumeDemo(t *testing.T) {
	config := global.Config("./config.ini")

	entries, _ := etcd.NewService([]string{config.EtcdConfig.Address}).GetCollectEntries(config.EtcdConfig.ConfigKeyLogCollect)

	topic := entries[0].Topic

	consumer, err := sarama.NewConsumer([]string{config.KafkaConfig.Address}, nil)
	if err != nil {
		log.Fatalf("error of new consumer: %v\n", err)
	}

	partitions, err := consumer.Partitions(topic)
	for _, partition := range partitions {
		pc, _ := consumer.ConsumePartition(
			topic,
			partition, // partition number
			//sarama.OffsetNewest, // 从最新标志位开始读
			sarama.OffsetOldest , // 从最老开始读
		)
		defer pc.AsyncClose()

		var wg sync.WaitGroup
		wg.Add(1)
		go func(pc sarama.PartitionConsumer) {
			for message := range pc.Messages() {
				log.Printf(">>> receive msg, partition: %v, offset: %v, key: %s, value:%s\n",
					message.Partition, message.Offset, message.Key, message.Value)
			}
		}(pc)
		wg.Wait()
	}
}
