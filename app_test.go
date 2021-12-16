package main

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"log"
	"sync"
	"testing"
	"time"
	"xiaoyureed.github.io/log_collection/global"
)

func TestChanBlock(t *testing.T) {
	f := func() <-chan int {
		ret := make(chan int)
		go func() {
			tick := time.Tick(time.Second)
			after := time.After(time.Second * 3)
			i := 0
			for {
				select {
				case <-tick:
					ret <- i
					i++
				case <-after:
					close(ret)

				}
			}
		}()
		return ret
	}
	ints := f()
	for i := range ints {
		log.Println(i)
	}
}

func TestChanReturn(t *testing.T) {
	f := func() <-chan int {
		ch := make(chan int)
		go func() {
			tick := time.Tick(time.Second)
			i := 0
			for {
				select {
				case <-tick:
					ch <- i
					i++
				}
			}
		}()

		return ch
	}
	ch := f()
	for ele := range ch {
		t.Log(ele)
	}
}

func TestKafkaConsume(t *testing.T) {
	config := global.Config("./config.ini")

	consumer, err := sarama.NewConsumer([]string{config.KafkaConfig.Address}, nil)
	if err != nil {
		log.Fatalf("error of new consumer: %v\n", err)
	}

	partitions, err := consumer.Partitions(config.KafkaConfig.Topic)
	for partition := range partitions {
		pc, _ := consumer.ConsumePartition(
			config.KafkaConfig.Topic,
			int32(partition), // partition number
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

func TestEtcdClient(t *testing.T) {

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: time.Second * 5,
	})
	if err != nil {
		fmt.Printf(">>> %v\n", err)
		return
	}
	defer client.Close()
	log.Println(">>> connect etcd ok")

	var wg sync.WaitGroup
	wg.Add(1)
	defer func() {
		println("wait")
		wg.Wait()
	}()
	go func() {
		// 可以监控不存在的 key
		watch := client.Watch(context.Background(), "bb")
		after := time.After(time.Second * 2)
		for {
			select {
			case response := <-watch:
				for _, event := range response.Events {
					log.Printf("type: %v, key: %s, value: %s\n", event.Type, event.Kv.Key, event.Kv.Value)
				}
			case <-after:
				log.Println("timeout")
				wg.Done()
			default:
				println("default...")
			}
		}
	}()

	timeout, cancelFunc := context.WithTimeout(context.Background(), time.Second)
	defer cancelFunc()
	// if the key already exist, old value will be covered by new value
	put, err := client.Put(timeout, "bb", "bb1")
	if err != nil {
		switch err {
		case context.Canceled:
			log.Fatalf("ctx is canceled by another routine: %v", err)
		case context.DeadlineExceeded:
			log.Fatalf("ctx is attached with a deadline is exceeded: %v", err)
		case rpctypes.ErrEmptyKey:
			log.Fatalf("client-side error: %v", err)
		default:
			log.Fatalf("bad cluster endpoints, which are not etcd servers: %v", err)
		}
		return
	}

	log.Printf(">>> put ok, resp: %v\n", put)

	timeout, cancelFunc = context.WithTimeout(context.Background(), time.Second)
	defer cancelFunc()
	get, err := client.Get(timeout, "aa")
	if err != nil {
		log.Fatalf(">>> %v\n", err)
	}
	for _, ele := range get.Kvs {
		log.Printf(">>> key: %s, value: %s\n", ele.Key, ele.Value)
	}

}

func TestSendToKafka(t *testing.T) {
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
	message.Topic = "topic_test"
	message.Value = sarama.StringEncoder("hahaha")

	pid, offset, err := producer.SendMessage(message)
	if err != nil {
		fmt.Println("error of send msg: ", err)
		return
	}
	fmt.Printf("%v, %v\n", pid, offset)
}
