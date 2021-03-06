package main

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"log"
	"net"
	"sync"
	"testing"
	"time"
)

func TestNet(t *testing.T) {
	dial, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Fatal(err)
	}
	defer dial.Close()

	addr := dial.LocalAddr()
	fmt.Println(addr)
	udpAddr := addr.(*net.UDPAddr)
	fmt.Println(udpAddr.IP.String())

	println("------------------------")

	addrs, _ := net.InterfaceAddrs()
	for _, addr := range addrs {
		ip, ok := addr.(*net.IPNet)
		if !ok {
			continue
		}
		if ip.IP.IsLoopback() {
			continue
		}
		if ip.IP.IsGlobalUnicast() {
			continue
		}
		println(ip.IP.String())
	}
}

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
