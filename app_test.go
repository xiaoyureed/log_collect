package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/Shopify/sarama"
	perrors "github.com/pkg/errors"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"log"
	"sync"
	"testing"
	"time"
)

type Atype struct {}

func (a Atype) Close() error {
	return errors.New("error off close")
}

func TestDefer(t *testing.T) {

	f := func(a Atype) (err error) {
		defer func() {
			// err 变量赋值会创建一个全新的变量, 不再是返回值定义中的 err 了
			// 若希望 defer 中的错误被返回回去, 使用 = 不要使用 :=
			if err := a.Close(); err != nil {

			}
		}()
		return
	}
	err := f(Atype{})
	if err != nil {
		log.Printf("%v\n", err)
	}
}

func TestKafkaConsume(t *testing.T) {
	consumer, err := sarama.NewConsumer([]string{"localhost:2379"}, nil)
	if err != nil {
		log.Fatalf("error of new consumer: %v\n", err)
	}
	config, _ := buildConfig("./config.ini")

	partitions, err := consumer.Partitions(config.KafkaConfig.Topic)
	for partition := range partitions {
		pc, _ := consumer.ConsumePartition(
			config.KafkaConfig.Topic,
			int32(partition), // partition number
			sarama.OffsetNewest, // 从最新标志位开始读
		)
		defer pc.AsyncClose()
	}
}

func TestWaitGroup(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)
	defer func() {
		println("wait")
		wg.Wait()
	}()
	go func() {
		for true {
			select {
			// 永远无法结束, 因为存在 default 分支, select 变为非阻塞, 会有多次循环,每次for循环, 都会新建一个 after channel, 永远不会 timeout
			case <-time.After(time.Second * 2):
				println("timeout")
				wg.Done()
				//default:

			}
		}
	}()
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

func TestPkgErrors(t *testing.T) {
	err := method1()
	//fmt.Printf("%v\n", err)
	//fmt.Printf("%q\n", err)
	fmt.Printf("%+v\n", err)

	err1 := perrors.Cause(err)
	//fmt.Printf("%v\n", err1)
	//fmt.Printf("%q\n", err1)
	fmt.Printf("%+v\n", err1)

}

func method1() error {
	_, err := doSth("a1")
	err = perrors.Wrap(err, "wrapped err msg")
	return err
}

func TestTime(t *testing.T) {
	for next := range time.Tick(time.Second * 3) {
		fmt.Printf("%v\n", next)
		//2021-12-08 23:24:49.772447 +0800 CST m=+3.004782554
	}
}

func TestA(t *testing.T) {
	err := grandParentMethod("a1")
	var e flagErr
	// As 第二个参数是用来存放提取出来的 err 的指针
	// 会把最内层的 error 提取出来
	if errors.As(err, &e) {
		fmt.Printf("err: %v, e: %v\n", err, e)
		//print
		//err: error of grand parent method, orginal: error of parent method, orginal err: error of flag, e: error of flag

		err := errors.Unwrap(err)
		fmt.Printf("unwrap1: %v\n", err)
		//unwrap1: error of parent method, orginal err: error of flag
		err = errors.Unwrap(err)
		fmt.Printf("unwrap2: %v\n", err)
		//unwrap2: error of flag
		err = errors.Unwrap(err)
		fmt.Printf("unwrap3: %v\n", err)
		// unwrap3: <nil>
	}
}

type flagErr struct {
	Msg string
}

func (e flagErr) Error() string {
	return e.Msg
}

func grandParentMethod(flag string) error {
	err := parentMethod(flag)
	if err != nil {
		err := fmt.Errorf("error of grand parent method, orginal: %w", err)
		fmt.Printf("grandParentMethod: type: %T, value: %v\n", err, err)
		//print
		//grandParentMethod: type: *fmt.wrapError, value: error off grand parent method, orginal: error of parent method, orginal err: error of flag
		return err
	}
	return nil
}

func parentMethod(flag string) error {
	_, err := doSth(flag)
	if err != nil {
		//包装错误时, 使用 %w
		err := fmt.Errorf("error of parent method, orginal err: %w", err)
		fmt.Printf("parentMethod: type: %T, value: %v\n", err, err)
		//print
		//parentMethod: type: *fmt.wrapError, value: error of parent method, orginal err: error of flag
		return err
	}
	return nil
}

func doSth(flag string) (string, error) {
	if flag != "a" {
		err := flagErr{"error of flag"}
		fmt.Printf("doSth: type: %T, value: %v\n", err, err)
		//print
		//doSth: type: main.flagErr, value: error of flag
		return "hello", err
	}
	return flag, nil
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
