package main

import (
	"errors"
	"fmt"
	"github.com/Shopify/sarama"
	perrors "github.com/pkg/errors"
	"testing"
	"time"
)

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

