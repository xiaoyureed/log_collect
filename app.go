package main

import (
	"fmt"
	"github.com/go-ini/ini"
	log "github.com/sirupsen/logrus"
	"time"
	"xiaoyureed.github.io/log_collection/kafka"
	"xiaoyureed.github.io/log_collection/tailf"
)

// 全局配置 struct
type Config struct {
	KafkaConfig   KafkaConfig `ini:"kafka"`
	CollectConfig `ini:"collect"`
}
type KafkaConfig struct {
	Address     string `ini:"address"`
	Topic       string `ini:"topic"`
	MsgChanSize int    `json:"msg_channel_size"`
}
type CollectConfig struct {
	LogfilePath string `ini:"logfile_path"`
}

func main() {
	config, err := buildConfig("./config.ini")
	if err != nil {
		return
	}

	err = kafka.Connect([]string{config.KafkaConfig.Address}, config.KafkaConfig.MsgChanSize)
	if err != nil {
		return
	}

	filename := config.CollectConfig.LogfilePath
	err = tailf.Init(filename)
	if err != nil {
		return
	}

	for {
		line, ok := <-tailf.TailObj.Lines
		if !ok {
			log.Errorf(">>> tail file closed, filename: %v\n", filename)
			time.Sleep(time.Second)
			continue
		}
		kafka.MsgChan <- kafka.BuildMsg(line.Text)
	}
}

func buildConfig(filename string) (*Config, error) {
	ret := new(Config)
	err := ini.MapTo(ret, filename)
	if err != nil {
		log.Errorf("error of map ini file to struct: %v\n", err)
		return nil, err
	}
	fmt.Printf("%#v\n", ret)
	return ret, nil
}

func loadConfigTest(filename string) error {
	load, err := ini.Load(filename)
	if err != nil {
		log.Errorf("erro of load config file: %v\n", err)
		return err
	}
	kafkaAddr := load.Section("kafka").Key("address").String()
	println(kafkaAddr)

	return nil
}
