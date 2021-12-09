package main

import (
	"github.com/go-ini/ini"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"os"
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
	MsgChanSize int    `ini:"msg_channel_size" `
}
type CollectConfig struct {
	LogfilePath string `ini:"logfile_path"`
}

func main() {
	setupLog()

	config, err := buildConfig("./config.ini")
	if err != nil {
		log.Errorf(">>> %+v\n", err)
		return
	}

	err = kafka.Connect([]string{config.KafkaConfig.Address}, config.KafkaConfig.MsgChanSize)
	if err != nil {
		log.Errorf(">>> %v\n", err)
		return
	}

	filename := config.CollectConfig.LogfilePath
	err = tailf.Init(filename)
	if err != nil {
		log.Errorf(">>> %v\n", err)
		return
	}

	for {
		line, ok := <-tailf.TailObj.Lines
		if !ok {
			log.Infof(">>> tail file closed, sleep 1s, filename: %v\n", filename)
			time.Sleep(time.Second)
			continue
		}
		msg := kafka.BuildMsg(line.Text)
		kafka.MsgChan <- msg
		log.Debugf(">>> send msg to msg chan ok: %v\n", msg)
	}
}

func setupLog() {
	formatter := new(log.TextFormatter)
	formatter.FullTimestamp = true
	formatter.TimestampFormat = "2006-01-02 15:04:05"
	formatter.DisableTimestamp = false // 禁止显示时间
	formatter.DisableColors = false    // 禁止颜色显示

	log.SetFormatter(formatter)
	log.SetOutput(os.Stdout)
	log.SetLevel(log.DebugLevel)

	log.Info(">>> setup log config ok")
}

func buildConfig(filename string) (*Config, error) {
	ret := new(Config)
	err := ini.MapTo(ret, filename)
	if err != nil {
		//return nil, fmt.Errorf("error of map ini file to struct: %v\n", err)
		return nil, errors.Wrap(err, "error of map ini file to struct")
	}

	//fmt.Printf()
	log.Info(">>> build config ok")
	log.Debugf("%#v\n", ret)

	return ret, nil
}
