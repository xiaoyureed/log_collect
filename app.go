package main

import (
	"github.com/go-ini/ini"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"os"
	"strings"
	"sync"
	"time"
	"xiaoyureed.github.io/log_collection/etcd"
	"xiaoyureed.github.io/log_collection/kafka"
	"xiaoyureed.github.io/log_collection/tailf"
)

// Config represent a global config model
type Config struct {
	KafkaConfig   KafkaConfig `ini:"kafka"`
	//CollectConfig `ini:"collect"`
	EtcdConfig    `ini:"etcd"`
}

// KafkaConfig is a kafka config model
type KafkaConfig struct {
	Address     string `ini:"address"`
	Topic       string `ini:"topic"`
	MsgChanSize int    `ini:"msg_channel_size" `
}

//EtcdConfig is a etcd config model
type EtcdConfig struct {
	Address             string `ini:"address"`
	ConfigKeyLogCollect string `ini:"config_key_log_collect"`
}

func main() {

	setupLog()

	config, err := buildConfig("./config.ini")
	if err != nil {
		log.Errorf(">>> %+v\n", err)
		return
	}

	serviceEtcd, err := etcd.NewService([]string{config.EtcdConfig.Address})
	if err != nil {
		log.Fatalf(">>> %v\n", err)
	}
	entries, err := serviceEtcd.GetCollectEntries(config.EtcdConfig.ConfigKeyLogCollect)
	if err != nil {
		log.Fatalf(">>> %v\n", err)
	}

	serviceTailf, err := tailf.NewService(entries)
	if err != nil {
		log.Errorf(">>> %v\n", err)
		return
	}

	var wg sync.WaitGroup

	for _, ele := range serviceTailf.TailTasks() {
		ele := ele
		wg.Add(1)
		go func() {
			defer wg.Done()

			serviceKafka, err := kafka.NewService([]string{config.KafkaConfig.Address}, config.KafkaConfig.MsgChanSize)
			if err != nil {
				log.Errorf(">>> %v\n", err)
				return
			}
			for {
				line, ok := <-ele.Tail.Lines
				if !ok {
					log.Infof(">>> tail file closed, sleep 1s, filename: %v\n", ele.Tail.Filename)
					time.Sleep(time.Second)
					continue
				}

				// trim spaces and \r\n
				//https://stackoverflow.com/questions/44448384/how-remove-n-from-lines
				lineTrim := strings.TrimFunc(strings.TrimSpace(line.Text), func(r rune) bool {
					return r == '\r' || r == '\n'
				})
				if len(lineTrim) == 0 {
					log.Debugln(">>> empty line")
					continue
				}

				//serviceKafka.PutWithDefaultTopic(line.Text)
				serviceKafka.Put(line.Text, ele.Topic)
				log.Debugf(">>> send msg to msg chan ok: %v\n", line.Text)
			}

		}()
	}

	log.Debugln("waiting for reading log...")
	wg.Wait()

	log.Debugln("main end -------")
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
	log.Debugf("%+v\n", ret)

	return ret, nil
}
