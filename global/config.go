package global

import (
	"github.com/go-ini/ini"
	log "github.com/sirupsen/logrus"
	"sync"
)

var conf *config
var locker sync.Mutex

// Config represent a global config model
type config struct {
	KafkaConfig kafkaConfig `ini:"kafka"`
	//CollectConfig `ini:"collect"`
	EtcdConfig etcdConfig `ini:"etcd"`
}

// kafkaConfig is a kafka config model
type kafkaConfig struct {
	Address     string `ini:"address"`
	Topic       string `ini:"topic"`
	MsgChanSize int    `ini:"msg_channel_size" `
}

//etcdConfig is a etcd config model
type etcdConfig struct {
	Address             string `ini:"address"`
	ConfigKeyLogCollect string `ini:"config_key_log_collect"`
}

func Config(filename string) *config {
	if conf != nil {
		return conf
	}

	locker.Lock()
	defer locker.Unlock()

	//double check
	if conf != nil {
		return conf
	}

	conf = &config{}
	err := ini.MapTo(conf, filename)
	if err != nil {
		log.Fatalf(">>>global, error of map ini file to struct, filepath:%v, err: %v\n", filename, err)
	}

	log.Info(">>> build config ok")
	log.Debugf("%+v\n", *conf)

	return conf
}
