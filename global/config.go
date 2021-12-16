package global

import (
	"fmt"
	"github.com/go-ini/ini"
	log "github.com/sirupsen/logrus"
	"net"
	"sync"
)

var conf *config
var locker sync.Mutex

// Config represent a global config model
type config struct {
	KafkaConfig kafkaConfig `ini:"kafka"`
	EtcdConfig etcdConfig `ini:"etcd"`
}

// kafkaConfig is a kafka config model
type kafkaConfig struct {
	Address     string `ini:"address"`
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

	configKey := fmt.Sprintf(conf.EtcdConfig.ConfigKeyLogCollect, ipString())
	log.Debugf("resolve config key: %v", configKey)

	conf.EtcdConfig.ConfigKeyLogCollect = configKey

	return conf
}


func ipString() string {
	dial, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Fatalf("error of get local ip, err:%v", err)
	}
	defer dial.Close()
	addr := dial.LocalAddr().(*net.UDPAddr)
	return addr.IP.String()
}