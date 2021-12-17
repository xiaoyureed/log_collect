package main

import (
	log "github.com/sirupsen/logrus"
	"xiaoyureed.github.io/log_collection/es"
	"xiaoyureed.github.io/log_collection/etcd"
	"xiaoyureed.github.io/log_collection/global"
	"xiaoyureed.github.io/log_collection/kafka"
)

func main() {
	conf := global.Config("./config.ini")
	etcdService := etcd.NewService([]string{conf.EtcdConfig.Address})
	entries, _ := etcdService.GetCollectEntries(conf.EtcdConfig.ConfigKeyLogCollect)
	kafkaService := kafka.NewTransferService([]string{conf.KafkaConfig.Address})
	esService := es.New(conf.EsConfig.Url)
	for _, ele := range entries {
		messages := kafkaService.Consume(ele.Topic, conf.KafkaConfig.MsgChanSize)
		go esService.Put(ele.Topic, messages)
	}

	log.Info("transfer app start ok")
	select {
	}
}
