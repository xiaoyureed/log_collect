package main

import (
	log "github.com/sirupsen/logrus"
	"xiaoyureed.github.io/log_collection/pkg/es"
	"xiaoyureed.github.io/log_collection/pkg/etcd"
	"xiaoyureed.github.io/log_collection/pkg/global"
	"xiaoyureed.github.io/log_collection/pkg/kafka"
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
