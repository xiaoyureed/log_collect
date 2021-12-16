package main

import (
	log "github.com/sirupsen/logrus"
	"os"
	"xiaoyureed.github.io/log_collection/etcd"
	"xiaoyureed.github.io/log_collection/global"
	"xiaoyureed.github.io/log_collection/kafka"
	"xiaoyureed.github.io/log_collection/tailf"
)


func main() {

	setupLog()

	conf := global.Config("./config.ini")

	serviceEtcd := etcd.NewService([]string{conf.EtcdConfig.Address})

	entries, newEntries := serviceEtcd.GetCollectEntries(conf.EtcdConfig.ConfigKeyLogCollect)

	serviceTailf, err := tailf.NewService(entries)
	if err != nil {
		log.Errorf(">>> %v\n", err)
		return
	}
	serviceKafka := kafka.NewService([]string{conf.KafkaConfig.Address}, conf.KafkaConfig.MsgChanSize)

	serviceTailf.Start(serviceKafka)
	serviceTailf.WatchNewEntries(newEntries, serviceKafka)

	select {

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
