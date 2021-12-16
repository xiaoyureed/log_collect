package etcd

import (
	"testing"
	"xiaoyureed.github.io/log_collection/global"
)

func TestSetupConfig(t *testing.T) {
	conf := global.Config("../config.ini")
	service:= NewService([]string {conf.EtcdConfig.Address})
	err := service.PutCollectEntries(conf.EtcdConfig.ConfigKeyLogCollect, []*CollectEntry{
		{Path: "./xxx.log", Topic: "log_collect"},
		//{Path: "./config.ini", Topic: "log_collect"},
	})
	if err != nil {
		t.Fatalf("%v\n", err)
	}
}

func TestWatch(t *testing.T) {
	conf := global.Config("../config.ini")
	service:= NewService([]string{conf.EtcdConfig.Address})
	service.watchCollectEntries(conf.EtcdConfig.ConfigKeyLogCollect)
}

func TestGetConf(t *testing.T) {
	conf := global.Config("../config.ini")
	service:= NewService([]string{conf.EtcdConfig.Address})
	entries, _ := service.GetCollectEntries(conf.EtcdConfig.ConfigKeyLogCollect)

	for _, ele := range entries {
		t.Log(ele.Path, ele.Topic)
	}
}
