package etcd

import (
	"testing"
	"xiaoyureed.github.io/log_collection/global"
)

func TestSetupConfig(t *testing.T) {
	service, _ := NewService([]string{"127.0.0.1:2379"})
	err := service.PutCollectEntries("log_collect_config", []*CollectEntry{
		{Path: "./xxx.log", Topic: "log_collect"},
		//{Path: "./config.ini", Topic: "log_collect"},
	})
	if err != nil {
		t.Fatalf("%v\n", err)
	}
}

func TestWatch(t *testing.T) {
	conf := global.Config("../config.ini")
	service, _ := NewService([]string{conf.EtcdConfig.Address})
	service.watchCollectEntries(conf.EtcdConfig.ConfigKeyLogCollect)
}

func TestGetConf(t *testing.T) {
	conf := global.Config("../config.ini")
	service, _ := NewService([]string{conf.EtcdConfig.Address})
	entries, err := service.GetCollectEntries(conf.EtcdConfig.ConfigKeyLogCollect)

	if err != nil {
		t.Fatalf("%v\n", err)
	}
	for _, ele := range entries {
		t.Log(ele.Path, ele.Topic)
	}
}
