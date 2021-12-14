package etcd

import "testing"

func TestSetupConfig(t *testing.T) {
	service, _ := NewService([]string{"127.0.0.1:2379"})
	err := service.PutCollectEntries("log_collect_config", []*CollectEntry{
		{Path: "./xxx.log", Topic: "log_collect"},
	})
	if err != nil {
		t.Fatalf("%v\n", err)
	}
}

func TestGetConf(t *testing.T) {

	service, err := NewService([]string{"127.0.0.1:2379"})
	if err != nil {
		t.Fatalf("%v\n", err)
	}
	entries, err := service.GetCollectEntries("log_collect_config")
	if err != nil {
		t.Fatalf("%v\n", err)
	}
	for _, ele := range entries {
		t.Log(ele.Path, ele.Topic)
	}
}
