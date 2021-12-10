package etcd

import "testing"

func TestGetConf(t *testing.T) {
	entries := []*CollectEntry{
		{
			Path:  "aa",
			Topic: "bb",
		},
	}

	err := Init([]string{"127.0.0.1:2379"})
	if err != nil {
		t.Fatalf("%v\n", err)
	}
	err = PutConf("aa", entries)
	if err != nil {
		t.Fatalf("%v\n", err)
	}
	conf, err := GetConf("aa")
	if err != nil {
		t.Fatalf("%v\n", err)
	}
	for _, ele := range conf {
		t.Log(ele.Path, ele.Topic)
	}
}
