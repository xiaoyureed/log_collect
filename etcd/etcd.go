package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	perrors "github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	clientv3 "go.etcd.io/etcd/client/v3"
	"time"
)

var client *clientv3.Client

// Init connect etcd with the given endpoints
func Init(endpoints []string) error {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: time.Second * 5,
	})
	if err != nil {
		//log.Fatalf(">>> %v\n", err)
		return perrors.Wrapf(err, "error of new etcd client v3, endpoints: %v", endpoints)
	}
	client = cli

	//defer client.Close()

	log.Println(">>> connect etcd ok")
	return nil
}

// PutConf put key value pair into etcd
func PutConf(key string, entries []*CollectEntry) error {
	value, err := json.Marshal(entries)
	if err != nil {
		return perrors.Wrapf(err, "error of marshal obj to json string, entries: %v", entries)
	}

	timeout, cancelFunc := context.WithTimeout(context.Background(), time.Second*2)
	defer cancelFunc()
	_, err = client.Put(timeout, key, string(value))
	if err != nil {
		return perrors.Wrapf(err, "error of put, key: %s, value: %s", key, value)
	}
	return nil
}

// CollectEntry 表示日志收集项
type CollectEntry struct {
	// Path 表示日志文件路径
	Path  string `json:"path"`
	// Topic 表示一条业务线
	Topic string `json:"topic"`
}

// KeyNotFoundError represent an error that cannot value from etcd by the specified key
type KeyNotFoundError string

func (e KeyNotFoundError) Error() string {
	return string(e)
}

// GetConf get config from etcd
func GetConf(key string) ([]*CollectEntry, error) {
	timeout, cancelFunc := context.WithTimeout(context.Background(), time.Second*2)
	defer cancelFunc()
	getResp, err := client.Get(timeout, key)
	if err != nil {
		return nil, perrors.Wrapf(err, "error of get key '%v'", key)
	}

	if len(getResp.Kvs) == 0 {
		return nil, KeyNotFoundError(fmt.Sprintf("key not found: %s", key))
	}

	kv := getResp.Kvs[0]
	log.Debugf(">>> GetConf(%s): %s\n", kv.Key, kv.Value)

	var collectEntries []*CollectEntry
	err = json.Unmarshal(kv.Value, &collectEntries)
	if err != nil {
		return nil, perrors.Wrapf(err, "error of parse json string: %s", kv.Value)
	}
	return collectEntries, nil
}
