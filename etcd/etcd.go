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


type Service struct {
	client *clientv3.Client
}

// NewService connect etcd with the given endpoints
func NewService(endpoints []string) (*Service, error) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: time.Second * 5,
	})
	if err != nil {
		//log.Fatalf(">>> %v\n", err)
		return nil, perrors.Wrapf(err, "error of new etcd client v3, endpoints: %v", endpoints)
	}
	//defer client.Close()

	log.Println(">>> new etcd service ok")
	return &Service{client: client}, nil
}

// PutCollectEntries put key value pair into etcd
func (s Service) PutCollectEntries(key string, entries []*CollectEntry) error {
	value, err := json.Marshal(entries)
	if err != nil {
		return perrors.Wrapf(err, "error of marshal obj to json string, entries: %v", entries)
	}

	timeout, cancelFunc := context.WithTimeout(context.Background(), time.Second*2)
	defer cancelFunc()
	_, err = s.client.Put(timeout, key, string(value))
	if err != nil {
		return perrors.Wrapf(err, "error of put, key: %s, value: %s", key, value)
	}
	return nil
}

// CollectEntry represent a log collection item, every business line has a topic
type CollectEntry struct {
	// Path 表示日志文件路径
	Path  string `json:"path"`
	// Topic 表示一条业务线
	Topic string `json:"topic"`
}

// KeyNotFoundError represent an error that cannot get value from etcd by the specified key
type KeyNotFoundError string

func (e KeyNotFoundError) Error() string {
	return string(e)
}

func (s Service) GetCollectEntries(key string) ([]*CollectEntry, error) {
	timeout, cancelFunc := context.WithTimeout(context.Background(), time.Second*2)
	defer cancelFunc()
	getResp, err := s.client.Get(timeout, key)
	if err != nil {
		return nil, perrors.Wrapf(err, "error of get key '%v'", key)
	}

	if len(getResp.Kvs) == 0 {
		return nil, KeyNotFoundError(fmt.Sprintf("key not found: %s", key))
	}

	kv := getResp.Kvs[0]
	log.Debugf(">>>etcd get entries ok:  %s\n", kv.Value)

	var collectEntries []*CollectEntry
	err = json.Unmarshal(kv.Value, &collectEntries)
	if err != nil {
		return nil, perrors.Wrapf(err, "error of parse value: %s", kv.Value)
	}
	return collectEntries, nil
}

