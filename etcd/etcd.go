package etcd

import (
	"context"
	"encoding/json"
	perrors "github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	clientv3 "go.etcd.io/etcd/client/v3"
	"time"
)

type Service struct {
	client *clientv3.Client
}

// NewService connect etcd with the given endpoints
func NewService(endpoints []string) *Service {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: time.Second * 5,
	})
	if err != nil {
		log.Fatalf(">>>etcd, error of new service: %v\n", err)
	}
	//defer client.Close()

	log.Infoln(">>>etcd, new etcd service ok")
	return &Service{client: client}
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
	Path string `json:"path"`
	// Topic 表示一条业务线
	Topic string `json:"topic"`
}

//获取配置 同时启动一个goroutine 监控变化
func (s Service) GetCollectEntries(key string) ([]*CollectEntry, <-chan []*CollectEntry) {
	timeout, cancelFunc := context.WithTimeout(context.Background(), time.Second*2)
	defer cancelFunc()
	getResp, err := s.client.Get(timeout, key)
	if err != nil {
		log.Fatalf(">>>etcd, error of get key, key: %v, err: %v", key, err)
	}

	if len(getResp.Kvs) == 0 {
		log.Fatalf(">>>etcd, key not found, key: %v", key)
	}

	kv := getResp.Kvs[0]

	entries, err := toEntries(kv.Value)
	if err != nil {
		log.Fatalf("error of get entries, err: %v", err)
	}

	log.Infof(">>>etcd, get entries ok:")
	for i, ele := range entries {
		log.Infof("%v - %+v", i, ele)
	}

	watchCollectEntries := s.watchCollectEntries(key)

	return entries, watchCollectEntries
}

func toEntries(value []byte) ([]*CollectEntry, error) {
	var collectEntries []*CollectEntry
	err := json.Unmarshal(value, &collectEntries)
	if err != nil {
		return nil, perrors.Wrapf(err, "error of unmarshal to entries: %s", value)
	}
	log.Debugf(">>>etcd, parse entries ok")
	return collectEntries, nil
}

//启动一个协程, 监听变化
func (s Service) watchCollectEntries(key string) <-chan []*CollectEntry {
	watchEntries := make(chan []*CollectEntry)

	cancelCon, _ := context.WithCancel(context.Background())
	go func() {
		watch := s.client.Watch(cancelCon, key)
		// The code blocks here until the listening configuration changes
		for resp := range watch {
			for _, ele := range resp.Events {
				log.Debugf(">>>etcd collect log config changed, event type: %v, value: %s", ele.Type.String(), ele.Kv.Value)

				entries, err := toEntries(ele.Kv.Value)
				if err != nil {
					log.Errorf("%v", err)
					continue
				}

				watchEntries <- entries
			}
		}
	}()

	return watchEntries
}
