package es

import (
	"context"
	"github.com/olivere/elastic/v7"
	log "github.com/sirupsen/logrus"
)

type Service interface {
	Put(index string, msgCh <-chan map[string]interface{})
}

type service struct {
	client *elastic.Client
}

func (s *service) Put(index string, messages <-chan map[string]interface{}) {
	for msg := range messages {
		resp, err := s.client.Index().Index(index).BodyJson(msg).Do(context.Background())
		if err != nil {
			log.Fatalf("error of write into es, err: %v\n", err)
		}
		log.Debugf("write into es: %v, resp: %v\n", msg, resp)
	}
}

func New(addrs ...string) Service {
	client, err := elastic.NewClient(elastic.SetURL(addrs...), elastic.SetSniff(false))
	if err != nil {
		log.Fatalf("error of create es client, addrs: %v, err: %v\n", addrs, err)
	}

	return &service{client: client}
}
