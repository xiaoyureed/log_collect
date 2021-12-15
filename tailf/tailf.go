package tailf

import (
	"github.com/hpcloud/tail"
	perrors "github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"strings"
	"time"
	"xiaoyureed.github.io/log_collection/etcd"
	"xiaoyureed.github.io/log_collection/kafka"
)

type service struct {
	nameTaskMapping map[string]*tailTask
}

// tailTask represent a log collecting item
type tailTask struct {
	Topic string
	Tail  *tail.Tail
}

//func (s service) tailTasks() []*tailTask {
//	tasks := make([]*tailTask, len(s.nameTaskMapping))
//	i := 0
//	for _, task := range s.nameTaskMapping {
//		tasks[i] = task
//		i++
//	}
//	return tasks
//}

//启动单个任务: 读取日志, 发往 kafka
func (t tailTask) start(serviceKafka *kafka.Service) {
	for {
		line, ok := <-t.Tail.Lines
		if !ok {
			log.Warnf(">>> tail file closed, sleep 1s, filename: %v\n", t.Tail.Filename)
			time.Sleep(time.Second)
			continue
		}

		// trim spaces and \r\n
		//https://stackoverflow.com/questions/44448384/how-remove-n-from-lines
		lineTrim := strings.TrimFunc(strings.TrimSpace(line.Text), func(r rune) bool {
			return r == '\r' || r == '\n'
		})
		if len(lineTrim) == 0 {
			log.Debugln(">>> empty line")
			continue
		}

		serviceKafka.Put(line.Text, t.Topic)
		log.Debugf(">>> send msg to msg chan ok: %v\n", line.Text)
	}
}

//启动所有任务
func (s service) Start(serviceKafka *kafka.Service) {
	for _, task := range s.nameTaskMapping {
		go task.start(serviceKafka)
		log.Infof(">>>tailf, tail task start ok, topic: %v, filename: %v\n", task.Topic, task.Tail.Filename)
	}
}

//将 collect entry 转换 为 tailTask
func NewService(entries []*etcd.CollectEntry) (*service, error) {
	//tasks := make([]*tailTask, len(entries))
	nameTaskMapping := make(map[string]*tailTask, len(entries))

	for _, ele := range entries {
		file, err := tail.TailFile(ele.Path, tail.Config{
			ReOpen: true, // 日志文件可能会归档成多个文件, true 表示自动跟踪日志归档文件
			Follow: true, // 等价 tail -f
			// 打开文件从哪里开始读取
			Location: &tail.SeekInfo{
				Offset: 0, Whence: 2,
			},
			MustExist: false, // 允许日志文件不存在
			Poll:      true,
		})
		if err != nil {
			return nil, perrors.Wrapf(err, "error of tail file, path: %v", ele.Path)
		}
		//tasks[key] = &tailTask{
		//	Topic: ele.Topic,
		//	Tail:  file,
		//}
		nameTaskMapping[buildTaskKey(ele)] = &tailTask{
			Topic: ele.Topic,
			Tail: file,
		}
	}

	log.Infof(">>>tailf, new tail service ok")

	return &service{
		nameTaskMapping: nameTaskMapping,
	}, nil
}

func buildTaskKey(entry *etcd.CollectEntry) string {
	return entry.Topic + "-" + entry.Path
}

//监视配置改动
//- 若有新配置项, 需要新建 task
//- 若配置项较少, 需要关掉相关 task
//- 若配置项修改, 需要更新 task
func (s service) WatchNewEntries(newEntries <-chan []*etcd.CollectEntry) {
	for entries := range newEntries {
		for _, entry := range entries {
			key := buildTaskKey(entry)
			_, ok := s.nameTaskMapping[key]
			if ok {
				continue
			}
		}
	}
}
