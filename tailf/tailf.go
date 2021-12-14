package tailf

import (
	"fmt"
	"github.com/hpcloud/tail"
	perrors "github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"time"
	"xiaoyureed.github.io/log_collection/etcd"
)

type Service struct {
	tailTasks []*tailTask
}

// tailTask represent a log collecting item
type tailTask struct {
	Topic string
	Tail  *tail.Tail
}

func (t Service) TailTasks() []*tailTask {
	return t.tailTasks
}

func NewService(entries []*etcd.CollectEntry) (*Service, error) {
	tasks := make([]*tailTask, len(entries))

	for key, en := range entries {
		file, err := tail.TailFile(en.Path, tail.Config{
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
			return nil, perrors.Wrapf(err, "error of tail file, path: %v", en.Path)
		}
		tasks[key] = &tailTask{
			Topic: en.Topic,
			Tail:  file,
		}
	}

	log.Infof(">>>tailf new tail service ok")

	return &Service{
		tailTasks: tasks,
	}, nil
}

func readLog(path string) {
	fileTail, err := tail.TailFile(path, tail.Config{
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
		fmt.Printf("error of tail file %s: %v\n", path, err)
		return
	}
	for {
		line, ok := <-fileTail.Lines
		if !ok {
			fmt.Printf("tail file closed, filename: %v\n", fileTail.Filename)
			time.Sleep(time.Second)
			continue
		}
		fmt.Println("msg: ", line.Text)
	}
}
