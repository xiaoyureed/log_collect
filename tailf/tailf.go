package tailf

import (
	"fmt"
	"github.com/hpcloud/tail"
	perrors "github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"time"
)

var TailObj *tail.Tail

func Init(filename string) error {
	fileTail, err := tail.TailFile(filename, tail.Config{
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
		return perrors.Wrap(err, fmt.Sprintf("error of tail -f log file: %v", filename))
	}

	TailObj = fileTail
	log.Infof("init tailf ok, filename: %v\n", filename)

	return nil
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
