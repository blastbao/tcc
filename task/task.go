package task

import (
	"context"
	"github.com/blastbao/tcc/constant"
	"github.com/blastbao/tcc/global/config"
	"github.com/blastbao/tcc/global/various"
	"github.com/blastbao/tcc/log"
	"github.com/blastbao/tcc/store/data"
	"github.com/blastbao/tcc/store/lock"
	"time"
)

type Task struct {
	Interval time.Duration // unit: second
	Off      chan bool     // 控制停止任务
	off      bool          // 标记当前任务是否已经停止
	F        func()
}

func (ts *Task) Start() {
	if ts.off {
		ts.off = false
		go ts.exec()
	}
}

func (ts *Task) Stop() {
	ts.off = true
	ts.Off <- true
}

func (ts *Task) exec() {
	// 定时器
	t := time.NewTicker(time.Second * ts.Interval)
FOR:
	for {
		select {
		case <-t.C:
			go ts.F()
		case off := <-ts.Off:
			if off {
				break FOR
			}
		}
	}
}

var defaultTask = &Task{
	Interval: time.Duration(*config.TimerInterval),
	Off:      make(chan bool, 1),
	F:        retryAndSend,
}

func Start() {
	defaultTask.Start()
}

func Stop() {
	defaultTask.Stop()
}

func retryAndSend() {


	ctx := context.Background()
	l, err := lock.NewEtcdLock(ctx, *config.TimerInterval, constant.LockEtcdPrefix)
	if err != nil {
		log.Warnf("cannot execute task because of the lock is got failed, error info is: %s", err)
		return
	}

	err = l.Lock(ctx)
	if err != nil {
		log.Warnf("cannot execute task because of the locker is not locked, error info is: %s", err)
		return
	}
	defer l.Unlock(ctx)


	data := getBaseData()
	if len(data) == 0 {
		return
	}

	//
	go taskToRetry(data)

	// 不断失败的事务，需要发送 email 告警
	go taskToSend(data, "there are some exceptional data, please fix it soon")
}

func getBaseData() []*data.RequestInfo {

	// 查找所有异常事务（状态为：2(提交失败) 和 4(回滚失败)）
	needRollbackData, err := various.C.ListExceptionalRequestInfo()
	if err != nil {
		log.Errorf("the data that required for the task is failed to load, please check it. error information: %s", err)
		return nil
	}

	return needRollbackData
}
