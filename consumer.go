package async_queue

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"github.com/sourcegraph/conc/pool"
	"sync"
	"time"
)

// 将有序集合中指定区间的元素移动到列表中，删除有序集合的元素，返回符合的元素的数量
var moveScript = redis.NewScript(`
local zsetName = KEYS[1]
local startIdx = tonumber(ARGV[1])
local endIdx = tonumber(ARGV[2])

local zsetValues = redis.call('ZRANGEBYSCORE', zsetName, startIdx, endIdx)

for _, value in ipairs(zsetValues) do
    local separatorIndex = string.find(value, "||") 
    if separatorIndex then
        local queueName = string.sub(value, 1, separatorIndex - 1)
        if queueName then
            local valueWithoutPrefix = string.sub(value, separatorIndex + 2)
            redis.call('RPUSH', queueName, valueWithoutPrefix)
        end
    end
end
redis.call('ZREMRANGEBYSCORE', zsetName, startIdx, endIdx)

return #zsetValues
`)

type FuncJob func(message Message) error

func (f FuncJob) Run(message Message) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("%s脚本执行失败，消息体%s ， Error：%v", message.Queue, message.Payload.String(), p)
			AsyncClient.WriteErr(err)
		}
	}()
	err = f(message)
	return
}

type AsyncConsumerTask struct {
	TaskName   string
	TaskDesc   string
	Process    FuncJob
	MaxProcess int //消费者数量
}

func (l AsyncConsumerTask) QueName() string {
	return AsyncClient.GetQueueName(l.TaskName)
}

type AsyncConsumer struct {
	job          []AsyncConsumerTask
	syncInterval time.Duration
	runOnce      sync.Once
	running      bool
	add          chan AsyncConsumerTask
	stop         chan struct{}
}

var asyncConsumerClient *AsyncConsumer
var asyncConsumerOnce sync.Once

func AsyncConsumerClient() *AsyncConsumer {
	asyncConsumerOnce.Do(func() {
		asyncConsumerClient = &AsyncConsumer{
			syncInterval: 2 * time.Second,
			add:          make(chan AsyncConsumerTask),
			stop:         make(chan struct{}),
		}
	})
	return asyncConsumerClient
}

// SetSyncInterval 设置当Redis队列为空是，阻塞时间
func (l *AsyncConsumer) SetSyncInterval(syncInterval time.Duration) *AsyncConsumer {
	if syncInterval.Seconds() > 0 {
		l.syncInterval = syncInterval
	}
	return l
}

func (l *AsyncConsumer) Add(task AsyncConsumerTask) {
	if task.Process == nil {
		panic("任务的函数不存在")
	}
	if task.TaskName == "" {
		panic("任务的监听的队列未设置")
	}
	if task.MaxProcess <= 0 {
		task.MaxProcess = 1
	}
	l.job = append(l.job, task)
	if l.running {
		l.add <- task
	}
}

func (l *AsyncConsumer) delayListToReadyList(delayListName string) {
	for AsyncClient.IsRuning() {
		duration, _ := time.ParseDuration("-500ms")
		var now = time.Now().Add(duration)
		var moveErr = moveScript.Run(context.Background(), AsyncClient.Config().Client, []string{
			delayListName,
		}, 0, now.Unix()).Err()
		if moveErr != nil {
			AsyncClient.WriteErr(fmt.Errorf("redis从延迟队列迁移数据错误：%e", moveErr))
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func (l *AsyncConsumer) asyncJob() {
	AsyncClient.asyncWg.Go(func() {
		l.delayListToReadyList(AsyncClient.GetDelayListName())
	})
	AsyncClient.asyncWg.Go(func() {
		l.delayListToReadyList(AsyncClient.GetWaitAckListName())
	})
	AsyncClient.asyncWg.Go(func() {
		for AsyncClient.IsRuning() {
			select {
			case <-time.After(30 * time.Minute):
				fmt.Printf("阻塞等待")
			case <-l.stop:
				fmt.Printf("消费执行结束")
			case newJob := <-l.add:
				l.runJob(newJob)
			}
		}
	})
}

func (l *AsyncConsumer) Start() {
	l.runOnce.Do(func() {
		l.running = true
		l.asyncJob()
		l.runJobs()
	})
}

func (l *AsyncConsumer) runJobs() {
	for _, job := range l.job {
		var jobRow = job
		l.runJob(jobRow)
	}
}

func (l *AsyncConsumer) runJob(jobRow AsyncConsumerTask) {
	AsyncClient.asyncWg.Go(func() {
		var maxProcess = jobRow.MaxProcess
		var wg = pool.New().WithMaxGoroutines(maxProcess)
		for i := 0; i < maxProcess; i++ {
			wg.Go(func() {
				for AsyncClient.IsRuning() {
					var bRPop = AsyncClient.Config().Client.BRPop(context.Background(), l.syncInterval, jobRow.QueName())
					var vList = bRPop.Val()
					for _, row := range vList {
						if row == jobRow.QueName() {
							continue
						}
						var message *Message
						err := UnmarshalFromString(row, &message)
						if err != nil {
							AsyncClient.WriteErr(fmt.Errorf("redis数据解析错误：%e", err))
							continue
						}
						message.BeforeConsumer()
						processErr := jobRow.Process.Run(*message)
						if processErr != nil {
							AsyncClient.WriteErr(fmt.Errorf("%s队列第%d次执行失败,Error: %e", message.Queue, message.RunTimes, processErr))
						}
						message.AfterConsumer()
					}
					time.Sleep(500 * time.Millisecond)
				}
			})
		}
		wg.Wait()
	})
}
