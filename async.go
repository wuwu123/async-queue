package async_queue

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"github.com/sourcegraph/conc/pool"
	"sync"
	"time"
)

var AsyncClient *AsyncScript
var asyncClientOnce sync.Once

type Config struct {
	QueueNamePrefix string
	//一个任务最大执行次数，若是没有设置默认是5
	RunMaxNum int
	//单次程序的最大执行时间
	RunMaxTime time.Duration
	//redis链接
	Client *redis.Client
	//是否走lua脚本
	NotLua bool
}

type Log interface {
	Info(v string)
	Error(err error)
}

type AsyncScript struct {
	config   Config
	log      Log
	running  bool
	asyncWg  *pool.Pool
	closing  chan struct{} //结束的信号
	Consumer *AsyncConsumer
	Producer AsyncProducer
}

func InitAsyncScript(config Config) error {
	if err := config.Client.Ping(context.Background()).Err(); err != nil {
		return fmt.Errorf("redis链接失败%e", err)
	}
	asyncClientOnce.Do(func() {
		if config.RunMaxNum == 0 {
			config.RunMaxNum = 5
		}
		if config.RunMaxTime == 0 {
			config.RunMaxTime = 30 * time.Minute
		}
		AsyncClient = &AsyncScript{
			config:   config,
			running:  true,
			asyncWg:  pool.New(),
			Consumer: AsyncConsumerClient(),
			Producer: AsyncProducerClient(),
		}
	})
	return nil
}

// Stop 结束任务
func (l *AsyncScript) Stop() {
	l.running = false
	l.Consumer.stop <- struct{}{}
	l.asyncWg.Wait()
}

func (l AsyncScript) IsRuning() bool {
	return l.running
}

func (l AsyncScript) Config() Config {
	return l.config
}

func (l AsyncScript) Log() Log {
	return l.log
}

func (l *AsyncScript) SetLog(log Log) *AsyncScript {
	l.log = log
	return l
}

func (l AsyncScript) GetQueueName(queueName string) string {
	return fmt.Sprintf("{async-queue}_%s%s", l.config.QueueNamePrefix, queueName)
}

// GetDelayListName 延迟队列的名字
func (l AsyncScript) GetDelayListName() string {
	return l.GetQueueName("async:delaylist")
}

// GetWaitAckListName 等待确认的队列的名字
func (l AsyncScript) GetWaitAckListName() string {
	return l.GetQueueName("async:waitack")
}

func (l AsyncScript) WriteErr(err error) {
	if l.log != nil {
		l.log.Error(err)
	}
}

func (l AsyncScript) WriteInfo(message string) {
	if l.log != nil {
		l.log.Info(message)
	}
}
