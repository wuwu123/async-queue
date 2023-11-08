package async_queue

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"testing"
	"time"
)

type AsyncLog struct {
}

func (l AsyncLog) Info(v string) {
	fmt.Println("这是一个Info日志：", v)
}
func (l AsyncLog) Error(v error) {
	fmt.Println("这是一个Error日志：", v)
}
func initClient() {
	client := redis.NewClient(&redis.Options{
		Addr:     "r-2zeks3bi66byn1a1ntpd.redis.rds.aliyuncs.com:65379",
		Password: "xTLEq^85&7Cp9pRc",
		DB:       2,
	})
	_ = InitAsyncScript(Config{
		QueueNamePrefix: "test_v1_",
		Client:          client,
		NotLua:          true,
	})
	AsyncClient.SetLog(AsyncLog{})
}

func TestAddDelayMessage(t *testing.T) {
	initClient()
	AsyncClient.Producer.AddDelayMessage(context.Background(), 3*time.Second, NewMessage(
		"queue",
		Payload("1234"),
	))
	AsyncClient.Consumer.Add(AsyncConsumerTask{
		TaskName: "queue",
		TaskDesc: "测试",
		Process: func(message Message) error {
			fmt.Println("消费时间", time.Now().String(), "数据时间", message.CreateTime.String(), "时间间隔", fmt.Sprintf("[%s]", time.Since(message.CreateTime)), message.Payload.String())
			return nil
		},
	})
	go func() {
		AsyncClient.Consumer.Start()
	}()
	time.Sleep(5 * time.Second)
}

// 测试循环执行的任务
func TestAddDelaysMessage(t *testing.T) {
	initClient()
	var message = NewMessage(
		"queue",
		Payload("1234"),
	)
	//message.RunRate = []time.Duration{3 * time.Second, 3 * time.Second}
	AsyncClient.Producer.AddDelayMessage(context.Background(), 4*time.Second, message)
	AsyncClient.Consumer.Add(AsyncConsumerTask{
		TaskName: "queue",
		TaskDesc: "测试",
		Process: func(message Message) error {
			fmt.Println("消费时间", time.Now().String(), "数据时间", message.CreateTime.String(), "时间间隔", fmt.Sprintf("[%s]", time.Since(message.CreateTime)), message.Payload.String())
			return nil
		},
	})
	go func() {
		AsyncClient.Consumer.Start()
	}()
	time.Sleep(10 * time.Second)
}

// 测试需要应答的消息
func TestAddAckMessage(t *testing.T) {
	initClient()
	var message = NewMessage(
		"queue",
		Payload("1234"),
	)
	message.WithAck(true, 2*time.Second)
	AsyncClient.Producer.AddDelayMessage(context.Background(), 1*time.Second, message)
	AsyncClient.Consumer.Add(AsyncConsumerTask{
		TaskName:   "queue",
		TaskDesc:   "测试",
		MaxProcess: 10,
		Process: func(message Message) error {
			fmt.Println("时间间隔", fmt.Sprintf("[%s]", time.Since(message.CreateTime)), "重试次数", message.RunTimes)
			if message.RunTimes == 3 {
				message.Ack()
			}
			time.Sleep(3 * time.Second)
			return nil
		},
	})
	go func() {
		AsyncClient.Consumer.Start()
	}()
	time.Sleep(20 * time.Second)
}

// 测试中途压消息
func TestAddChMessage(t *testing.T) {
	initClient()
	go func() {
		for {
			var message = NewMessage(
				"queue",
				Payload("1234"),
			)
			AsyncClient.Producer.AddBatchMessage(context.Background(), []*Message{message})
			time.Sleep(1 * time.Second)
		}

	}()

	go func() {
		AsyncClient.Consumer.Start()
	}()
	go func() {
		time.AfterFunc(5*time.Second, func() {
			AsyncClient.Consumer.Add(AsyncConsumerTask{
				TaskName:   "queue",
				TaskDesc:   "测试",
				MaxProcess: 10,
				Process: func(message Message) error {
					fmt.Println("消费时间", time.Now().String(), "时间间隔", fmt.Sprintf("[%s]", time.Since(message.CreateTime)), "重试次数", message.RunTimes)
					return nil
				},
			})
		})
	}()

	go func() {
		time.AfterFunc(7*time.Second, func() {
			fmt.Println("脚本结束不在执行等待相应")
			AsyncClient.Stop()
			fmt.Println("脚本stop")
		})
	}()
	fmt.Println("消费时间", time.Now().String())
	time.Sleep(20 * time.Second)
}
