# async-queue
使用 `redis`实现异步队列和延迟任务

## 功能
1. 延迟任务
2. 实时的异步任务
3. 提供Ack机制，若是消息在程序的最大相应时间内没有执行成功将重试
4. 支持全量的和根绝消息的重试最大次数限制
5. 每个queue都维护自己的队列，支持设置queue的并发数

## 案例

### 初始化服务
```go
type AsyncLog struct {
}

func (l AsyncLog) Info(v string) {
	fmt.Println("这是一个Info日志：", v)
}
func (l AsyncLog) Error(v error) {
	fmt.Println("这是一个Error日志：", v)
}
client := redis.NewClient(&redis.Options{
    Addr: "127.0.0.1:6379",
})
_ = InitAsyncScript(Config{
    QueueNamePrefix: "test_v1_",//队列前缀
    Client:          client,
})
AsyncClient.SetLog(AsyncLog{})

//停止服务
AsyncClient.Stop()
```

### 压入消息

#### 延迟任务
```go
//3秒后执行
AsyncClient.Producer.AddDelayMessage(context.Background(), 3*time.Second, NewMessage(
		"queue",
		[]byte("1234"),
	))
```

#### 试试任务
```go
var message = NewMessage(
"queue",
[]byte("1234"),
)
AsyncClient.Producer.AddBatchMessage(context.Background(), []*Message{message})
```

### 消息消费

```go
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
```