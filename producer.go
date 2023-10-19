package async_queue

import (
	"context"
	"github.com/redis/go-redis/v9"
	"time"
)

type AsyncProducer struct {
}

func AsyncProducerClient() AsyncProducer {
	return AsyncProducer{}
}

// AddDelayMessage 压入延迟任务
func (l AsyncProducer) AddDelayMessage(ctx context.Context, delay time.Duration, message *Message) error {
	return l.AddBatchDelayMessageQueue(ctx, AsyncClient.GetDelayListName(), delay, []*Message{message})
}

// AddBatchDelayMessage 批量压入延迟任务
func (l AsyncProducer) AddBatchDelayMessage(ctx context.Context, delay time.Duration, messages []*Message) error {
	return l.AddBatchDelayMessageQueue(ctx, AsyncClient.GetDelayListName(), delay, messages)
}

func (l AsyncProducer) AddBatchDelayMessageQueue(ctx context.Context, queueName string, delay time.Duration, messages []*Message) error {
	if delay.Seconds() <= 0 {
		return l.AddBatchMessage(ctx, messages)
	}
	var now = time.Now().Add(delay)
	var redisSlcie []redis.Z
	for _, message := range messages {
		var messageStr, messageErr = message.DelayString()
		if messageErr != nil {
			return messageErr
		}
		redisSlcie = append(redisSlcie, redis.Z{
			Score:  float64(now.Unix()),
			Member: messageStr,
		})
	}
	return AsyncClient.Config().Client.ZAdd(ctx, queueName, redisSlcie...).Err()
}

// AddBatchMessage 批量压入任务
func (l AsyncProducer) AddBatchMessage(ctx context.Context, messages []*Message) error {
	var redisMap = make(map[string][]interface{})
	for _, message := range messages {
		var messageStr, messageErr = message.String()
		if messageErr != nil {
			return messageErr
		}
		redisMap[message.GetQueueName()] = append(redisMap[message.GetQueueName()], messageStr)
	}
	client := AsyncClient.Config().Client
	_, err := client.Pipelined(ctx, func(pipeliner redis.Pipeliner) error {
		for queueName, valueList := range redisMap {
			pipeliner.RPush(ctx, queueName, valueList...)
		}
		return nil
	})
	return err
}
