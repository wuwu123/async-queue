package async_queue

import (
	"context"
	"fmt"
	uuid2 "github.com/google/uuid"
	"time"
)

type Payload []byte

func (b Payload) String() string {
	return string(b)
}

type Message struct {
	//消息体
	Payload Payload `json:"payload"`
	//唯一的消息Id
	Uuid string
	//队列名字
	Queue string
	//正在第几次执行
	RunTimes int
	//是否需要应答
	NeedAck bool
	//单次程序的最大执行时间，当超过这个时间再次压入队列重新执行
	MaxRunTime time.Duration
	//最大执行次数
	MaxNum int
	//这个消息还需要消费的间隔，在上次执行结束之后添加的时间
	RunRate []time.Duration
	//创建时间
	CreateTime time.Time
}

func NewMessage(queueName string, payload Payload) *Message {
	return &Message{
		Uuid:       uuid2.NewString(),
		Payload:    payload,
		Queue:      queueName,
		CreateTime: time.Now(),
	}
}

func (m *Message) WithAck(ack bool, maxRunTime time.Duration) *Message {
	m.NeedAck = ack
	m.MaxRunTime = maxRunTime
	return m
}

func (m *Message) GetQueueName() string {
	return AsyncClient.GetQueueName(m.Queue)
}

func (m *Message) GetPayload() Payload {
	return m.Payload
}

// DelayString 延迟队列的拼接队列的名字，用于拆分任务的时候使用
func (m *Message) DelayString() (string, error) {
	var messageStr, err = m.String()
	if err != nil {
		return "", err
	}
	return m.GetQueueName() + "||" + messageStr, nil
}

// Ack 消息应答
func (m *Message) Ack() error {
	if m.NeedAck == false {
		return nil
	}
	var member, _ = m.DelayString()
	return AsyncClient.config.Client.ZRem(context.Background(), AsyncClient.GetWaitAckListName(), member).Err()
}

// 普通压入消息的结构
func (m *Message) String() (string, error) {
	return MarshalToString(m)
}

// AgainAddDelayList 再一次将消息压入队列
func (m *Message) AgainAddDelayList() {
	var nextTime = m.GetNextTime()
	if nextTime.Seconds() == 0 {
		return
	}
	err := AsyncClient.Producer.AddDelayMessage(context.Background(), nextTime, m)
	if err != nil {
		AsyncClient.WriteErr(fmt.Errorf("%s队列第%d次压入队列失败,Error: %e", m.Queue, m.RunTimes+1, err))
	}
}

func (m *Message) BeforeConsumer() {
	m.RunTimes = m.RunTimes + 1
	//需要确认，并且设置了最大执行时间的压入待确认的队列
	if m.NeedAck && m.GetMaxRunTime().Seconds() != 0 && len(m.RunRate) == 0 {
		if m.runTimesLimit() {
			err := AsyncClient.Producer.AddBatchDelayMessageQueue(context.Background(), AsyncClient.GetWaitAckListName(), m.MaxRunTime, []*Message{m})
			if err != nil {
				AsyncClient.WriteErr(fmt.Errorf("%s队列压入确认队列失败,Error: %e", m.Queue, err))
			}
		}
	}
}

func (m *Message) AfterConsumer() {
	m.AgainAddDelayList()
}

// GetNextTime 计算队列的下一次的执行时间
func (m *Message) GetNextTime() time.Duration {
	if len(m.RunRate) == 0 {
		return 0
	}
	if !m.runTimesLimit() {
		return 0
	}
	return m.RunRate[m.RunTimes-1]
}

// GetMaxRunTime 计算程序的最大超时时间
func (m *Message) GetMaxRunTime() time.Duration {
	if m.MaxRunTime.Seconds() == 0 {
		return AsyncClient.Config().RunMaxTime
	}
	return m.MaxRunTime
}

// 消息的最大运行次数限制
func (m *Message) runTimesLimit() bool {
	if m.MaxNum > 0 && m.RunTimes >= m.MaxNum {
		return false
	}
	if m.RunTimes >= AsyncClient.config.RunMaxNum {
		return false
	}
	return true
}
