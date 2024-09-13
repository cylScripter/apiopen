package rabbitmq

import (
	"context"
	"github.com/cloudwego/kitex/pkg/klog"
	"time"
)

type Consumer struct {
	*MqGroup
	Group int32
}

type ConsumeFunc func(ctx *context.Context, req *ConsumeReq) error
type ConsumeFuncV2 func(ctx *context.Context, req *ConsumeReq) (*ConsumeResp, error)

func NewConsumer(rabbit *MqGroup, group int32) *Consumer {
	return &Consumer{rabbit, group}
}

type Queue struct {
	Retry         bool
	Name          string
	handlerFunc   ConsumeFunc
	handlerFuncV2 ConsumeFuncV2
}

func (c *Consumer) AddQueue(ctx *context.Context, queue Queue) error {
	ch, err := c.GetNode(c.Group).Channel()
	if err != nil {
		return err
	}
	_, err = ch.QueueDeclare(
		queue.Name,
		true,
		false,
		false,
		false,
		nil,
	)
	//2 接收消息
	consumeChan, err := ch.Consume(
		queue.Name,
		"",   //用来区分多个消费者
		true, //是否自动应答,告诉我已经消费完了
		false,
		false, //若设置为true,则表示为不能将同一个connection中发送的消息传递给这个connection中的消费者.
		false, //消费队列是否设计阻塞
		nil,
	)
	if err != nil {
		klog.Errorf("consume error %v", err)
		return err
	}
	go func() {
		for msg := range consumeChan {
			req := NewConsumeReq()
			now := time.Now()
			req.Data = msg.Body
			req.MsgId = msg.MessageId
			req.CreatedAt = int32(now.Unix())
			err = queue.handlerFunc(ctx, req)
			if err != nil {
				klog.Errorf("consume error %v", err)
				return
			}
		}
	}()
	return nil
}
