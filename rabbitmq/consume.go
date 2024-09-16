package rabbitmq

import (
	"context"
	"fmt"
	"github.com/cloudwego/kitex/pkg/klog"
	"time"
)

type Consumer struct {
	*MqGroup
	Group int32
}

const (
	MqKindFanout = "fanout"
	MqKindDirect = "direct"
	MqKindTopic  = "topic"
)

type ConsumeFunc func(ctx *context.Context, req *ConsumeReq) error
type ConsumeFuncV2 func(ctx *context.Context, req *ConsumeReq) (*ConsumeResp, error)

func NewConsumer(rabbit *MqGroup, group int32) *Consumer {
	return &Consumer{rabbit, group}
}

type QueueReq struct {
	Retry         bool
	Name          string
	HandlerFunc   ConsumeFunc
	HandlerFuncV2 ConsumeFuncV2
}

type PublishReq struct {
	Retry        bool
	ExchangeName string

	HandlerFunc   ConsumeFunc
	HandlerFuncV2 ConsumeFuncV2
}

type RoutingReq struct {
	Retry        bool
	ExchangeName string
	RoutingKey   string

	HandlerFunc   ConsumeFunc
	HandlerFuncV2 ConsumeFuncV2
}

type TopicReq struct {
	Retry         bool
	ExchangeName  string
	RoutingKey    string
	HandlerFunc   ConsumeFunc
	HandlerFuncV2 ConsumeFuncV2
}

// AddQueue 工作模式
func (c *Consumer) AddQueue(ctx *context.Context, queue QueueReq) error {

	if queue.HandlerFunc == nil && queue.HandlerFuncV2 == nil {
		return fmt.Errorf("either HandlerFunc or HandlerFuncV2 must be provided")
	}

	mqNode := c.getNode(c.Group)
	if mqNode == nil {
		klog.Errorf("no node")
		return fmt.Errorf("no mq node")
	}

	ch, err := mqNode.Channel(queue.Name)
	if err != nil {
		klog.Errorf("channel error %v", err)
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
			err = queue.HandlerFunc(ctx, req)
			if err != nil {
				klog.Errorf("consume error %v", err)
				return
			}
		}
	}()
	return nil
}

// AddPublish 订阅模式
func (c *Consumer) AddPublish(ctx *context.Context, publish PublishReq) error {

	if publish.HandlerFunc == nil && publish.HandlerFuncV2 == nil {
		return fmt.Errorf("either HandlerFunc or HandlerFuncV2 must be provided")
	}

	mqNode := c.getNode(c.Group)
	if mqNode == nil {
		klog.Errorf("no node")
		return fmt.Errorf("no mq node")
	}

	ch, err := mqNode.Channel(publish.ExchangeName)
	if err != nil {
		klog.Errorf("channel error %v", err)
		return err
	}

	//1.试探性创建交换机
	err = ch.ExchangeDeclare(
		publish.ExchangeName,
		//交换机类型
		MqKindFanout,
		true,
		false,
		//YES表示这个exchange不可以被client用来推送消息，仅用来进行exchange和exchange之间的绑定
		false,
		false,
		nil,
	)
	if err != nil {
		klog.Errorf("err:%v", err)
		return err
	}

	//2.试探性创建队列，这里注意队列名称不要写
	q, err := ch.QueueDeclare(
		"", //随机生产队列名称
		false,
		false,
		true,
		false,
		nil,
	)

	if err != nil {
		klog.Errorf("err:%v", err)
		return err
	}

	//绑定队列到 exchange 中
	err = ch.QueueBind(
		q.Name,
		//在pub/sub模式下，这里的key要为空
		"",
		publish.ExchangeName,
		false,
		nil)

	if err != nil {
		klog.Errorf("err:%v", err)
		return err
	}
	//消费消息
	messages, err := ch.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		klog.Errorf("err:%v", err)
		return err
	}
	go func() {
		for m := range messages {
			req := NewConsumeReq()
			now := time.Now()
			req.Data = m.Body
			req.MsgId = m.MessageId
			req.CreatedAt = int32(now.Unix())
			err = publish.HandlerFunc(ctx, req)
			if err != nil {
				klog.Errorf("consume error %v", err)
				return
			}
		}
	}()
	return nil
}

// AddRouting 路由模式
func (c *Consumer) AddRouting(ctx *context.Context, routing RoutingReq) error {

	if routing.HandlerFunc == nil && routing.HandlerFuncV2 == nil {
		return fmt.Errorf("either HandlerFunc or HandlerFuncV2 must be provided")
	}

	mqNode := c.getNode(c.Group)
	if mqNode == nil {
		klog.Errorf("no node")
		return fmt.Errorf("no mq node")
	}

	ch, err := mqNode.Channel(routing.ExchangeName)
	if err != nil {
		klog.Errorf("channel error %v", err)
		return err
	}

	//1.试探性创建交换机
	err = ch.ExchangeDeclare(
		routing.ExchangeName,
		//交换机类型
		MqKindDirect,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		klog.Errorf("err:%v", err)
		return err
	}

	//2.试探性创建队列，这里注意队列名称不要写
	q, err := ch.QueueDeclare(
		"", //随机生产队列名称
		false,
		false,
		true,
		false,
		nil,
	)

	if err != nil {
		klog.Errorf("err:%v", err)
		return err
	}
	//绑定队列到 exchange 中
	err = ch.QueueBind(
		q.Name,
		//需要绑定key
		routing.RoutingKey,
		routing.ExchangeName,
		false,
		nil)

	if err != nil {
		klog.Errorf("err:%v", err)
		return err
	}
	//消费消息
	messages, err := ch.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		klog.Errorf("err:%v", err)
		return err
	}

	go func() {
		for m := range messages {
			req := NewConsumeReq()
			now := time.Now()
			req.Data = m.Body
			req.MsgId = m.MessageId
			req.CreatedAt = int32(now.Unix())
			err = routing.HandlerFunc(ctx, req)
			if err != nil {
				klog.Errorf("consume error %v", err)
				return
			}
		}
	}()
	return nil
}

// AddTopic 主题模式
func (c *Consumer) AddTopic(ctx *context.Context, topic TopicReq) error {

	if topic.HandlerFunc == nil && topic.HandlerFuncV2 == nil {
		return fmt.Errorf("either HandlerFunc or HandlerFuncV2 must be provided")
	}

	if topic.RoutingKey == "" {
		return fmt.Errorf("routing key must be provided")
	}

	mqNode := c.getNode(c.Group)
	if mqNode == nil {
		klog.Errorf("no node")
		return fmt.Errorf("no mq node")
	}

	ch, err := mqNode.Channel(topic.ExchangeName)
	if err != nil {
		klog.Errorf("channel error %v", err)
		return err
	}

	//1.试探性创建交换机
	err = ch.ExchangeDeclare(
		topic.ExchangeName,
		//交换机类型
		MqKindTopic,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		klog.Errorf("err:%v", err)
		return err
	}

	//2.试探性创建队列，这里注意队列名称不要写
	q, err := ch.QueueDeclare(
		"", //随机生产队列名称
		false,
		false,
		true,
		false,
		nil,
	)

	if err != nil {
		klog.Errorf("err:%v", err)
		return err
	}
	//绑定队列到 exchange 中
	err = ch.QueueBind(
		q.Name,
		//需要绑定key
		topic.RoutingKey,
		topic.ExchangeName,
		false,
		nil)

	if err != nil {
		klog.Errorf("err:%v", err)
		return err
	}
	//消费消息
	messages, err := ch.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		klog.Errorf("err:%v", err)
		return err
	}

	go func() {
		for m := range messages {
			req := NewConsumeReq()
			now := time.Now()
			req.Data = m.Body
			req.MsgId = m.MessageId
			req.CreatedAt = int32(now.Unix())
			err = topic.HandlerFunc(ctx, req)
			if err != nil {
				klog.Errorf("consume error %v", err)
				return
			}
		}
	}()
	return nil
}
