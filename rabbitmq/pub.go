package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"reflect"
)

type JsonMsg struct {
	*MqGroup
	ExchangeName string
	Group        int32
	MsgType      reflect.Type
}

func NewJsonMsg(rabbit *MqGroup, exchangeName string, msgType interface{}) *JsonMsg {
	p := &JsonMsg{
		MqGroup:      rabbit,
		ExchangeName: exchangeName,
	}
	t := reflect.TypeOf(msgType)
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		panic("invalid type, expect struct")
	}
	p.MsgType = t
	return p
}

func (j *JsonMsg) Pub(ctx *context.Context, req *PubReq, msg interface{}) error {
	// 检查msg的类型是否与预期的MsgType一致
	if reflect.TypeOf(msg) != j.MsgType {
		return fmt.Errorf("invalid message type, expected %v", j.MsgType)
	}
	ch, err := j.getNode(req.Group).Channel(req.RouterKey)
	if err != nil {
		return err
	}
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	err = ch.PublishWithContext(*ctx, j.ExchangeName, req.RouterKey, false, false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        data,
			MessageId:   req.MsgId,
		})
	if err != nil {
		return err
	}
	return nil
}
