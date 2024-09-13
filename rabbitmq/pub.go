package rabbitmq

import (
	"context"
	"encoding/json"
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
	ch, err := j.GetNode(req.Group).Channel()
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
		})
	if err != nil {
		return err
	}
	return nil
}
