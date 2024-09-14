package rabbitmq

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"sync"
)

type MqGroup struct {
	nodeList []*MqNode
	mu       *sync.RWMutex
}

func New() (*MqGroup, error) {
	Mu := &sync.RWMutex{}
	rabbitmq := &MqGroup{
		nodeList: make([]*MqNode, 0),
		mu:       Mu,
	}
	return rabbitmq, nil
}

func (g *MqGroup) AddNode(node *MqNode) {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.nodeList = append(g.nodeList, node)
}

func (g *MqGroup) GetNode(group int32) *MqNode {
	g.mu.RLock()
	defer g.mu.RUnlock()
	if group >= int32(len(g.nodeList)) {
		return nil
	}
	return g.nodeList[group]
}

// NodeConfig 定义了 RabbitMQ 的配置信息
type NodeConfig struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	User     string `json:"user"`
	Password string `json:"password"`
	VHost    string `json:"v_host"`
}

// MqNode 负责管理 RabbitMQ 的连接
type MqNode struct {
	config  NodeConfig
	conn    *amqp.Connection
	channel sync.Map
}

// NewRabbitMQNode 创建一个新的 ConnectionManager
func NewRabbitMQNode(config NodeConfig) (*MqNode, error) {
	cm := &MqNode{config: config}
	var err error
	cm.conn, err = amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s:%d/%s", config.User, config.Password, config.Host, config.Port, config.VHost))
	if err != nil {
		return nil, err
	}
	return cm, nil
}

// Close 关闭连接
func (cm *MqNode) Close() error {
	return cm.conn.Close()
}

// Channel 获取一个新的 Channel
func (cm *MqNode) Channel(name string) (*amqp.Channel, error) {
	// 尝试从缓存中获取已存在的通道
	if ch, ok := cm.channel.Load(name); ok {
		return ch.(*amqp.Channel), nil
	}

	// 如果不存在，则创建新的通道
	newCh, err := cm.conn.Channel()
	if err != nil {
		return nil, err
	}
	// 将新创建的通道存入缓存
	cm.channel.Store(name, newCh)
	return newCh, nil
}
