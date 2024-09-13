package rabbitmq

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"sync"
)

type MqGroup struct {
	nodeList []*MqNode
	Mu       *sync.RWMutex
}

func New() (*MqGroup, error) {
	Mu := &sync.RWMutex{}
	rabbitmq := &MqGroup{
		nodeList: make([]*MqNode, 0),
		Mu:       Mu,
	}
	return rabbitmq, nil
}

func (g *MqGroup) AddNode(node *MqNode) {
	g.Mu.RLock()
	defer g.Mu.RUnlock()
	g.nodeList = append(g.nodeList, node)
}

func (g *MqGroup) GetNode(group int32) *MqNode {
	g.Mu.Lock()
	defer g.Mu.Unlock()
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
	config NodeConfig
	conn   *amqp.Connection
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
func (cm *MqNode) Channel() (*amqp.Channel, error) {
	return cm.conn.Channel()
}
