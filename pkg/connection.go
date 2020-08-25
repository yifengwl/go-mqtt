package pkg

import (
	mq "github.com/huin/mqtt"
	"github.com/rs/zerolog/log"
	"net"
	"sync"
)

type job struct {
	msg mq.Message
	r   receipt
}
type Connection struct {
	con      net.Conn
	jobs     chan job
	clientId string
	topics   map[string]bool
	srv      *Service

	/*Qos2 级别消息采用存储报文标识符立即开始向前分发消息的方式，
	客户端重连且CleanSession为0时不需要采取行动
	读取均在reader线程，故不需要加锁*/
	received map[uint16]bool //

	//send 操作需要加锁
	mu   sync.Mutex
	//客户端重连且CleanSession为0时需要重新发送。
	send map[uint16]*mq.Publish
}

func (c *Connection) Run() {
	go c.Reader()
	go c.Writer()
}

func (c *Connection) Reader() {
	defer func() {
		c.del()
		c.srv.sub.unSubAll(c)
		c.con.Close()
		close(c.jobs)
	}()
	for ; ; {
		msg, err := mq.DecodeOneMessage(c.con, nil)
		if err != nil {
			break
		}
		switch msg := msg.(type) {
		case *mq.Connect:
			rc := mq.RetCodeAccepted
			if msg.ProtocolName != "MQTT" {
				rc = mq.RetCodeUnacceptableProtocolVersion
			}
			if len(msg.ClientId) < 1 && len(msg.ClientId) > 23 {
				rc = mq.RetCodeIdentifierRejected
			}
			c.clientId = msg.ClientId
			if existing := c.add(); existing != nil {
				disconnect := &mq.Disconnect{}
				receipt := existing.submitSync(disconnect)
				receipt.wait()
				c.add()
			}
			ack := &mq.ConnAck{
				ReturnCode: rc,
			}
			c.submit(ack)
		case *mq.Subscribe:
			ack := &mq.SubAck{
				Header: mq.Header{
					QosLevel: mq.QosAtMostOnce,
				},
				MessageId: msg.MessageId,
			}
			for i := 0; i < len(msg.Topics); i++ {
				ack.TopicsQos = append(ack.TopicsQos, mq.QosAtMostOnce)
				//维护当前链接所订阅的话题，链接断开时全部取消订阅
				c.addSub(msg.Topics[i].Topic)
				c.srv.sub.add(c, msg.Topics[i].Topic)
			}
			c.submit(ack)
			//订阅完成后发送保留消息
			for i := 0; i < len(msg.Topics); i++ {
				c.srv.sub.sendRetain(c, msg.Topics[i].Topic)
			}

		case *mq.Unsubscribe:
			ack := &mq.UnsubAck{
				Header: mq.Header{
					QosLevel: mq.QosAtMostOnce,
				},
				MessageId: msg.MessageId,
			}
			//取消订阅话题
			for i := 0; i < len(msg.Topics); i++ {
				c.unSub(msg.Topics[i])
				c.srv.sub.unSub(c, msg.Topics[i])
			}
			c.submit(ack)
		case *mq.PingReq:
			ack := &mq.PingResp{
				Header: mq.Header{
					QosLevel: mq.QosAtMostOnce,
				},
			}
			c.submit(ack)
		case *mq.Publish:
			if !msg.QosLevel.IsValid() {
				return
			}
			//Qos 0 级别消息
			if msg.QosLevel == mq.QosAtMostOnce {
				c.srv.sub.submit(c, msg)
				continue
			}
			//Qos 1 级别消息
			if msg.QosLevel == mq.QosAtLeastOnce {
				c.srv.sub.submit(c, msg)
				ack := &mq.PubAck{
					Header: mq.Header{
						QosLevel: mq.QosAtLeastOnce,
					},
					MessageId: msg.MessageId,
				}
				c.submit(ack)
				continue
			}
			//Qos 2 级别消息
			if msg.QosLevel == mq.QosExactlyOnce {
				//回应PubRec 消息
				ack := &mq.PubRec{
					Header: mq.Header{
						QosLevel: mq.QosExactlyOnce,
					},
					MessageId: msg.MessageId,
				}
				c.submit(ack)

				//判断是否已收到过该消息id的消息，已收到不允许重复分发
				_, ok := c.received[msg.MessageId]
				if !ok {
					c.srv.sub.submit(c, msg)
					c.received[msg.MessageId] = true
				}
				continue
			}
		case *mq.PubRel:
			//丢弃消息id
			delete(c.received, msg.MessageId)
			ack := &mq.PubComp{
				Header: mq.Header{
					QosLevel: mq.QosExactlyOnce,
				},
				MessageId: msg.MessageId,
			}
			c.submit(ack)
		case *mq.PubAck:
			//分发的Qos 1 消息返回
			c.mu.Lock()
			delete(c.send, msg.MessageId)
			c.mu.Unlock()
		case *mq.PubRec:
			//Qos 2代理端分发至其他客户端 step 1
			ack := &mq.PubRel{
				Header: mq.Header{
					QosLevel: mq.QosExactlyOnce,
				},
				MessageId: msg.MessageId,
			}
			c.submit(ack)
		case *mq.PubComp:
			//Qos 2代理端分发至其他客户端 step 2
			c.mu.Lock()
			delete(c.send, msg.MessageId)
			c.mu.Unlock()
		case *mq.Disconnect:
			return
		}
	}

}

func (c *Connection) Writer() {
	defer func() {
		c.con.Close()
		log.Printf("client %s : disconnected", c.clientId)
	}()
	for job := range c.jobs {
		switch msg := job.msg.(type) {
		case *mq.Publish:
			//存储等待回应的消息 客户端CleanSession为0 重连时需要发送未确认的 publish报文
			if msg.QosLevel == mq.QosAtLeastOnce || msg.QosLevel == mq.QosExactlyOnce {
				c.mu.Lock()
				c.send[msg.MessageId] = msg
				c.mu.Unlock()
			}
		}
		err := job.msg.Encode(c.con)

		//通知同步执行的函数
		if (job.r != nil) {
			close(job.r)
		}
		if err != nil {
			log.Printf("writer %v : disconnected", err)
			return
		}
		if _, ok := job.msg.(*mq.Disconnect); ok {
			log.Print("writer: sent disconnect message")
			return
		}
		if _, ok := job.msg.(*mq.Disconnect); ok {
			log.Printf("client %s : disconnected", c.clientId)
			return
		}
	}
}

//提交job Writer去执行
func (c *Connection) submit(msg mq.Message) {
	//本函数会同时在reader subscription.run 中执行；
	//subscription.run中会存在reader中已将c.jobs channel关闭 仍然写入的情况，故recover()
	defer func() {
		if err := recover(); err != nil {
			log.Printf("connection %s channel closed :%v", c.clientId, err)
		}
	}()
	j := job{msg: msg}
	select {
	case c.jobs <- j:
	default:
		log.Print(c, ":faied to submit")
	}
}

type receipt chan struct{}

func (r receipt) wait() {
	<-r
}
func (c *Connection) submitSync(msg mq.Message) (receipt) {
	j := job{
		msg: msg,
		r:   make(receipt),
	}
	c.jobs <- j
	return j.r
}

//add
func (c *Connection) add() (*Connection) {
	clientsMu.Lock()
	defer clientsMu.Unlock()
	existing, ok := clients[c.clientId]
	if ok {
		return existing
	}
	clients[c.clientId] = c
	return nil
}

func (c *Connection) del() {
	clientsMu.Lock()
	defer clientsMu.Unlock()
	delete(clients, c.clientId)
}

func (c *Connection) addSub(topic string) {
	c.topics[topic] = true
}

func (c *Connection) unSub(topic string) {
	delete(c.topics, topic)
}
