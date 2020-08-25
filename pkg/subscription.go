package pkg

import (
	"github.com/huin/mqtt"
	"github.com/rs/zerolog/log"
	"sync"
)

type post struct {
	c   *Connection
	pub *mqtt.Publish
}
type retain struct {
	pub *mqtt.Publish
}
type Subscription struct {
	worker int
	posts  chan post
	mu     sync.Mutex
	sub    map[string]map[*Connection]bool
	ratain map[string]retain //保留消息
}

func NewSubscription(worker int) (sub *Subscription) {
	sub = &Subscription{
		worker: worker,
		posts:  make(chan post, 20),
		sub:    make(map[string]map[*Connection]bool),
		ratain: make(map[string]retain),
	}
	for i := 0; i < worker; i++ {
		go sub.Run(i)
	}
	return sub
}

func (s *Subscription) Run(i int) {
	log.Printf("process %d started", i)
	defer func() {
		log.Printf("process %d ended", i)
	}()
	for post := range s.posts {
		retain := post.pub.Retain
		post.pub.Retain = false

		//publish为保留消息但是 Payload为空，视为清空该话题下的保留消息
		if retain && post.pub.Payload.Size() == 0 {
			s.mu.Lock()
			delete(s.ratain, post.pub.TopicName)
			s.mu.Unlock()
			return
		}

		conns := s.getTopicConns(post.pub.TopicName)
		for _, con := range conns {
			con.submit(post.pub)
		}
		//保留消息
		if retain {
			s.addRetain(post.pub)
		}

	}
}

//获取订阅话题下所有链接
func (s *Subscription) getTopicConns(topic string) (c []*Connection) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c = make([]*Connection, 0, len(s.sub[topic]))
	for k, _ := range s.sub[topic] {
		c = append(c, k)
	}
	return
}

//链接增加订阅
func (s *Subscription) add(c *Connection, topic string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.sub[topic] == nil {
		s.sub[topic] = make(map[*Connection]bool)
	}
	s.sub[topic][c] = true
}

//取消链接单个订阅
func (s *Subscription) unSub(c *Connection, topic string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.sub[topic] == nil {
		return
	}
	delete(s.sub[topic], c)
}

//取消链接所有订阅
func (s *Subscription) unSubAll(c *Connection) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for k, _ := range c.topics {
		if s.sub[k] == nil {
			continue
		}
		delete(s.sub[k], c)
	}
	//清空连接所记录的所以topic
	c.topics = make(map[string]bool)
}

func (s *Subscription) submit(c *Connection, pub *mqtt.Publish) {
	s.posts <- post{c: c, pub: pub,}
}

func (s *Subscription) addRetain(pub *mqtt.Publish) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ratain[pub.TopicName] = retain{pub: pub}
}

func (s *Subscription) sendRetain(c *Connection, topic string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	retain, ok := s.ratain[topic]
	if ok {
		retain.pub.Retain = true
		c.submit(retain.pub)
	}
}
