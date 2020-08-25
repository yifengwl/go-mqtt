package pkg

import (
	"fmt"
	"github.com/huin/mqtt"
	"github.com/rs/zerolog/log"
	"net"
	"runtime"
	"sync"
)

var clients map[string]*Connection
var clientsMu sync.Mutex

func init() {
	clients = make(map[string]*Connection)
}

type Service struct {
	ls   net.Listener
	Done chan struct{}
	sub  *Subscription
}

func NewService() (s *Service) {
	l, err := net.Listen("tcp", ":8888")
	if err != nil {
		log.Print("listen: ", err)
		return
	}
	s = &Service{
		ls:  l,
		sub: NewSubscription(runtime.GOMAXPROCS(0)),
	}
	return s
}

func (s *Service) NewConnection(c net.Conn) (con *Connection) {
	return &Connection{
		con:      c,
		jobs:     make(chan job, 10),
		topics:   make(map[string]bool),
		srv:      s,
		received: make(map[uint16]bool),
		send:     make(map[uint16]*mqtt.Publish),
	}
}

func (s *Service) Start() {
	go func() {
		for {
			c, err := s.ls.Accept()
			if err != nil {
				fmt.Println("accept error:", err)
				break
			}
			con := s.NewConnection(c)
			go con.Run()
		}
		close(s.Done)
	}()

}
