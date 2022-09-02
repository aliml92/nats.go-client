package natsclient

// specific nats client for the nats-server
// Language: go
// Path: nats-server.go

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

type NatsClient struct {
	Conn          *nats.Conn
	Subs          map[string]*nats.Subscription
	UniqueReplyTo map[string]string
	ReqTimeout    time.Duration
}


func NewNatsClient(hostname string, port string) *NatsClient {
	url := "nats://" + hostname + ":" + port
	nc, err := nats.Connect(url)
	if err != nil {
		log.Println(err)
	}
	return &NatsClient{
		Conn:          nc,
		Subs:          make(map[string]*nats.Subscription),
		UniqueReplyTo: make(map[string]string),
		ReqTimeout:    time.Second * 5,
	}
}

func (nc *NatsClient) Close() {
	nc.Conn.Close()
}

func (nc *NatsClient) SetRequestTimeout(timeout time.Duration) {
	nc.ReqTimeout = timeout
}

func RequestAndGather[T any](nc *NatsClient, subject string) ([]T, error) {
	var uniqueReplyTo string
	var e error
	sub, ok := nc.Subs[subject]
	if !ok {
		uniqueReplyTo = nats.NewInbox()
		sub, e = nc.Conn.SubscribeSync(uniqueReplyTo)
		if e != nil {
			log.Println(e)
			return nil, e
		}
		nc.Subs[subject] = sub
		nc.UniqueReplyTo[subject] = uniqueReplyTo
	} else {
		uniqueReplyTo = nc.UniqueReplyTo[subject]
	}
	if e = nc.Conn.PublishRequest(subject, uniqueReplyTo, []byte("req")); e != nil {
		log.Println(e)
		return nil, e
	}
	start := time.Now()
	var responses []T
	for time.Since(start) < nc.ReqTimeout {
		// Probably at maximum 0.1 seconds is enough to get the responses
		// but we'll wait for 1 second just in case it takes longer
		msg, err := sub.NextMsg(time.Second)
		if err != nil {
			e = err
			break
		}
		var response T
		if err := json.Unmarshal(msg.Data, &response); err != nil {
			e = err
			log.Println(err)
			break
		}
		responses = append(responses, response)
	}
	return responses, e
}


func Request[T any](nc *NatsClient, subject string) ([]T, error) {
	var uniqueReplyTo string
	var e error
	sub, ok := nc.Subs[subject]
	if !ok {
		uniqueReplyTo = nats.NewInbox()
		sub, e = nc.Conn.SubscribeSync(uniqueReplyTo)
		if e != nil {
			log.Println(e)
			return nil, e
		}
		nc.Subs[subject] = sub
		nc.UniqueReplyTo[subject] = uniqueReplyTo
	} else {
		uniqueReplyTo = nc.UniqueReplyTo[subject]
	}
	if e = nc.Conn.PublishRequest(subject, uniqueReplyTo, []byte("req")); e != nil {
		log.Println(e)
		return nil, e
	}
	var responses []T
	msg, err := sub.NextMsg(time.Second)
	if err != nil {
		e = err
		return nil, e
	}
	if err := json.Unmarshal(msg.Data, &responses); err != nil {
		e = err
		log.Println(err)
		return nil, e
	}
	return responses, e
}


func (nc *NatsClient) Reply(subject string, data interface{}, wg *sync.WaitGroup) error {
	sub, ok := nc.Subs[subject]
	if !ok {
		if _, err := nc.Conn.Subscribe(subject, func(m *nats.Msg) {
			j, _ := json.Marshal(data)
			err := m.Respond(j)
			if err != nil {
				return
			}
		}); err != nil {
			wg.Done()
			return err
		}
		nc.Subs[subject] = sub
	}
	return nil
}

func (nc *NatsClient)  Subscribe(subject string, ch chan interface{}) error {
	sub, ok := nc.Subs[subject]
	var err error
	if !ok {
		if _, err := nc.Conn.Subscribe(subject, func(m *nats.Msg) {
			var d interface{}
			log.Println("Received:", string(m.Data))
			err = json.Unmarshal(m.Data, &d)
			if err != nil {
				log.Println(err)
				log.Println("Come here 1")
				return
			}
			ch <- d
		}); err != nil {
			log.Println("Come here 2")
			return err
		}
		log.Println("Come here 3")
		nc.Subs[subject] = sub
	}
	return nil
}

func (nc *NatsClient) Publish(subject string, data interface{}) error {
	j, err := json.Marshal(data)
	if err != nil {
		return err
	}
	log.Println("Publishing:", string(j))
	return nc.Conn.Publish(subject, j)
}
