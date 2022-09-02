
// Package wraps around NATS golang client
package natsclient


import (
	"encoding/json"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

// A NatsClient wraps a nats connection, subscriptions and a timeout for scatter-gather pattern  
type NatsClient struct {
	Conn          *nats.Conn
	Subs          map[string]*nats.Subscription
	Timeout    	  time.Duration
}


// NewNatsClient creates a new NatsClient with a default timeout of five seconds
// and returns it along with error if occurs
// TODO: allow using credentials when connecting to NATS server
func NewNatsClient(hostname string, port string) (*NatsClient, error) {
	url := "nats://" + hostname + ":" + port
	nc, err := nats.Connect(url)
	if err != nil {
		return nil, err
	}
	return &NatsClient{
		Conn:          nc,
		Subs:          make(map[string]*nats.Subscription),
		Timeout:       time.Second * 5,
	}, nil
}


// Close closes underlying nats connection
func (nc *NatsClient) Close() {
	nc.Conn.Close()
}


// SetTimeout sets a timeout used for scatter-gather pattern 
func (nc *NatsClient) SetTimeout(timeout time.Duration) {
	nc.Timeout = timeout
}



// ScatterGather publishes data on subject and waits on uniqueReplyTo until timeout exceeds to gather responses
// and it returns collected responses or error if occurs 
func ScatterGather[T any](nc *NatsClient, subject string, data interface{}) ([]T, error) {
	var err error
	uniqueReplyTo := nats.NewInbox()
	sub, err := nc.Conn.SubscribeSync(uniqueReplyTo)
	if err != nil {
		return nil, err
	}	
	defer sub.Unsubscribe()
	b, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	var responses []T
	err = nc.Conn.PublishRequest(subject, uniqueReplyTo, b)
	if err != nil {
		return nil, err
	}
	for time.Since(time.Now()) < nc.Timeout {
		msg, err := sub.NextMsg(1 * time.Second)
		if err != nil {
			break
		}
		var response T
		if err = json.Unmarshal(msg.Data, &response); err != nil {
			break
		}
		responses = append(responses, response)
	}
	if len(responses) > 0 {
		err = nil
	}
	return responses, nil
}




// Reply is not tested,
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


// Subscribe subscribes on subject receives data via ch channel 
func Subscribe[T any](nc *NatsClient, subject string, ch chan T) error {
	sub, ok := nc.Subs[subject]
	var err error
	if !ok {
		if _, err = nc.Conn.Subscribe(subject, func(m *nats.Msg) {
			var d T
			err = json.Unmarshal(m.Data, &d)
			if err != nil {
				return
			}
			ch <- d
		}); err != nil {
			return err
		}
		nc.Subs[subject] = sub
	}
	return nil
}


// Publish publishes data on subject or returns error if data cannot be converted to json 
func (nc *NatsClient) Publish(subject string, data interface{}) error {
	j, err := json.Marshal(data)
	if err != nil {
		return err
	}
	return nc.Conn.Publish(subject, j)
}
