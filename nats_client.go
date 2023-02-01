
// Package wraps around NATS golang client
package natsclient


import (
	"encoding/json"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)



type NatsClient struct {
	Conn	*nats.Conn
	Subs	map[string]*nats.Subscription
}


func NewNatsClient(urls string, opts []nats.Option) (*NatsClient, error) {
	nc, err := nats.Connect(urls, opts...)
	if err != nil {
		return nil, err
	}
	return &NatsClient{
		Conn:	nc,
		Subs:	make(map[string]*nats.Subscription),
	}, nil
}


func (nc *NatsClient) Close() {
	nc.Conn.Close()
}


//  ScatterGather is a common scatter-gather function for collecting responses 
//  from multiple services whose responses are of type T.
//  Example:    
//                     
//                      
//	          /---> (Service-B) 
//	         /  
//	        /     
//	   (Service-A) ----> (Service-B) 
//	        \
//	         \            
//	          \	    
// 		       \----> (Service-B) 
//    		 	        
//    			       
//
//   In the above example, 
//                 	    Service-A is the caller
//                     	Service-B is the responder
//                      T is the type of response from Service-B.
//
func ScatterGather[T any](nc *NatsClient, subject string, data interface{}, timeout time.Duration) (responses []T, err error) {
	replyTo := nats.NewInbox()
	sub, err := nc.Conn.SubscribeSync(replyTo)
	defer sub.Unsubscribe()
	if err != nil {
		return nil, err
	}
	nc.Conn.Flush()

	defer sub.Unsubscribe()
	b, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	if err = nc.Conn.PublishRequest(subject, replyTo, b); err != nil {
		return nil, err
	}
	start := time.Now()
	for time.Since(start) < timeout {
		msg, err := sub.NextMsg(timeout)
		if err != nil {
			break
		}
		var response T
		if err = json.Unmarshal(msg.Data, &response); err != nil {
			break
		}
		responses = append(responses, response)
	}
	return responses, err
}

// Reply is a common reply function for replying to a request
// Can be used as responder to a request of ScatterGather and RequestAction functions
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


//   RequestAction is a specailized scatter-gather function for collecting responses 
//   from multiple services which in turn are collecting T type responses from multiple services.
//   Example:    
//                     /----> (Service-C)
//                    /   
//          /---> (Service-B) ----> (Service-C)
//         /  
//        /     
//   (Service-A) ----> (Service-B) ----> (Service-C)
//        \
//         \             /----> (Service-C)
//          \	    	/
//           \----> (Service-B) ----> (Service-C)
//    		 	        \
//    			         \----> (Service-C)
//
//   In the above example, 
//                 	    Service-A is the caller
//                     	Service-B is the responder
//                      Service-C is the responder to Service-B.
//                      T is the type of response from Service-C.
//
func RequestAction[T any](nc *NatsClient, subject string, data interface{}, timeout time.Duration) (responses [][]T, err error) {
	replyTo := nats.NewInbox()
	sub, err := nc.Conn.SubscribeSync(replyTo)
	if err != nil {
		return nil, err
	}
	defer sub.Unsubscribe()
	nc.Conn.Flush()

	b, _ := json.Marshal(data)
	if err = nc.Conn.PublishRequest(subject, replyTo, b); err != nil {
		return nil, err
	}
	start := time.Now()
	for time.Since(start) < timeout {
		msg, err := sub.NextMsg(timeout)
		if err != nil {
			break
		}
		var response []T
		if err := json.Unmarshal(msg.Data, &response); err != nil {
			break
		}
		responses = append(responses, response)
	}
	return responses, err
}


// Subscribe subscribes on subject receives data via ch channel
// NOTE: needs testing 
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
// NOTE: needs testing  
func (nc *NatsClient) Publish(subject string, data interface{}) error {
	j, err := json.Marshal(data)
	if err != nil {
		return err
	}
	return nc.Conn.Publish(subject, j)
}
