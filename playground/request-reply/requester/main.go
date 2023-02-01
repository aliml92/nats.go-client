package main

import (
	"flag"
	"log"
	"time"

	"github.com/nats-io/nats.go"
)

func main() {

	var num = flag.String("x", "1", "number")
	flag.Parse()

	nc, err := nats.Connect(nats.GetDefaultOptions().Url)
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	result, err := request(nc, "math/log", []byte(*num), 1*time.Second) 
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("natural logarithm of %s is %s", *num, result)

	// replyTo := nats.NewInbox()
	// sub, err := nc.SubscribeSync(replyTo)
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// nc.Flush()

	// if err = nc.PublishRequest("math/log", replyTo, []byte(*num)); err != nil {
	// 	log.Fatal(err)
	// }
	// start := time.Now()
	// for time.Since(start) < 1*time.Second {
	// 	msg, err := sub.NextMsg(time.Second)
	// 	if err != nil {
	// 		break
	// 	}
	// 	log.Printf("natural logarithm of %s is %s", *num, msg.Data)
	// }
	// sub.Unsubscribe()

}

func request(nc *nats.Conn, subject string, data []byte, timeout time.Duration) (result []byte, err error) {
	replyTo := nats.NewInbox()
	sub, err := nc.SubscribeSync(replyTo)
	if err != nil {
		return nil, err
	}
	defer sub.Unsubscribe()
	nc.Flush()
	
	if err = nc.PublishRequest(subject, replyTo, data); err != nil {
		return nil, err
	}
	start := time.Now()
	for time.Since(start) < timeout {
		msg, err := sub.NextMsg(timeout)
		if err != nil {
			break
		}
		result = msg.Data
		break
	}
	return result, err
}