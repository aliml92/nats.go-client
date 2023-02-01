package main

import (
	"flag"
	"log"
	"time"

	"github.com/nats-io/nats.go"
)

func main() {

	var amount = flag.String("convert-usd", "1", "amount to convert")
	flag.Parse()

	nc, err := nats.Connect(nats.GetDefaultOptions().Url)
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()
	var results []string
	results, err = request(nc, "finance/convert/usd", []byte(*amount), 1*time.Second)
	if err != nil {
		log.Fatal(err)
	}
	for _, result := range results {
		log.Printf("converted %s usd to %s", *amount, result)
	}

	// replyTo := nats.NewInbox()
	// sub, err := nc.SubscribeSync(replyTo)
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// nc.Flush()

	// if err = nc.PublishRequest("math/log", replyTo, []byte(*num)); err != nil {
	// 	log.Fatal(err)
	// }

	// var responses []string
	// start := time.Now()
	// for time.Since(start) < 1*time.Second {
	// 	msg, err := sub.NextMsg(time.Second)
	// 	if err != nil {
	// 		break
	// 	}
	// 	log.Printf("natural logarithm of %s is %s", *num, msg.Data)
	// 	responses = append(responses, string(msg.Data))
	// }
	// sub.Unsubscribe()

}


func request(nc *nats.Conn, subject string, data []byte, timeout time.Duration) (results []string, err error) {
	replyTo := nats.NewInbox()
	sub, err := nc.SubscribeSync(replyTo)
	defer sub.Unsubscribe()
	if err != nil {
		return nil, err
	}
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
		results = append(results, string(msg.Data))
	}
	return results, err
}