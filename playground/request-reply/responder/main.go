package main

import (
	"log"
	"math"
	"strconv"
	"sync"

	"github.com/nats-io/nats.go"
)

var Subs map[string]*nats.Subscription

func main(){

	Subs = make(map[string]*nats.Subscription)
	nc, err := nats.Connect(nats.GetDefaultOptions().Url)
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()
	wg := sync.WaitGroup{}
	wg.Add(1)
	if err := reply(nc, "math/log", &wg); err != nil {
		log.Fatal(err)
	}	
	wg.Wait()

}


func reply(nc *nats.Conn, subject string, wg *sync.WaitGroup) error {
	sub, ok := Subs[subject]
	if !ok {
		if _, err := nc.Subscribe(subject, func(m *nats.Msg) {
			num, err := strconv.ParseFloat(string(m.Data), 64)
			if err != nil {
				log.Println(err)
				return
			}
			// calculate logarithm
			num = math.Log(num)
			data := strconv.FormatFloat(num, 'f', 6, 64)	
			err = m.Respond([]byte(data))
			if err != nil {
				return
			}
		}); err != nil {
			wg.Done()
			return err
		}
		Subs[subject] = sub
	}
	return nil
}
