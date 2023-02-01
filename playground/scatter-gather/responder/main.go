package main

import (
	"flag"
	"log"
	"math"
	"strconv"

	"sync"

	"github.com/nats-io/nats.go"
)

var Subs map[string]*nats.Subscription

func main(){
	var currency = flag.String("to", "", "a currency to convert to")
	flag.Parse()

	Subs = make(map[string]*nats.Subscription)
	nc, err := nats.Connect(nats.GetDefaultOptions().Url)
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()
	wg := sync.WaitGroup{}
	wg.Add(1)
	if err := reply(nc, "finance/convert/usd", &wg, *currency); err != nil {
		log.Fatal(err)
	}
	wg.Wait()

}


func reply(nc *nats.Conn, subject string, wg *sync.WaitGroup, toCurrency string) error {
	sub, ok := Subs[subject]
	if !ok {
		if _, err := nc.Subscribe(subject, func(m *nats.Msg) {
			usd, err := strconv.Atoi(string(m.Data))
			if err != nil {
				log.Println(err)
				return
			}
			// calculate logarithm
			converted := convert(usd, toCurrency)
			data := strconv.FormatFloat(converted, 'f', 6, 64)	
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


func convert(usd int, toCurrency string) float64 {
	switch toCurrency {
	case "EUR":
		return float64(usd) * 0.88
	case "GBP":
		return float64(usd) * 0.77
	case "CAD":
		return float64(usd) * 1.32
	case "AUD":
		return float64(usd) * 1.43
	case "NZD":
		return float64(usd) * 1.53	
	case "JPY":
		return float64(usd) * 111.12
	case "CNY":
		return float64(usd) * 6.92
	case "INR":
		return float64(usd) * 71.37
	case "CHF":
		return float64(usd) * 0.99
	case "RUB":
		return float64(usd) * 63.92
	default:
		return math.NaN()
	}
}