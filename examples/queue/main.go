package main

import (
	"context"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/alecmerdler/natscontext"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func main() {
	entrypoint := os.Getenv("ENTRYPOINT")
	if entrypoint == "" {
		entrypoint = "generate"
	}

	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = nats.DefaultURL
	}

	var nc *nats.Conn
	var err error
	for {
		nc, err = nats.Connect(
			natsURL,
			nats.Timeout(10*time.Second),
			nats.MaxReconnects(-1),
			nats.ReconnectWait(time.Second),
		)
		if err == nil {
			break
		}
		log.Printf("[%s] connect: %v, retrying...", entrypoint, err)
		time.Sleep(time.Second)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatalf("[%s] jetstream: %v", entrypoint, err)
	}

	ctx := context.Background()
	kv, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: "queue",
	})
	if err != nil {
		log.Fatalf("[%s] create kv: %v", entrypoint, err)
	}

	dctx, err := natscontext.New(ctx, kv, "queue")
	if err != nil {
		log.Fatalf("[%s] new context: %v", entrypoint, err)
	}

	switch entrypoint {
	case "generate":
		generate(dctx)
	case "enrich":
		enrich(dctx)
	case "filter":
		filter(dctx)
	default:
		log.Fatalf("[%s] unknown entrypoint: %s", entrypoint, entrypoint)
	}
}

const (
	generateKey    natscontext.NatsContextValue = "generate"
	generateTicker                              = 10 * time.Millisecond
)

func generate(ctx context.Context) {
	ticker := time.NewTicker(generateTicker)
	defer ticker.Stop()

	var err error
	for {
		select {
		case <-ctx.Done():
			log.Printf("[generate] context done: %v", ctx.Err())
			return
		case <-ticker.C:
			value := time.Now().Unix()
			ctx, err = natscontext.WithValue(ctx, generateKey, value)
			if err != nil {
				log.Printf("[generate] with value: %v", err)
				continue
			}

			log.Printf("[generate] generated value: %d", value)
		}
	}
}

const (
	enrichKey    natscontext.NatsContextValue = "enrich"
	enrichTicker                              = 50 * time.Millisecond
)

func enrich(ctx context.Context) {
	ticker := time.NewTicker(enrichTicker)
	defer ticker.Stop()

	var err error
	var lastValue float64
	for {
		select {
		case <-ctx.Done():
			log.Printf("[enrich] context done: %v", ctx.Err())
			return
		case <-ticker.C:
			rawValue := ctx.Value(generateKey)
			if rawValue == nil {
				log.Printf("[enrich] no value found")
				continue
			}

			value, ok := rawValue.(float64)
			if !ok {
				log.Printf("[enrich] value is not float64: %T", rawValue)
				continue
			}

			if lastValue == value {
				log.Printf("[enrich] value unchanged: %g", value)
				continue
			}

			lastValue = value
			enrichedValue := value - rand.Float64()*10
			ctx, err = natscontext.WithValue(ctx, enrichKey, enrichedValue)
			if err != nil {
				log.Printf("[enrich] with value: %v", err)
				continue
			}
			log.Printf("[enrich] enriched value: %g", enrichedValue)
		}
	}
}

const (
	filterKey    natscontext.NatsContextValue = "filter"
	filterTicker                              = 100 * time.Millisecond
)

func filter(ctx context.Context) {
	ticker := time.NewTicker(filterTicker)
	defer ticker.Stop()

	var err error
	var lastValue float64
	for {
		select {
		case <-ctx.Done():
			log.Printf("[filter] context done: %v", ctx.Err())
			return
		case <-ticker.C:
			rawValue := ctx.Value(enrichKey)
			if rawValue == nil {
				log.Printf("[filter] no value found")
				continue
			}

			value, ok := rawValue.(float64)
			if !ok {
				log.Printf("[filter] value is not string: %T", rawValue)
				continue
			}

			if lastValue == value {
				log.Printf("[filter] value unchanged: %g", value)
				continue
			}

			lastValue = value
			if int(value)%2 == 0 {
				log.Printf("[filter] value is even, filtering: %g", value)
			} else {
				log.Printf("[filter] value is odd: %g", value)
				ctx, err = natscontext.WithValue(ctx, filterKey, value)
				if err != nil {
					log.Printf("[filter] with value: %v", err)
					continue
				}
			}
		}
	}
}
