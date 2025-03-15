package main

import (
	"context"
	"log"
	"time"

	"github.com/obrikash/event-driven-rmq/internal"
	"golang.org/x/sync/errgroup"
)

func main() {
	conn, err := internal.ConnectRabbitMQ("obrikash", "2288919ziqZ", "localhost:5672", "customers")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	client, err := internal.NewRabbitMQClient(conn)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	messageBus, err := client.Consume("customers_created", "email-service", false)
	if err != nil {
		panic(err)
	}

    var blocking chan struct{}

    // set a timeout for 15 secs
    ctx := context.Background()
    ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
    defer cancel()

    g, ctx := errgroup.WithContext(ctx)

    //errgroups allow us concurrent tasks

    g.SetLimit(10)
    
    go func() {
        for message := range messageBus {
            msg := message
            g.Go(func() error {
                log.Printf("New message: %v\n", msg)
                time.Sleep(10 * time.Second)
                if err := msg.Ack(false); err != nil {
                    log.Println("Ack message failed!")
                    return err
                }

                log.Printf("Acknowledged message: %s\n", message.MessageId)
                return nil
            })
        }
    }()

    log.Println("Consuming, use CRTL+C to exit")
    <-blocking
}
