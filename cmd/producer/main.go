package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/joho/godotenv"
	"github.com/obrikash/event-driven-rmq/internal"
	"github.com/rabbitmq/amqp091-go"
)

func main() {
    err := godotenv.Load()
    if err != nil {
        log.Fatal("Error loading .env file")
    }
    rmqPassword := os.Getenv("RMQ_PASSWORD")

	conn, err := internal.ConnectRabbitMQ("obrikash", rmqPassword, "localhost:5672", "customers")

	if err != nil {
		panic(err)
	}
	defer conn.Close()

	client, err := internal.NewRabbitMQClient(conn)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	if err := client.CreateQueue("customers_created", true, false); err != nil {
		panic(err)
	}

	if err := client.CreateQueue("customers_test", false, true); err != nil {
		panic(err)
	}

	if err := client.CreateBinding("customers_created", "customers.created.*", "customer_events"); err != nil {
		panic(err)
	}

	if err := client.CreateBinding("customers_test", "customers.*", "customer_events"); err != nil {
		panic(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for i := 0; i < 10; i++ {
		if err := client.Send(ctx, "customer_events", "customers.created.us", amqp091.Publishing{
			ContentType:  "text/plain",
			DeliveryMode: amqp091.Persistent,
			Body:         []byte(`A cool message between services`),
		}); err != nil {
			panic(err)
		}

		if err := client.Send(ctx, "customer_events", "customers.test", amqp091.Publishing{
			ContentType:  "text/plain",
			DeliveryMode: amqp091.Transient,
			Body:         []byte(`An uncool undurable message`),
		}); err != nil {
			panic(err)

		}
	}
	log.Println(client)
}
