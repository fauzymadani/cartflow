// notification service: consumes terminal saga events and "notifies". Here that's
// a log line — real email/SMS/push is a swap inside notify(), not a new design.
package main

import (
	"context"
	"encoding/json"
	"log"

	"order-system/internal/events"
	"order-system/internal/obs"

	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
)

func main() {
	ctx := context.Background()

	if shutdown, err := obs.Init(ctx, "notification"); err != nil {
		log.Printf("otel: %v", err)
	} else {
		defer shutdown(ctx)
	}

	js, err := events.Connect(ctx)
	if err != nil {
		log.Fatalf("nats: %v", err)
	}

	cons, err := js.CreateOrUpdateConsumer(ctx, events.StreamName, jetstream.ConsumerConfig{
		Durable: "notification-service",
		FilterSubjects: []string{
			events.SubjectPaymentCompleted,
			events.SubjectPaymentFailed,
			events.SubjectInventoryFailed,
		},
		AckPolicy: jetstream.AckExplicitPolicy,
	})
	if err != nil {
		log.Fatalf("consumer: %v", err)
	}

	if _, err := cons.Consume(func(msg jetstream.Msg) {
		_, span := otel.Tracer("notification").Start(obs.Extract(context.Background(), msg), "notify")
		defer span.End()
		var e struct {
			OrderID string `json:"order_id"`
			Reason  string `json:"reason"`
		}
		json.Unmarshal(msg.Data(), &e)
		notify(msg.Subject(), e.OrderID, e.Reason)
		msg.Ack()
	}); err != nil {
		log.Fatalf("consume: %v", err)
	}

	log.Println("notification service consuming")
	select {}
}

func notify(subject, orderID, reason string) {
	if reason != "" {
		reason = " (" + reason + ")"
	}
	log.Printf("NOTIFY order %s: %s%s", orderID, subject, reason)
}
