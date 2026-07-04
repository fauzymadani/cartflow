// payment service: mock charge on inventory.reserved. Deterministic so demos are
// repeatable — declines when qty >= 5 (simulates "amount too high"). Swap charge()
// for a real gateway (Midtrans/Stripe) later; the events don't change.
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

const declineAtQty = 5 // ponytail: fixed rule; real gateway call goes in charge()

func main() {
	ctx := context.Background()

	if shutdown, err := obs.Init(ctx, "payment"); err != nil {
		log.Printf("otel: %v", err)
	} else {
		defer shutdown(ctx)
	}

	js, err := events.Connect(ctx)
	if err != nil {
		log.Fatalf("nats: %v", err)
	}

	cons, err := js.CreateOrUpdateConsumer(ctx, events.StreamName, jetstream.ConsumerConfig{
		Durable:        "payment-service",
		FilterSubjects: []string{events.SubjectInventoryReserved},
		AckPolicy:      jetstream.AckExplicitPolicy,
	})
	if err != nil {
		log.Fatalf("consumer: %v", err)
	}

	if _, err := cons.Consume(func(msg jetstream.Msg) {
		mctx, span := otel.Tracer("payment").Start(obs.Extract(context.Background(), msg), "payment.charge")
		defer span.End()
		var e events.InventoryReserved
		json.Unmarshal(msg.Data(), &e)
		charge(mctx, js, e)
		msg.Ack()
	}); err != nil {
		log.Fatalf("consume: %v", err)
	}

	log.Println("payment service consuming inventory.reserved")
	select {}
}

func charge(ctx context.Context, js jetstream.JetStream, e events.InventoryReserved) {
	if e.Qty >= declineAtQty {
		payload, _ := json.Marshal(events.PaymentFailed{OrderID: e.OrderID, Reason: "amount too high"})
		obs.Publish(ctx, js, events.SubjectPaymentFailed, payload)
		log.Printf("declined %s (qty %d)", e.OrderID, e.Qty)
		return
	}
	payload, _ := json.Marshal(events.PaymentCompleted{OrderID: e.OrderID})
	obs.Publish(ctx, js, events.SubjectPaymentCompleted, payload)
	log.Printf("charged %s -> completed", e.OrderID)
}
