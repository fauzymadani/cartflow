// Package events is the shared messaging contract: subject names, payload shapes,
// and the JetStream setup every service uses so producers and consumers can't drift.
package events

import (
	"context"
	"os"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	StreamName = "ORDERS"

	SubjectOrderCreated      = "order.created"
	SubjectInventoryReserved = "inventory.reserved"
	SubjectInventoryFailed   = "inventory.failed"
	SubjectPaymentCompleted  = "payment.completed"
	SubjectPaymentFailed     = "payment.failed"
)

type OrderCreated struct {
	OrderID string `json:"order_id"`
	Item    string `json:"item"`
	Qty     int    `json:"qty"`
}

type InventoryReserved struct {
	OrderID string `json:"order_id"`
	Item    string `json:"item"`
	Qty     int    `json:"qty"` // carried so payment can decide without re-fetching
}

type InventoryFailed struct {
	OrderID string `json:"order_id"`
	Reason  string `json:"reason"`
}

type PaymentCompleted struct {
	OrderID string `json:"order_id"`
}

type PaymentFailed struct {
	OrderID string `json:"order_id"`
	Reason  string `json:"reason"`
}

// Connect dials NATS (from $NATS_URL) and ensures the stream exists. The stream
// is idempotent, so any service may boot first.
func Connect(ctx context.Context) (jetstream.JetStream, error) {
	nc, err := nats.Connect(os.Getenv("NATS_URL"))
	if err != nil {
		return nil, err
	}
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}
	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     StreamName,
		Subjects: []string{"order.*", "inventory.*", "payment.*"},
	})
	return js, err
}
