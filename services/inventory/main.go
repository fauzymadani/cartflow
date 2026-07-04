// inventory service: reserves stock on order.created, and compensates by releasing
// it when payment fails. Tracks reservations so it can undo the exact amount.
package main

import (
	"context"
	"encoding/json"
	"log"
	"os"

	"order-system/internal/events"
	"order-system/internal/obs"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
)

func main() {
	ctx := context.Background()

	if shutdown, err := obs.Init(ctx, "inventory"); err != nil {
		log.Printf("otel: %v", err)
	} else {
		defer shutdown(ctx)
	}

	db, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatalf("db connect: %v", err)
	}
	if _, err := db.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS stock(item text PRIMARY KEY, available int NOT NULL);
		CREATE TABLE IF NOT EXISTS reservation(order_id text PRIMARY KEY, item text NOT NULL, qty int NOT NULL);`); err != nil {
		log.Fatalf("schema: %v", err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO stock(item,available)
		VALUES ('widget',10),('gadget',5) ON CONFLICT DO NOTHING`); err != nil {
		log.Fatalf("seed: %v", err)
	}

	js, err := events.Connect(ctx)
	if err != nil {
		log.Fatalf("nats: %v", err)
	}
	cons, err := js.CreateOrUpdateConsumer(ctx, events.StreamName, jetstream.ConsumerConfig{
		Durable: "inventory-service",
		FilterSubjects: []string{
			events.SubjectOrderCreated,
			events.SubjectPaymentCompleted,
			events.SubjectPaymentFailed,
		},
		AckPolicy: jetstream.AckExplicitPolicy,
	})
	if err != nil {
		log.Fatalf("consumer: %v", err)
	}

	if _, err := cons.Consume(func(msg jetstream.Msg) {
		mctx, span := otel.Tracer("inventory").Start(obs.Extract(context.Background(), msg), "inventory."+msg.Subject())
		defer span.End()
		var e struct {
			OrderID string `json:"order_id"`
			Item    string `json:"item"`
			Qty     int    `json:"qty"`
		}
		if err := json.Unmarshal(msg.Data(), &e); err != nil {
			log.Printf("bad %s: %v", msg.Subject(), err)
			msg.Ack() // poison message
			return
		}
		switch msg.Subject() {
		case events.SubjectOrderCreated:
			reserve(mctx, db, js, e.OrderID, e.Item, e.Qty)
		case events.SubjectPaymentFailed:
			release(mctx, db, e.OrderID) // compensation: give the stock back
		case events.SubjectPaymentCompleted:
			db.Exec(mctx, `DELETE FROM reservation WHERE order_id=$1`, e.OrderID) // reservation now permanent
		}
		msg.Ack()
	}); err != nil {
		log.Fatalf("consume: %v", err)
	}

	log.Println("inventory service consuming")
	select {}
}

// reserve decrements stock only if enough is available (atomic via WHERE), records
// the reservation so it can be undone, and reports the outcome.
func reserve(ctx context.Context, db *pgxpool.Pool, js jetstream.JetStream, orderID, item string, qty int) {
	tag, err := db.Exec(ctx,
		`UPDATE stock SET available=available-$1 WHERE item=$2 AND available>=$1`, qty, item)
	if err == nil && tag.RowsAffected() == 1 {
		db.Exec(ctx, `INSERT INTO reservation(order_id,item,qty) VALUES($1,$2,$3)
			ON CONFLICT DO NOTHING`, orderID, item, qty)
		payload, _ := json.Marshal(events.InventoryReserved{OrderID: orderID, Item: item, Qty: qty})
		obs.Publish(ctx, js, events.SubjectInventoryReserved, payload)
		log.Printf("reserved %d x %s for %s", qty, item, orderID)
		return
	}
	reason := "insufficient stock"
	if err != nil {
		reason = "reserve error"
		log.Printf("reserve %s: %v", orderID, err)
	}
	payload, _ := json.Marshal(events.InventoryFailed{OrderID: orderID, Reason: reason})
	obs.Publish(ctx, js, events.SubjectInventoryFailed, payload)
	log.Printf("failed %s: %s", orderID, reason)
}

// release returns a reservation's stock. Idempotent: deleting the row first means
// a redelivered payment.failed can't double-credit.
func release(ctx context.Context, db *pgxpool.Pool, orderID string) {
	var item string
	var qty int
	err := db.QueryRow(ctx,
		`DELETE FROM reservation WHERE order_id=$1 RETURNING item,qty`, orderID).Scan(&item, &qty)
	if err != nil {
		return // no reservation to release (never reserved, or already released)
	}
	db.Exec(ctx, `UPDATE stock SET available=available+$1 WHERE item=$2`, qty, item)
	log.Printf("released %d x %s for %s", qty, item, orderID)
}
