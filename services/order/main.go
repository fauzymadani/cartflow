// order service: REST edge for placing/reading orders. Publishes order.created
// and settles status from the saga's terminal events (payment / inventory failure).
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log"
	"net/http"
	"os"

	"order-system/internal/events"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go/jetstream"
)

func main() {
	ctx := context.Background()

	db, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatalf("db connect: %v", err)
	}
	if _, err := db.Exec(ctx, `CREATE TABLE IF NOT EXISTS orders(
		id text PRIMARY KEY, item text NOT NULL, qty int NOT NULL, status text NOT NULL)`); err != nil {
		log.Fatalf("schema: %v", err)
	}

	js, err := events.Connect(ctx)
	if err != nil {
		log.Fatalf("nats: %v", err)
	}

	// Settle the order from whichever terminal event arrives.
	cons, err := js.CreateOrUpdateConsumer(ctx, events.StreamName, jetstream.ConsumerConfig{
		Durable: "order-service",
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
		var e struct {
			OrderID string `json:"order_id"`
		}
		json.Unmarshal(msg.Data(), &e)
		status := "cancelled"
		if msg.Subject() == events.SubjectPaymentCompleted {
			status = "confirmed"
		}
		if _, err := db.Exec(ctx, `UPDATE orders SET status=$1 WHERE id=$2`, status, e.OrderID); err != nil {
			log.Printf("settle %s: %v", e.OrderID, err)
			msg.Nak() // redeliver rather than lose the update
			return
		}
		log.Printf("order %s -> %s", e.OrderID, status)
		msg.Ack()
	}); err != nil {
		log.Fatalf("consume: %v", err)
	}

	http.HandleFunc("POST /orders", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Item string `json:"item"`
			Qty  int    `json:"qty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Item == "" || req.Qty <= 0 {
			http.Error(w, `{"error":"item required, qty must be > 0"}`, http.StatusBadRequest)
			return
		}
		id := newID()
		if _, err := db.Exec(ctx, `INSERT INTO orders(id,item,qty,status) VALUES($1,$2,$3,'pending')`,
			id, req.Item, req.Qty); err != nil {
			http.Error(w, `{"error":"could not save order"}`, http.StatusInternalServerError)
			return
		}
		payload, _ := json.Marshal(events.OrderCreated{OrderID: id, Item: req.Item, Qty: req.Qty})
		if _, err := js.Publish(ctx, events.SubjectOrderCreated, payload); err != nil {
			log.Printf("publish order.created %s: %v", id, err)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"id": id, "status": "pending"})
	})

	http.HandleFunc("GET /orders/{id}", func(w http.ResponseWriter, r *http.Request) {
		var item, status string
		var qty int
		err := db.QueryRow(ctx, `SELECT item,qty,status FROM orders WHERE id=$1`,
			r.PathValue("id")).Scan(&item, &qty, &status)
		if err != nil {
			http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"id": r.PathValue("id"), "item": item, "qty": qty, "status": status})
	})

	log.Println("order service on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func newID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b)
}
