// order service: gRPC API for placing/reading orders (called by the gateway).
// Publishes order.created and settles status from the saga's terminal events.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log"
	"net"
	"os"

	"order-system/internal/events"
	"order-system/internal/obs"
	orderpb "order-system/proto"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type server struct {
	orderpb.UnimplementedOrderServiceServer
	db *pgxpool.Pool
	js jetstream.JetStream
}

func (s *server) CreateOrder(ctx context.Context, req *orderpb.CreateOrderRequest) (*orderpb.OrderResponse, error) {
	if req.Item == "" || req.Qty <= 0 {
		return nil, status.Error(codes.InvalidArgument, "item required, qty must be > 0")
	}
	id := newID()
	if _, err := s.db.Exec(ctx, `INSERT INTO orders(id,item,qty,status) VALUES($1,$2,$3,'pending')`,
		id, req.Item, req.Qty); err != nil {
		return nil, status.Error(codes.Internal, "could not save order")
	}
	payload, _ := json.Marshal(events.OrderCreated{OrderID: id, Item: req.Item, Qty: int(req.Qty)})
	if err := obs.Publish(ctx, s.js, events.SubjectOrderCreated, payload); err != nil {
		log.Printf("publish order.created %s: %v", id, err)
	}
	return &orderpb.OrderResponse{Id: id, Item: req.Item, Qty: req.Qty, Status: "pending"}, nil
}

func (s *server) GetOrder(ctx context.Context, req *orderpb.GetOrderRequest) (*orderpb.OrderResponse, error) {
	var item, st string
	var qty int32
	if err := s.db.QueryRow(ctx, `SELECT item,qty,status FROM orders WHERE id=$1`, req.Id).
		Scan(&item, &qty, &st); err != nil {
		return nil, status.Error(codes.NotFound, "not found")
	}
	return &orderpb.OrderResponse{Id: req.Id, Item: item, Qty: qty, Status: st}, nil
}

func main() {
	ctx := context.Background()

	if shutdown, err := obs.Init(ctx, "order"); err != nil {
		log.Printf("otel: %v", err)
	} else {
		defer shutdown(ctx)
	}

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

	// Settle the order from whichever terminal event arrives (async, over NATS).
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
		mctx, span := otel.Tracer("order").Start(obs.Extract(context.Background(), msg), "order.settle")
		defer span.End()
		var e struct {
			OrderID string `json:"order_id"`
		}
		json.Unmarshal(msg.Data(), &e)
		st := "cancelled"
		if msg.Subject() == events.SubjectPaymentCompleted {
			st = "confirmed"
		}
		if _, err := db.Exec(mctx, `UPDATE orders SET status=$1 WHERE id=$2`, st, e.OrderID); err != nil {
			log.Printf("settle %s: %v", e.OrderID, err)
			msg.Nak()
			return
		}
		log.Printf("order %s -> %s", e.OrderID, st)
		msg.Ack()
	}); err != nil {
		log.Fatalf("consume: %v", err)
	}

	lis, err := net.Listen("tcp", ":9090")
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer(grpc.StatsHandler(otelgrpc.NewServerHandler()))
	orderpb.RegisterOrderServiceServer(srv, &server{db: db, js: js})
	log.Println("order gRPC on :9090")
	log.Fatal(srv.Serve(lis))
}

func newID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b)
}
