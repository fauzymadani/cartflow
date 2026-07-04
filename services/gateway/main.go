// gateway: the authenticated REST edge. POST /login issues a JWT; /orders is
// JSON in, but internally the gateway calls the order service over gRPC.
package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"order-system/internal/obs"
	orderpb "order-system/proto"

	"github.com/golang-jwt/jwt/v5"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func main() {
	ctx := context.Background()
	if shutdown, err := obs.Init(ctx, "gateway"); err != nil {
		log.Printf("otel: %v", err)
	} else {
		defer shutdown(ctx)
	}

	conn, err := grpc.NewClient(getenv("ORDER_ADDR", "order:9090"),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()))
	if err != nil {
		log.Fatalf("grpc dial: %v", err)
	}
	orders := orderpb.NewOrderServiceClient(conn)

	secret := []byte(getenv("JWT_SECRET", "dev-secret"))
	user := getenv("AUTH_USER", "demo")
	pass := getenv("AUTH_PASS", "demo")
	tracer := otel.Tracer("gateway")

	mux := http.NewServeMux()

	mux.HandleFunc("POST /login", func(w http.ResponseWriter, r *http.Request) {
		var c struct{ User, Pass string }
		if err := json.NewDecoder(r.Body).Decode(&c); err != nil || c.User != user || c.Pass != pass {
			http.Error(w, `{"error":"invalid credentials"}`, http.StatusUnauthorized)
			return
		}
		tok := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
			"sub": c.User, "exp": time.Now().Add(time.Hour).Unix(),
		})
		signed, err := tok.SignedString(secret)
		if err != nil {
			http.Error(w, `{"error":"could not sign token"}`, http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"token": signed})
	})

	mux.Handle("POST /orders", requireJWT(secret, func(w http.ResponseWriter, r *http.Request) {
		rctx, span := tracer.Start(r.Context(), "POST /orders")
		defer span.End()
		var req struct {
			Item string `json:"item"`
			Qty  int32  `json:"qty"`
		}
		json.NewDecoder(r.Body).Decode(&req)
		resp, err := orders.CreateOrder(rctx, &orderpb.CreateOrderRequest{Item: req.Item, Qty: req.Qty})
		if err != nil {
			httpError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, resp)
	}))

	mux.Handle("GET /orders/{id}", requireJWT(secret, func(w http.ResponseWriter, r *http.Request) {
		rctx, span := tracer.Start(r.Context(), "GET /orders/{id}")
		defer span.End()
		resp, err := orders.GetOrder(rctx, &orderpb.GetOrderRequest{Id: r.PathValue("id")})
		if err != nil {
			httpError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, resp)
	}))

	log.Println("gateway on :8080")
	log.Fatal(http.ListenAndServe(":8080", mux))
}

// requireJWT rejects anything without a valid HS256 Bearer token (pinning the
// method blocks the classic alg-confusion attack).
func requireJWT(secret []byte, next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		tok, err := jwt.Parse(raw, func(t *jwt.Token) (any, error) {
			if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, jwt.ErrSignatureInvalid
			}
			return secret, nil
		})
		if err != nil || !tok.Valid {
			http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
			return
		}
		next(w, r)
	})
}

// httpError maps gRPC status codes back to HTTP for the client.
func httpError(w http.ResponseWriter, err error) {
	code := http.StatusInternalServerError
	switch status.Code(err) {
	case codes.InvalidArgument:
		code = http.StatusBadRequest
	case codes.NotFound:
		code = http.StatusNotFound
	}
	http.Error(w, `{"error":"`+status.Convert(err).Message()+`"}`, code)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
