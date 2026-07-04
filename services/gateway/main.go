// gateway: the authenticated edge. POST /login issues a JWT; everything under
// /orders is reverse-proxied to the order service behind Bearer-token auth.
// ponytail: internal call is HTTP reverse-proxy, not gRPC — gRPC is an opt-in
// upgrade (needs protoc); the proxy does the edge's real job with zero new deps.
package main

import (
	"encoding/json"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func main() {
	orderURL, err := url.Parse(getenv("ORDER_URL", "http://order:8080"))
	if err != nil {
		log.Fatalf("bad ORDER_URL: %v", err)
	}
	proxy := httputil.NewSingleHostReverseProxy(orderURL)
	secret := []byte(getenv("JWT_SECRET", "dev-secret"))
	user := getenv("AUTH_USER", "demo")
	pass := getenv("AUTH_PASS", "demo")

	mux := http.NewServeMux()

	mux.HandleFunc("POST /login", func(w http.ResponseWriter, r *http.Request) {
		var c struct{ User, Pass string }
		if err := json.NewDecoder(r.Body).Decode(&c); err != nil || c.User != user || c.Pass != pass {
			http.Error(w, `{"error":"invalid credentials"}`, http.StatusUnauthorized)
			return
		}
		tok := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
			"sub": c.User,
			"exp": time.Now().Add(time.Hour).Unix(),
		})
		signed, err := tok.SignedString(secret)
		if err != nil {
			http.Error(w, `{"error":"could not sign token"}`, http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"token": signed})
	})

	authed := requireJWT(secret, proxy)
	mux.Handle("/orders", authed)  // POST /orders
	mux.Handle("/orders/", authed) // GET /orders/{id}

	log.Println("gateway on :8080 ->", orderURL)
	log.Fatal(http.ListenAndServe(":8080", mux))
}

// requireJWT rejects anything without a valid HS256 Bearer token (pinning the
// method blocks the classic alg-confusion attack).
func requireJWT(secret []byte, next http.Handler) http.Handler {
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
		next.ServeHTTP(w, r)
	})
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
