# CartFlow — Event-Driven Order System

Go microservices demonstrating a **choreography-based saga** with **compensation**
over NATS JetStream, fronted by a JWT gateway, running on Kubernetes with a Tilt
live-reload dev loop.

## Flow

```
client ─POST /orders─▶ gateway(JWT) ─▶ order ─order.created─▶ inventory
                                        ▲                        │ reserve (atomic) + record
                                        │                        ▼
                                        │                  inventory.reserved ─▶ payment
                                        │                        │                  │ mock charge
                                        │        inventory.failed │        ┌─────────┴─────────┐
                                        │                         │  payment.completed   payment.failed
                                        │                         ▼        ▼                  ▼
                                        └── order status: cancelled | confirmed | cancelled
                                                                                   │
                                              inventory RELEASES stock ◀───────────┘ (compensation)
        notification logs every terminal event.
```

No orchestrator — each service reacts to events. JetStream makes events durable,
so services boot in any order and nothing is lost.

## Services

- **gateway** — JWT auth edge; `POST /login`, reverse-proxies `/orders` to order.
- **order** — `POST /orders`, `GET /orders/{id}`; publishes `order.created`, settles from terminal events.
- **inventory** — reserves stock atomically, records reservations, **releases on payment failure**.
- **payment** — mock charge on `inventory.reserved`; deterministic decline at `qty >= 5`.
- **notification** — logs every terminal outcome (swap `notify()` for real email/SMS).

Seeded stock: `widget=10`, `gadget=5`. Demo credentials: `demo` / `demo`.

## Run

Terraform provisions the platform (NATS via Helm + the app Secret); Tilt builds and
runs Postgres + the services. **Terraform first** — the services mount its Secret.

```bash
minikube start
make tf-up         # terraform: NATS (Helm) + Secret with generated JWT key
make up            # tilt: builds 5 images, deploys Postgres + services
make demo          # asserts confirm / out-of-stock / payment-decline+compensation + 401
```

Ownership: **Terraform** = NATS + secrets · **Kustomize/Tilt** = Postgres + your services.
CI (`.github/workflows/ci.yml`) runs `build + vet + test` on every push.

Manual:
```bash
TOKEN=$(curl -s -XPOST localhost:8080/login -d '{"user":"demo","pass":"demo"}' | jq -r .token)
curl -XPOST localhost:8080/orders -H "Authorization: Bearer $TOKEN" -d '{"item":"widget","qty":2}'
curl localhost:8080/orders/<id>  -H "Authorization: Bearer $TOKEN"
```

## Layout

```
services/{gateway,order,inventory,payment,notification}/main.go
internal/events/events.go   # shared subjects, payloads, JetStream setup
Dockerfile                  # one file, --build-arg SERVICE=...
deploy/k8s/                 # Postgres + services (kustomize)
deploy/terraform/           # NATS (Helm) + app Secret
Tiltfile · Makefile · .github/workflows/ci.yml
```

## Deliberate cuts

- Gateway↔order is HTTP reverse-proxy, **not gRPC** — gRPC is an opt-in upgrade (needs protoc).
- Shared Postgres DB (separate tables), not DB-per-service — comes with the Phase 4 AWS/RDS module.
- Postgres stays on Kustomize (single-instance dev DB doesn't need a Helm chart); no PVC.
- No Kustomize overlays yet (one env); no tracing/metrics — Phase 4.
- Payment is a mock rule; `charge()` is where a real gateway (Midtrans/Stripe) goes.
