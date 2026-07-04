KUBE_CONTEXT ?= minikube
TF := terraform -chdir=deploy/terraform

# Provision platform (NATS via Helm + app Secret). Run before `up`.
tf-up:
	$(TF) init -input=false
	$(TF) apply -auto-approve -var kube_context=$(KUBE_CONTEXT)

tf-down:
	$(TF) destroy -auto-approve -var kube_context=$(KUBE_CONTEXT)

up:      ## build images + deploy Postgres and services
	tilt up

demo:
	./demo.sh

test:
	go build ./... && go vet ./... && go test ./...

proto:   ## regenerate gRPC stubs from proto/order.proto
	protoc --go_out=. --go_opt=module=order-system \
	       --go-grpc_out=. --go-grpc_opt=module=order-system proto/order.proto

.PHONY: tf-up tf-down up demo test proto
