# Phase 3 local infra-as-code. Provisions the platform pieces onto the current
# cluster (minikube): NATS via its Helm chart, and an app Secret with a generated
# JWT signing key. Postgres + the services themselves stay on Kustomize/Tilt.
#
# Workflow: `terraform apply` (this) THEN `tilt up`. The services mount the Secret
# created here, so it must exist first.

terraform {
  required_providers {
    kubernetes = { source = "hashicorp/kubernetes", version = "~> 2.30" }
    helm       = { source = "hashicorp/helm", version = "~> 2.17" }
    random     = { source = "hashicorp/random", version = "~> 3.6" }
  }
}

variable "kube_context" {
  type        = string
  default     = "minikube"
  description = "kubeconfig context to deploy into"
}

provider "kubernetes" {
  config_path    = "~/.kube/config"
  config_context = var.kube_context
}

provider "helm" {
  kubernetes {
    config_path    = "~/.kube/config"
    config_context = var.kube_context
  }
}

# JWT signing key, generated here so it never lives in git or a manifest.
resource "random_password" "jwt" {
  length  = 48
  special = false
}

resource "kubernetes_secret" "app" {
  metadata { name = "app" }
  data = {
    JWT_SECRET = random_password.jwt.result
    # dev Postgres runs in-cluster with a dev password; the URL lives here so
    # services get it from one place. Real DB creds land in Phase 4 (RDS).
    DATABASE_URL = "postgres://postgres:postgres@postgres:5432/app"
  }
}

# NATS with JetStream, from the official chart. Service is named after the release
# ("nats"), so clients reach it at nats://nats:4222 — matching NATS_URL.
resource "helm_release" "nats" {
  name       = "nats"
  repository = "https://nats-io.github.io/k8s/helm/charts/"
  chart      = "nats"

  set {
    name  = "config.jetstream.enabled"
    value = "true"
  }
}
