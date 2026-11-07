# Kubernetes manifests (M7.2–M7.4)

Namespace: **`payment-platform`**

| File | Resources |
|------|-----------|
| `00-namespace.yaml` | Namespace `payment-platform` |
| `01-configmap.yaml` | Non-secret app env (DB URL, Redis, Kafka bootstrap, probes flag) |
| `02-secret.yaml` | `DB_USER` / `DB_PASSWORD` (**placeholders — set before apply**) |
| `03-postgres.yaml` | Headless Service + StatefulSet + **2Gi** PVC |
| `04-redis.yaml` | ClusterIP Service + Deployment (**no** PVC) |
| `05-kafka.yaml` | Headless Service + StatefulSet (KRaft, 1 replica) + **2Gi** PVC |
| `06-app.yaml` | App **Deployment** + **ClusterIP Service** (`distributed-payment-platform`) |

**Not in these phases:** Ingress, NodePort/LoadBalancer, Jaeger, Grafana.

## Isolation from Docker Compose

- Postgres and Kafka use **Kubernetes PVCs** from StatefulSet `volumeClaimTemplates`.
- Manifests do **not** use `hostPath` or Compose volume names.
- Compose and kind data directories are **independent**.

## Resource sizing (laptop-oriented)

| Component | Requests | Limits | Notes |
|-----------|----------|--------|-------|
| PostgreSQL | 100m / 256Mi | 500m / 512Mi | |
| Redis | 50m / 64Mi | 200m / 128Mi | |
| Kafka | 250m / 512Mi | 500m / 1Gi | Heap `-Xms256m -Xmx256m` |
| **App** | **250m / 384Mi** | **750m / 768Mi** | Image JVM `MaxRAMPercentage=75` |

## Prerequisites before the app

1. Image loaded into kind: `distributed-payment-platform:0.1.0` (`imagePullPolicy: IfNotPresent`).
2. Postgres, Redis, and Kafka pods Ready in `payment-platform`.
3. ConfigMap `payment-platform-config` and Secret `payment-platform-secret` present.


## Configure the Secret before deployment

`k8s/02-secret.yaml` ships with **placeholder** values (`REPLACE_ME_DB_USER` / `REPLACE_ME_DB_PASSWORD`).

1. Edit `k8s/02-secret.yaml` and set real `DB_USER` and `DB_PASSWORD` values (do not commit secrets).
2. Or create/override the Secret out-of-band, e.g.:

```bash
kubectl -n payment-platform create secret generic payment-platform-secret \
  --from-literal=DB_USER='your-user' \
  --from-literal=DB_PASSWORD='your-password' \
  --dry-run=client -o yaml | kubectl apply -f -
```

Key names must remain `DB_USER` and `DB_PASSWORD` so the app Deployment and Postgres StatefulSet resolve them.

## Apply order (when approved — do not run until instructed)

```bash
kubectl apply -f k8s/00-namespace.yaml
kubectl apply -f k8s/01-configmap.yaml
kubectl apply -f k8s/02-secret.yaml
kubectl apply -f k8s/03-postgres.yaml
kubectl apply -f k8s/04-redis.yaml
kubectl apply -f k8s/05-kafka.yaml
kubectl apply -f k8s/06-app.yaml
```

Or: `kubectl apply -f k8s/`

### Dry-run

```bash
kubectl apply --dry-run=client -f k8s/06-app.yaml
kubectl apply --dry-run=server -f k8s/06-app.yaml
```

## Application (M7.4)

| Item | Value |
|------|--------|
| Deployment / Service | `distributed-payment-platform` |
| Image | `distributed-payment-platform:0.1.0` |
| Replicas | 1 |
| Service type | ClusterIP port **8080** → container port **http** |
| Env | `envFrom` ConfigMap + Secret (no credentials in the Deployment YAML) |
| Security | `runAsUser/Group 1001`, `allowPrivilegeEscalation: false`, drop **ALL** caps, `seccompProfile: RuntimeDefault` |
| Probes | startup/liveness `/actuator/health/liveness`, readiness `/actuator/health/readiness` |

### Inspect

```bash
kubectl -n payment-platform get deploy,svc,pods -l app.kubernetes.io/name=distributed-payment-platform
kubectl -n payment-platform describe deploy/distributed-payment-platform
kubectl -n payment-platform get events --sort-by='.lastTimestamp'
kubectl -n payment-platform logs -l app.kubernetes.io/name=distributed-payment-platform -f
```

### Health from inside the cluster

```bash
kubectl -n payment-platform run curl --rm -it --restart=Never --image=curlimages/curl:8.5.0 -- \
  curl -sS http://distributed-payment-platform:8080/actuator/health
```

### Port-forward for local access

```bash
kubectl -n payment-platform port-forward svc/distributed-payment-platform 8080:8080
# then: curl -s http://127.0.0.1:8080/actuator/health
```

## Kafka notes (M7.3)

- Image: `apache/kafka:3.7.1`; KRaft single node; bootstrap `kafka:9092`.
- Probes: **tcpSocket :9092** (lightweight for constrained kind nodes).
- PVC: 2Gi `standard` at `/var/lib/kafka/data`.

## Actuator probes

ConfigMap sets `MANAGEMENT_ENDPOINT_HEALTH_PROBES_ENABLED=true` so the app exposes Kubernetes liveness/readiness groups without changing `application.yml`.
