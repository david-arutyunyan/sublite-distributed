# Sublite Distributed on Kubernetes (kind)

Manifests for the same distributed system `docker-compose.yml` runs
locally, ported to plain Kubernetes resources: Namespace, ConfigMap,
Secret, Deployment, Service, probes (step 11a), plus Ingress and HPA
(step 11b) with the cluster add-ons they need - ingress-nginx and
metrics-server.

Deliberately scoped to the CORE distributed system only - Kafka, both
Postgres instances, Mongo, and the three application services. The
observability stack from steps 9-10 (Prometheus, Grafana, Jaeger) stays
docker-compose-only: re-deploying an entire second observability stack
onto a second platform is scope bloat this step doesn't need to take on
to demonstrate Deployment/Service/ConfigMap/Secret/probes/HPA properly.

## Prerequisites

- Docker Desktop (or another Docker daemon `kind` can talk to)
- [`kind`](https://kind.sigs.k8s.io/) - not a standard install; see below
- `kubectl`

If your shell has `HTTP_PROXY`/`HTTPS_PROXY` pointed at `127.0.0.1:<port>`
(common with local proxy tools), **fix this before creating the
cluster** - see the gotcha below. `kind` bakes the proxy env vars from
the *calling shell* into the node container at creation time, and
`127.0.0.1` inside that container is the container's OWN loopback, not
the host's.

```bash
# If needed, point the proxy at host.docker.internal instead of 127.0.0.1
# BEFORE creating the cluster:
export HTTP_PROXY="http://host.docker.internal:<port>"
export HTTPS_PROXY="http://host.docker.internal:<port>"
export NO_PROXY="localhost,127.0.0.1,::1,.local,10.96.0.0/16,10.244.0.0/16,.svc,.svc.cluster,.svc.cluster.local"
```

## Bring it up

```bash
# 1. Build the three app images the normal way (docker-compose.yml's
#    own build contexts - this step doesn't duplicate them).
docker compose build subscription-service billing-service notification-service

# 2. Create the cluster.
kind create cluster --config kind-config.yaml

# 3. Load the locally-built images into it - kind clusters don't have
#    a path to a registry these came from, they only exist as local
#    Docker images.
kind load docker-image \
  sublite-distributed-subscription-service:latest \
  sublite-distributed-billing-service:latest \
  sublite-distributed-notification-service:latest \
  --name sublite

# 4. Apply the core system.
kubectl apply -f k8s/00-namespace.yaml
kubectl apply -f k8s/01-kafka.yaml
kubectl apply -f k8s/02-kafka-init-job.yaml
kubectl apply -f k8s/03-subscription-postgres.yaml
kubectl apply -f k8s/04-billing-postgres.yaml
kubectl apply -f k8s/05-notification-mongo.yaml
kubectl apply -f k8s/06-subscription-service.yaml
kubectl apply -f k8s/07-billing-service.yaml
kubectl apply -f k8s/08-notification-service.yaml

# 5. Install ingress-nginx (kind's own deploy manifest, already wired for
#    kind's node ports - this is why kind-config.yaml's extraPortMappings
#    and ingress-ready label had to exist BEFORE the cluster was created).
kubectl apply -f https://raw.githubusercontent.com/kubernetes/ingress-nginx/controller-v1.11.3/deploy/static/provider/kind/deploy.yaml
kubectl wait --namespace ingress-nginx \
  --for=condition=ready pod \
  --selector=app.kubernetes.io/component=controller \
  --timeout=180s

# 6. Install metrics-server (HPA needs it for CPU numbers) - and patch it
#    for kind's self-signed kubelet certs, which a stock metrics-server
#    doesn't trust.
kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml
kubectl patch deployment metrics-server -n kube-system --type='json' \
  -p='[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--kubelet-insecure-tls"}]'

# 7. Apply Ingress + HPA.
kubectl apply -f k8s/09-ingress.yaml
kubectl apply -f k8s/10-hpa.yaml

# 8. Watch it come up (takes a couple of minutes - Postgres/Kafka/Mongo
#    all need to actually start before the app services' init
#    containers let them proceed).
kubectl get pods -n sublite -w
```

## Verify

Through the Ingress - no port-forward needed, kind-config.yaml's
`extraPortMappings` already put ports 80/443 on `localhost`:

```bash
curl -X POST http://localhost/subscription-service/subscriptions \
  -H "Content-Type: application/json" \
  -d '{"customerId":"11111111-2222-3333-4444-555555555555","planPriceId":"33333333-3333-3333-3333-333333333333"}'

kubectl exec -n sublite deploy/subscription-postgres -- \
  psql -U subscription -d subscription -c "select id, status from subscriptions;"
```

The path prefix (`/subscription-service/...`) is stripped by the
Ingress's `rewrite-target` before subscription-service ever sees the
request - it still just sees `POST /subscriptions`.

Or, without the Ingress, the same `kubectl port-forward` approach from
step 11a still works exactly as before:

```bash
kubectl port-forward -n sublite svc/subscription-service 8081:8081
```

HPA - `kubectl get hpa -n sublite` shows `<unknown>` for the CPU target
until metrics-server has completed at least one scrape cycle (a few tens
of seconds after it becomes Ready), then a real percentage:

```bash
kubectl get hpa -n sublite
kubectl top pods -n sublite
```

## Real gotchas hit standing this up (not hypothetical)

1. **The proxy issue above** - `kind` copies `HTTP_PROXY`/`HTTPS_PROXY`
   from the shell that runs `kind create cluster` into the node
   container's environment. If that proxy is bound to `127.0.0.1`, it's
   unreachable from inside the node (a different network namespace) -
   every image pull failed with `ImagePullBackOff` /
   `proxyconnect ... connection refused` until the proxy address was
   changed to `host.docker.internal`.

2. **`kind load docker-image` failing on multi-arch images** -
   `postgres:17`, `mongo:7.0`, and `apache/kafka:3.8.0` all failed to
   load with `content digest ... not found`, even loaded one at a time.
   Root cause: `kind load` always imports `--all-platforms`, but a
   normal `docker pull` only fetches the current platform's layers -
   importing the full multi-arch manifest list fails on the platforms
   whose blobs were never pulled. Fixed by giving the (proxy-fixed) kind
   node direct internet access instead and letting Kubernetes pull these
   itself, rather than pre-loading them.

3. **A self-referencing single-broker Kafka deadlock.** This one took
   two attempts to fix. Kafka's own `KAFKA_CONTROLLER_QUORUM_VOTERS`
   points at `kafka:19093` (the Service) - this single broker needs to
   reach *itself* through the Service to complete its own controller
   registration. A normal `ClusterIP` Service only ever has endpoints for
   *ready* pods, but this pod can't become ready until that
   registration succeeds - a dependency cycle with no way out.
   - First fix attempt: make the Service headless (`clusterIP: None`).
     Didn't fully work - headless Services still gate DNS answers on
     readiness by default, so an un-ready pod still had no DNS record at
     all (`UnknownHostException: kafka`).
   - Actual fix: **also** set `publishNotReadyAddresses: true`, which is
     what makes DNS answer with the pod's IP even before its
     `readinessProbe` has ever passed once.

4. **Probe `timeoutSeconds` defaulting to 1 second.** Kafka's readiness/
   liveness probe (`kafka-broker-api-versions.sh`) and Mongo's
   (`mongosh --eval ...`) both spin up a whole runtime (JVM, Node.js)
   per invocation - routinely slower than Kubernetes' default 1-second
   probe timeout, especially under a laptop-hosted kind cluster's CPU
   contention. The result was a real outage: an otherwise-healthy broker
   got killed and restarted repeatedly because the CHECK COMMAND timed
   out, not because the broker was actually unhealthy. Fixed with an
   explicit `timeoutSeconds: 10` (Kafka) / `5` (Mongo) - easy to miss
   since every other probe field was already set explicitly except this
   one.

Both a headless-vs-normal Service and a probe timeout are the kind of
thing that's invisible in a docker-compose setup (no Service abstraction,
no probe timeout to configure) and only shows up once the exact same
system moves to Kubernetes - which is arguably the more interesting
lesson from this step than the manifests themselves.

5. **`livenessProbe.initialDelaySeconds` too tight for JVM startup under
   kind's CPU contention (step 11b).** After recreating the cluster for
   Ingress support, subscription-service and billing-service both got
   stuck in a genuine restart loop. `kubectl logs --previous` showed the
   full story: `Started SubscriptionServiceApplication in 55.091 seconds`
   immediately followed, one second later, by `Commencing graceful
   shutdown` - the pod was killed right as it finally became healthy.
   With `initialDelaySeconds: 30` and the default `failureThreshold: 3`
   on a 15s period, kubelet gives up at ~75s - not enough when this JVM
   (Kafka consumer group join, JPA, the OTel javaagent's own bytecode
   instrumentation at startup) was observed taking 55-75s just to finish
   Spring Boot startup on a laptop-hosted cluster juggling Kafka, two
   Postgres instances, and Mongo at the same time. Different from gotcha
   #4 above - that was one slow PROBE COMMAND; this is the total time
   BEFORE the first probe can succeed at all. Fixed with
   `initialDelaySeconds: 60` + `failureThreshold: 5` (~135s of grace) on
   both services' liveness probes. Readiness was left alone - a failed
   readiness check just withholds traffic, it doesn't kill the pod, so
   there was no equivalent risk there.
