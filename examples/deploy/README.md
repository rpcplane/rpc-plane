# Deployment examples

Ready-to-apply manifests for running RPC Plane in common environments. The
**[deployment guide](https://docs.rpcplane.dev/guides/deployment/)** is the
narrative version of these files — start there if you want the explanation; come
here when you just want something to `apply`.

| Path | Platform | Listen mode |
|------|----------|-------------|
| [`docker-compose/`](docker-compose/) | Docker Compose (+ optional Prometheus/Grafana) | TCP port |
| [`kubernetes/deployment.yaml`](kubernetes/deployment.yaml) | Kubernetes — shared Deployment + Service | TCP port |
| [`kubernetes/sidecar.yaml`](kubernetes/sidecar.yaml) | Kubernetes — per-pod sidecar | **Unix socket** |
| [`kubernetes/servicemonitor.yaml`](kubernetes/servicemonitor.yaml) | Prometheus Operator scrape config | — |
| [`nomad/rpc-plane.nomad.hcl`](nomad/rpc-plane.nomad.hcl) | Nomad (docker driver, Consul service) | TCP port |
| [`systemd/`](systemd/) | Bare metal / VM | TCP port or Unix socket |

## Port vs socket

- **TCP port** — reachable over the network; required when the proxy and its
  clients are on different hosts/pods. The default.
- **Unix socket** — no network port, lower loopback overhead; only works when
  the client is on the *same host or pod* (Compose shared volume, K8s sidecar,
  systemd on the same box). See the [Unix socket guide](https://docs.rpcplane.dev/guides/unix-socket/).

RPC Plane is an internal sidecar — keep `9400`/`9401` private (ClusterIP,
loopback, or a private network), never exposed to the public internet.

## Quick start

```bash
# Docker Compose
cd docker-compose && cp .env.example .env   # add your keys
docker compose up -d

# Kubernetes (shared deployment)
kubectl apply -f kubernetes/deployment.yaml

# Nomad
nomad job run nomad/rpc-plane.nomad.hcl
```

Every manifest uses `${HELIUS_API_KEY}`-style references for provider keys —
supply them as container/process env vars and RPC Plane expands them when it
loads the config. Run `rpc-plane check` to confirm none expanded to empty.
