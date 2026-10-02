# VeloFlux Edge on Fastly Compute

This deployment runs a Fastly-native VeloFlux data-plane adapter compiled to
WebAssembly. It uses the same VeloFlux Control Plane contract as the managed
Vercel edge while using Fastly-native request/backend primitives.

## Runtime model

Fastly owns the public listener and TLS. The VeloFlux adapter:

- runs as `wasip1/wasm`;
- registers or reattaches to one stable logical VeloFlux managed node;
- fetches the normal VeloFlux host-routing snapshot;
- refreshes configuration opportunistically while serving requests;
- sends heartbeats without requiring a background process;
- creates a Fastly dynamic backend for the selected VeloFlux origin;
- fails closed for unknown hosts;
- supports exact and one-label wildcard host assignments;
- supports round-robin/failover for idempotent requests;
- exposes `/healthz` and `/readyz`;
- returns `X-VeloFlux-Edge`, `X-VeloFlux-Edge-ID`, and
  `X-VeloFlux-Provider: fastly`.

No database, billing credential, Fastly API token, or VeloFlux administrative
credential is embedded in the WASM artifact.

## Fastly service resources

Create/link these resources to the Compute service.

### Static backend

Link name:

`veloflux_control_plane`

Target:

`https://api.veloflux.io`

### Config Store

Link name:

`veloflux_config`

Keys:

- `control_plane_url=https://api.veloflux.io`
- `edge_cluster=<VeloFlux cluster>`
- `edge_node_name=fastly-edge-primary`
- `edge_node_id=<optional known node id>`

### Secret Store

Link name:

`veloflux_secrets`

Secrets:

- `edge_node_token` — stable random data-plane secret unique to this logical
  Fastly edge.
- `edge_token` — one-time VeloFlux enrollment credential. It is needed only
  when the logical node does not yet exist.

After first successful enrollment and reattach validation, the one-time
`edge_token` may be removed from the Fastly Secret Store.

## Build

```bash
GOOS=wasip1 GOARCH=wasm go build -trimpath -ldflags="-s -w" \
  -o bin/main.wasm ./cmd/fastly
```

Or:

```bash
fastly compute build
```

## Deploy

After Fastly Compute is enabled on the account:

```bash
fastly compute publish
```

The assigned `*.edgecompute.app` hostname should be used for the first
end-to-end validation. Do not attach Sendbot/VeloFlux production domains until
all readiness gates below pass.

## Required Fastly entitlement

The runtime uses Fastly dynamic backends because VeloFlux origin selection is
controlled by the VeloFlux routing snapshot. The Fastly Compute service must be
allowed to create dynamic backends. If Fastly returns
`ErrDynamicBackendDisallowed`, enable the capability for the account/service
before routing production traffic.

## Validation gates

1. `fastly compute build` succeeds.
2. Assigned `*.edgecompute.app/healthz` returns 200.
3. The runtime registers/reattaches to exactly one managed VeloFlux node.
4. `/readyz` returns 200 after receiving a non-empty routing snapshot.
5. A test VeloFlux hostname reaches the intended origin.
6. Response includes `X-VeloFlux-Provider: fastly` and the logical edge ID.
7. Unknown host returns 421.
8. Idempotent-origin failover is validated.
9. POST/PUT/PATCH bodies are validated end-to-end.
10. Dynamic-backend entitlement is confirmed.
11. WebSocket/SSE behavior is validated before realtime production traffic.
12. Only after these gates should a production hostname be bound to Fastly.

## Security boundaries

- DevTools/API credentials for provisioning Fastly are not runtime secrets.
- VeloFlux node credentials live in Fastly Secret Store.
- Non-secret node/provider metadata lives in Fastly Config Store.
- The adapter accepts routing only from the authenticated VeloFlux Control
  Plane.
- Request bodies are bounded to 10 MiB.
- Non-idempotent requests are never replayed to a failover origin.
- TLS certificate hostname validation is enabled for HTTPS dynamic backends.

## Rollback

Fastly is additive. To remove it from traffic:

1. remove/drain the Fastly managed placement from the VeloFlux Control Plane;
2. confirm native/Vercel edges continue serving traffic;
3. revoke the Fastly logical node;
4. remove provider hostname/domain bindings;
5. delete or disable the Fastly Compute service if desired.
