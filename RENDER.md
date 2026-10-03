# Render Free edge canary

This deployment uses the existing public standalone managed-edge runtime, not the private Control Plane repository. No native/Vercel runtime setting or production route is changed by this Blueprint.

## Create the service

Import this branch into a new Render Blueprint using `render.yaml`. Confirm the resource name is unused and the creation summary says Free ($0 compute). The Blueprint creates exactly one Docker Web Service in Frankfurt, disables automatic deploys/previews, sets PORT=10000 and creates no database, disk, worker, cron job or custom domain.

Use a dedicated, non-production VeloFlux cluster for EDGE_CLUSTER. Enter its one-time enrollment token as EDGE_TOKEN directly in Render. EDGE_NODE_TOKEN is generated once by Render (256 random bits); keep that value unchanged across sleep/restart/redeploy. Never put either token into Git. Remove EDGE_TOKEN after successful enrollment, config sync and a tested stateless reattach.

Render terminates public TLS. This runtime already listens on the injected PORT and does not require a new VELOFLUX_RUNTIME value or local ACME/certificate storage. GOMAXPROCS=1 and GOMEMLIMIT=384MiB leave headroom within the free instance; GOMEMLIMIT is a Go soft limit, not a hard process memory cap.

## Validation and isolation

- `/healthz` is Render's process-liveness check, NOT certification that routing is ready.
- After enrollment and assignment of only a test host, `/readyz` must return 200.
- Validate the canary route, query/UTM preservation, X-VeloFlux-Edge-ID and rejection of unknown hosts.
- Restart the service without the enrollment token; it must reattach as the same node.
- Do not add this free node to production DNS, shared production clusters, weighted routing or automatic failover. This Blueprint does not do any of those actions.
- Do not mark WebSocket/SSE, production load or provider cache behavior as certified by the unit tests. They require real Render tests.

## Free-plan limits and spend gate

As checked on 2026-10-03, free web services sleep after 15 minutes without inbound traffic and can take about a minute to wake. Local files are ephemeral; 750 free instance-hours are shared per workspace. Free web services do not include Render edge caching, persistent disks or SSH. The existing Vercel CDN response headers do not create a Render cache.

Free compute is not a guarantee of zero invoice when a payment method is already attached: bandwidth/build overages can be billed. Before creation, inspect the workspace's current included usage and billing settings; do not upgrade, add a payment method, raise spending limits or enable paid extras. Do not add synthetic keep-alive traffic. Start with bounded test requests only. High service-initiated outbound traffic can also lead to free-service suspension.

Official references:
- https://render.com/docs/free
- https://render.com/docs/blueprint-spec
- https://render.com/docs/web-services
- https://render.com/docs/faq

## Verification in the repository

`go test -race ./... -count=1` includes Render port, first enrollment, stateless sleep/wake identity reuse, authenticated config/heartbeat, readiness, proxy query preservation and fail-closed routing tests, against local mock servers only. The Docker build executes all *_test.go files. Local tests/build are not evidence that a Render service exists.

## Rollback

Keep existing deployments unchanged. For a deployed canary, remove any test-host assignment, revoke only its node credentials, then suspend/delete only the newly created Render service. Do not remove the shared public runtime repository or modify existing Vercel services.
