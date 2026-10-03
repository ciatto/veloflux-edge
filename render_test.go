package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestRenderRuntimeUsesPlatformPort(t *testing.T) {
	t.Setenv("CONTROL_PLANE_URL", "https://control.example")
	t.Setenv("EDGE_NODE_TOKEN", "test-render-stable")
	t.Setenv("EDGE_CLUSTER", "render-free-canary")
	t.Setenv("EDGE_NODE_NAME", "render-free-fra-20261003")
	t.Setenv("EDGE_NODE_ID", "")
	t.Setenv("EDGE_TOKEN", "")
	t.Setenv("PORT", "10000")
	cfg, err := loadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Port != "10000" {
		t.Fatalf("platform port = %q", cfg.Port)
	}
	if cfg.NodeName != "render-free-fra-20261003" {
		t.Fatalf("node name = %q", cfg.NodeName)
	}
}

func TestRenderSleepWakeReattachesAndPreservesRouting(t *testing.T) {
	const nodeID = "test-render-node"
	const secret = "test-render-stable"
	var registered atomic.Bool
	var enrollments, reattaches atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		io.WriteString(w, r.URL.RequestURI())
	}))
	defer upstream.Close()
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wantAuth := "Bearer " + secret
		if r.URL.Path == "/api/edge/register" {
			wantAuth = "Bearer test-once"
		}
		if r.Header.Get("Authorization") != wantAuth {
			t.Errorf("unexpected credential for %s", r.URL.Path)
			http.Error(w, "unauthorized", 401)
			return
		}
		if r.Header.Get("X-VeloFlux-Managed-Ingress") != "true" {
			t.Errorf("managed ingress header missing for %s", r.URL.Path)
		}
		switch r.URL.Path {
		case "/api/edge/reattach":
			reattaches.Add(1)
			if !registered.Load() {
				http.Error(w, "not found", 404)
				return
			}
			json.NewEncoder(w).Encode(identityResponse{NodeID: nodeID})
		case "/api/edge/register":
			var req registerRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Error(err)
			}
			if !req.IsManaged || req.IsBYO || req.NodeName != "render-free-fra-20261003" {
				t.Errorf("invalid registration: %+v", req)
			}
			if req.APITokenHash != fmt.Sprintf("%x", sha256.Sum256([]byte(secret))) {
				t.Error("node token must be registered as SHA-256 only")
			}
			enrollments.Add(1)
			registered.Store(true)
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(identityResponse{NodeID: nodeID})
		case "/api/edge/config":
			if r.Header.Get("X-Edge-Node-ID") != nodeID {
				t.Error("wrong config node identity")
			}
			json.NewEncoder(w).Encode(Snapshot{ByHost: map[string]DynamicConfig{
				"render-canary.example": {DefaultUpstream: strptr(upstream.URL)},
			}})
		case "/api/edge/heartbeat":
			if r.Header.Get("X-Edge-Node-ID") != nodeID {
				t.Error("wrong heartbeat node identity")
			}
			w.WriteHeader(http.StatusOK)
		default:
			t.Errorf("unexpected control path %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer control.Close()
	cfg := Config{ControlPlaneURL: control.URL, NodeToken: secret, EnrollmentToken: "test-once",
		Cluster: "render-free-canary", NodeName: "render-free-fra-20261003", Port: "10000"}
	first := newEdge(cfg)
	defer first.transport.CloseIdleConnections()
	if err := first.bootstrap(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Simulate ephemeral filesystem and process loss: no node ID or enrollment token.
	cfg.EnrollmentToken = ""
	awake := newEdge(cfg)
	defer awake.transport.CloseIdleConnections()
	if err := awake.bootstrap(context.Background()); err != nil {
		t.Fatal(err)
	}
	if awake.currentNodeID() != nodeID || enrollments.Load() != 1 || reattaches.Load() != 2 {
		t.Fatalf("identity was not reused: node=%q enrollments=%d reattaches=%d",
			awake.currentNodeID(), enrollments.Load(), reattaches.Load())
	}
	ready := httptest.NewRecorder()
	awake.ready(ready, httptest.NewRequest("GET", "http://edge.example/readyz", nil))
	if ready.Code != 200 {
		t.Fatalf("ready status=%d", ready.Code)
	}
	req := httptest.NewRequest("GET", "https://render-canary.example/chat?utm_source=render&test=1", nil)
	rec := httptest.NewRecorder()
	awake.proxy(rec, req)
	if rec.Code != 200 || strings.TrimSpace(rec.Body.String()) != "/chat?utm_source=render&test=1" {
		t.Fatalf("proxy lost path/query: status=%d body=%q", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("X-VeloFlux-Edge-ID") != nodeID || rec.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("identity or no-store headers were not preserved")
	}
	unknown := httptest.NewRecorder()
	awake.proxy(unknown, httptest.NewRequest("GET", "https://unknown.example/", nil))
	if unknown.Code != http.StatusMisdirectedRequest {
		t.Fatalf("unknown host status=%d", unknown.Code)
	}
}
