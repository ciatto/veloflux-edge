package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func TestLookupHostConfigExactWinsOverWildcard(t *testing.T) {
	exact := DynamicConfig{DefaultUpstream: strptr("https://exact.example")}
	wild := DynamicConfig{DefaultUpstream: strptr("https://wild.example")}
	configs := map[string]DynamicConfig{
		"*.sendbot.chat":   wild,
		"bot.sendbot.chat": exact,
	}

	got, ok := lookupHostConfig(configs, "BOT.SENDBOT.CHAT:443")
	if !ok || got.DefaultUpstream == nil || *got.DefaultUpstream != "https://exact.example" {
		t.Fatalf("exact route did not win: %#v ok=%v", got, ok)
	}
}

func TestLookupHostConfigWildcardMatchesOneLabelOnly(t *testing.T) {
	wild := DynamicConfig{DefaultUpstream: strptr("https://viewer.example")}
	configs := map[string]DynamicConfig{"*.sendbot.chat": wild}

	for _, host := range []string{"demo.sendbot.chat", "DEMO.SENDBOT.CHAT:443"} {
		got, ok := lookupHostConfig(configs, host)
		if !ok || got.DefaultUpstream == nil || *got.DefaultUpstream != "https://viewer.example" {
			t.Fatalf("wildcard did not match %q: %#v ok=%v", host, got, ok)
		}
	}
	if _, ok := lookupHostConfig(configs, "a.b.sendbot.chat"); ok {
		t.Fatal("wildcard must not match nested labels")
	}
}

func TestManagedHeadersReachUpstream(t *testing.T) {
	var gotHost, gotUA, gotCustom string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHost = r.Header.Get("X-Forwarded-Host")
		gotUA = r.Header.Get("User-Agent")
		gotCustom = r.Header.Get("custom_domain")
		w.Header().Set("Content-Type", "text/plain")
		_, _ = io.WriteString(w, "viewer-ok")
	}))
	defer upstream.Close()

	custom := "custom_domain"
	ua := "Mozilla/5.0 (compatible; SendbotShield/1.0)"
	cfg := DynamicConfig{
		Routes:             []RouteRule{{Path: "/*", Upstreams: []string{upstream.URL}, Strategy: "failover"}},
		CustomDomainHeader: &custom,
		UpstreamUserAgent:  &ua,
	}

	e := newEdge(Config{NodeName: "vercel-edge-primary"})
	e.nodeID.Store("node-test")

	req := httptest.NewRequest(http.MethodGet, "https://demo.sendbot.chat/", nil)
	req.Host = "demo.sendbot.chat"
	rec := httptest.NewRecorder()

	targets, _ := selectTargets(cfg, req.Host, req.URL.Path)
	if len(targets) != 1 {
		t.Fatalf("targets=%v", targets)
	}
	target, err := parseURL(targets[0])
	if err != nil {
		t.Fatal(err)
	}
	if err := e.serveTarget(rec, req, target, cfg); err != nil {
		t.Fatalf("proxy: %v", err)
	}

	if rec.Code != http.StatusOK || strings.TrimSpace(rec.Body.String()) != "viewer-ok" {
		t.Fatalf("response status=%d body=%q", rec.Code, rec.Body.String())
	}
	if gotHost != "demo.sendbot.chat" {
		t.Fatalf("x-forwarded-host=%q", gotHost)
	}
	if gotCustom != "demo.sendbot.chat" {
		t.Fatalf("custom_domain=%q", gotCustom)
	}
	if gotUA != ua {
		t.Fatalf("user-agent=%q", gotUA)
	}
	if rec.Header().Get("X-VeloFlux-Edge-ID") != "node-test" {
		t.Fatalf("edge id header=%q", rec.Header().Get("X-VeloFlux-Edge-ID"))
	}
}

func TestPathMatchesManagedCatchAll(t *testing.T) {
	for _, pattern := range []string{"*", "/*", "/"} {
		if !pathMatches(pattern, "/anything/here") {
			t.Fatalf("pattern %q should match", pattern)
		}
	}
}

func strptr(v string) *string { return &v }

func parseURL(raw string) (*url.URL, error) {
	return url.Parse(raw)
}

func TestApplyVercelCDNPolicyPreservesBrowserNoStore(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "https://demo.sendbot.chat/?utm_source=test", nil)
	req.Host = "demo.sendbot.chat"
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header: http.Header{
			"Cache-Control":                []string{"no-store"},
			"Cloudflare-Cdn-Cache-Control": []string{"public, max-age=10, stale-while-revalidate=300, stale-if-error=86400"},
		},
	}

	applyVercelCDNPolicy(resp, req)

	if got := resp.Header.Get("Cache-Control"); got != "no-store" {
		t.Fatalf("browser cache-control changed: %q", got)
	}
	want := "public, max-age=10, stale-while-revalidate=300, stale-if-error=86400"
	if got := resp.Header.Get("Vercel-CDN-Cache-Control"); got != want {
		t.Fatalf("vercel cache policy=%q want=%q", got, want)
	}
	if got := resp.Header.Get("Vercel-Cache-Tag"); got != "sendbot-viewer-demo-sendbot-chat" {
		t.Fatalf("cache tag=%q", got)
	}
}

func TestApplyVercelCDNPolicyRejectsPrivateAndSetCookie(t *testing.T) {
	for _, tc := range []struct {
		name   string
		policy string
		cookie bool
	}{
		{name: "private", policy: "private, max-age=60"},
		{name: "no-store", policy: "public, no-store, max-age=60"},
		{name: "set-cookie", policy: "public, max-age=60", cookie: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "https://demo.sendbot.chat/", nil)
			resp := &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Cloudflare-Cdn-Cache-Control": []string{tc.policy}},
			}
			if tc.cookie {
				resp.Header.Add("Set-Cookie", "session=secret")
			}
			applyVercelCDNPolicy(resp, req)
			if got := resp.Header.Get("Vercel-CDN-Cache-Control"); got != "" {
				t.Fatalf("unexpected Vercel cache policy %q", got)
			}
		})
	}
}

func TestApplyVercelCDNPolicyKeepsExplicitVercelPolicy(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "https://demo.sendbot.chat/", nil)
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header: http.Header{
			"Vercel-Cdn-Cache-Control":     []string{"public, max-age=120"},
			"Cloudflare-Cdn-Cache-Control": []string{"public, max-age=10"},
		},
	}
	applyVercelCDNPolicy(resp, req)
	if got := resp.Header.Get("Vercel-CDN-Cache-Control"); got != "public, max-age=120" {
		t.Fatalf("explicit Vercel policy overwritten: %q", got)
	}
}
