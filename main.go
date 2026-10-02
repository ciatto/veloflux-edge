package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Config struct {
	ControlPlaneURL string
	EnrollmentToken string
	NodeToken       string
	NodeID          string
	Cluster         string
	NodeName        string
	Port            string
}

type RouteRule struct {
	Path      string   `json:"path"`
	Upstreams []string `json:"upstreams"`
	Strategy  string   `json:"strategy"`
}

type DynamicConfig struct {
	DefaultUpstream    *string             `json:"default_upstream,omitempty"`
	HostUpstreams      map[string][]string `json:"host_upstreams,omitempty"`
	Routes             []RouteRule         `json:"routes,omitempty"`
	PreserveHost       *bool               `json:"preserve_host,omitempty"`
	CustomDomainHeader *string             `json:"custom_domain_header,omitempty"`
	UpstreamUserAgent  *string             `json:"upstream_user_agent,omitempty"`
}

type Snapshot struct {
	Global     DynamicConfig            `json:"Global"`
	ByHost     map[string]DynamicConfig `json:"ByHost"`
	ByTenant   map[string]DynamicConfig `json:"ByTenant"`
	HostTenant map[string]string        `json:"HostTenant"`
}

type registerRequest struct {
	ClusterName  string `json:"cluster_name"`
	NodeName     string `json:"node_name"`
	APITokenHash string `json:"api_token_hash"`
	IsBYO        bool   `json:"is_byo"`
	IsManaged    bool   `json:"is_managed"`
}

type identityRequest struct {
	ClusterName string `json:"cluster_name"`
	NodeName    string `json:"node_name"`
}

type identityResponse struct {
	NodeID string `json:"node_id"`
}

type edge struct {
	cfg         Config
	bootstrapMu sync.Mutex
	nodeID      atomic.Value
	snapshot    atomic.Value
	rrCounter   atomic.Uint64
	client      *http.Client
	transport   *http.Transport
}

func main() {
	cfg, err := loadConfig()
	if err != nil {
		log.Fatal(err)
	}

	e := newEdge(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := e.bootstrap(ctx); err != nil {
		log.Printf("bootstrap pending: %v", err)
	}
	go e.controlLoop(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/readyz", e.ready)
	mux.HandleFunc("/__veloflux/vercel-api-authz", vercelAPIAuthzProbe)
	mux.HandleFunc("/", e.proxy)

	server := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       90 * time.Second,
		MaxHeaderBytes:    32 << 10,
	}

	log.Printf("VeloFlux managed edge listening on :%s", cfg.Port)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

func vercelAPIAuthzProbe(w http.ResponseWriter, r *http.Request) {
	if strings.TrimSpace(os.Getenv("VERCEL_ENV")) != "preview" {
		http.NotFound(w, r)
		return
	}
	token := strings.TrimSpace(os.Getenv("VERCEL_OIDC_TOKEN"))
	if token == "" {
		http.Error(w, "oidc token unavailable", http.StatusServiceUnavailable)
		return
	}
	const teamID = "team_gzSri4uwXAyphdWnL9LMxtdA"
	const projectID = "prj_lShBMMLmFVY0pHW0bPnNWTNAykI5"
	client := &http.Client{Timeout: 8 * time.Second}

	call := func(method, endpoint string, body io.Reader) int {
		req, err := http.NewRequestWithContext(r.Context(), method, endpoint, body)
		if err != nil {
			return 0
		}
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Accept", "application/json")
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		resp, err := client.Do(req)
		if err != nil {
			return 0
		}
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		return resp.StatusCode
	}

	getStatus := call(http.MethodGet,
		"https://api.vercel.com/v9/projects/"+projectID+"/domains?limit=1&teamId="+teamID, nil)
	postStatus := call(http.MethodPost,
		"https://api.vercel.com/v10/projects/"+projectID+"/domains?teamId="+teamID,
		strings.NewReader(`{"name":"invalid"}`))

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"oidc_present": true,
		"get_domains_status": getStatus,
		"invalid_add_domain_status": postStatus,
	})
}

func loadConfig() (Config, error) {
	cfg := Config{
		ControlPlaneURL: strings.TrimRight(strings.TrimSpace(os.Getenv("CONTROL_PLANE_URL")), "/"),
		EnrollmentToken: strings.TrimSpace(os.Getenv("EDGE_TOKEN")),
		NodeToken:       strings.TrimSpace(os.Getenv("EDGE_NODE_TOKEN")),
		NodeID:          strings.TrimSpace(os.Getenv("EDGE_NODE_ID")),
		Cluster:         strings.TrimSpace(os.Getenv("EDGE_CLUSTER")),
		NodeName:        strings.TrimSpace(os.Getenv("EDGE_NODE_NAME")),
		Port:            strings.TrimSpace(os.Getenv("PORT")),
	}
	if cfg.Port == "" {
		cfg.Port = "8080"
	}
	if _, err := strconv.Atoi(cfg.Port); err != nil {
		return Config{}, fmt.Errorf("invalid PORT")
	}
	if cfg.ControlPlaneURL == "" {
		return Config{}, fmt.Errorf("CONTROL_PLANE_URL is required")
	}
	if cfg.NodeToken == "" {
		return Config{}, fmt.Errorf("EDGE_NODE_TOKEN is required")
	}
	if cfg.Cluster == "" {
		return Config{}, fmt.Errorf("EDGE_CLUSTER is required")
	}
	if cfg.NodeName == "" {
		return Config{}, fmt.Errorf("EDGE_NODE_NAME is required")
	}
	if cfg.NodeID == "" && cfg.EnrollmentToken == "" {
		// Reattach can still succeed without enrollment. The explicit error is
		// delayed until reattach confirms that the logical node does not exist.
	}
	return cfg, nil
}

func newEdge(cfg Config) *edge {
	tr := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		MaxIdleConns:          1024,
		MaxIdleConnsPerHost:   128,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
		ForceAttemptHTTP2:     true,
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
	}
	e := &edge{
		cfg: cfg,
		client: &http.Client{
			Timeout:   15 * time.Second,
			Transport: tr,
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		transport: tr,
	}
	e.nodeID.Store(cfg.NodeID)
	e.snapshot.Store(&Snapshot{
		ByHost:     map[string]DynamicConfig{},
		ByTenant:   map[string]DynamicConfig{},
		HostTenant: map[string]string{},
	})
	return e
}

func (e *edge) bootstrap(ctx context.Context) error {
	e.bootstrapMu.Lock()
	defer e.bootstrapMu.Unlock()

	if e.currentNodeID() == "" {
		if err := e.reattach(ctx); err != nil {
			if !errors.Is(err, errNodeNotFound) {
				return err
			}
			if e.cfg.EnrollmentToken == "" {
				return fmt.Errorf("managed node not found and EDGE_TOKEN enrollment credential is missing")
			}
			if err := e.register(ctx); err != nil {
				return err
			}
		}
	}
	if err := e.syncConfig(ctx); err != nil {
		return err
	}
	return e.heartbeat(ctx)
}

var errNodeNotFound = errors.New("managed node not found")

func (e *edge) reattach(ctx context.Context) error {
	payload, _ := json.Marshal(identityRequest{ClusterName: e.cfg.Cluster, NodeName: e.cfg.NodeName})
	resp, err := e.doJSON(ctx, http.MethodPost, "/api/edge/reattach", e.cfg.NodeToken, payload, "")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusNotFound {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		return errNodeNotFound
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("reattach failed: HTTP %d", resp.StatusCode)
	}
	var out identityResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&out); err != nil {
		return err
	}
	if strings.TrimSpace(out.NodeID) == "" {
		return fmt.Errorf("reattach returned empty node_id")
	}
	e.nodeID.Store(strings.TrimSpace(out.NodeID))
	log.Printf("reattached logical node %s", out.NodeID)
	return nil
}

func (e *edge) register(ctx context.Context) error {
	sum := sha256.Sum256([]byte(e.cfg.NodeToken))
	payload, _ := json.Marshal(registerRequest{
		ClusterName:  e.cfg.Cluster,
		NodeName:     e.cfg.NodeName,
		APITokenHash: hex.EncodeToString(sum[:]),
		IsBYO:        false,
		IsManaged:    true,
	})
	resp, err := e.doJSON(ctx, http.MethodPost, "/api/edge/register", e.cfg.EnrollmentToken, payload, "")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusConflict {
		// A concurrent cold start may have won the one-time enrollment race.
		return e.reattach(ctx)
	}
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("registration failed: HTTP %d", resp.StatusCode)
	}
	var out identityResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&out); err != nil {
		return err
	}
	if strings.TrimSpace(out.NodeID) == "" {
		return fmt.Errorf("registration returned empty node_id")
	}
	e.nodeID.Store(strings.TrimSpace(out.NodeID))
	log.Printf("registered logical node %s", out.NodeID)
	return nil
}

func (e *edge) controlLoop(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if e.currentNodeID() == "" {
				if err := e.bootstrap(ctx); err != nil {
					log.Printf("bootstrap retry failed: %v", err)
				}
				continue
			}
			if err := e.syncConfig(ctx); err != nil {
				log.Printf("config sync failed: %v", err)
			}
			if err := e.heartbeat(ctx); err != nil {
				log.Printf("heartbeat failed: %v", err)
			}
		}
	}
}

func (e *edge) syncConfig(ctx context.Context) error {
	nodeID := e.currentNodeID()
	if nodeID == "" {
		return errNodeNotFound
	}
	resp, err := e.doJSON(ctx, http.MethodGet, "/api/edge/config", e.cfg.NodeToken, nil, nodeID)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("config sync failed: HTTP %d", resp.StatusCode)
	}
	var snap Snapshot
	if err := json.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(&snap); err != nil {
		return err
	}
	if snap.ByHost == nil {
		snap.ByHost = map[string]DynamicConfig{}
	}
	if snap.ByTenant == nil {
		snap.ByTenant = map[string]DynamicConfig{}
	}
	if snap.HostTenant == nil {
		snap.HostTenant = map[string]string{}
	}
	normalized := make(map[string]DynamicConfig, len(snap.ByHost))
	for host, cfg := range snap.ByHost {
		normalized[normalizeHost(host)] = cfg
	}
	snap.ByHost = normalized
	e.snapshot.Store(&snap)
	return nil
}

func (e *edge) heartbeat(ctx context.Context) error {
	nodeID := e.currentNodeID()
	if nodeID == "" {
		return errNodeNotFound
	}
	resp, err := e.doJSON(ctx, http.MethodPost, "/api/edge/heartbeat", e.cfg.NodeToken, []byte("{}"), nodeID)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("heartbeat failed: HTTP %d", resp.StatusCode)
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	return nil
}

func (e *edge) doJSON(ctx context.Context, method, path, token string, body []byte, nodeID string) (*http.Response, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, e.cfg.ControlPlaneURL+path, reader)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if nodeID != "" {
		req.Header.Set("X-Edge-Node-ID", nodeID)
	}
	req.Header.Set("X-VeloFlux-Managed-Ingress", "true")
	return e.client.Do(req)
}

func (e *edge) ready(w http.ResponseWriter, r *http.Request) {
	snap := e.currentSnapshot()
	if e.currentNodeID() == "" || len(snap.ByHost) == 0 {
		if err := e.bootstrap(r.Context()); err != nil {
			log.Printf("request-time bootstrap failed: %v", err)
		}
		snap = e.currentSnapshot()
	}
	if e.currentNodeID() == "" || len(snap.ByHost) == 0 {
		http.Error(w, "not-ready", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ready\n"))
}

func (e *edge) proxy(w http.ResponseWriter, r *http.Request) {
	snap := e.currentSnapshot()
	if e.currentNodeID() == "" || len(snap.ByHost) == 0 {
		if err := e.bootstrap(r.Context()); err != nil {
			log.Printf("request-time bootstrap failed: %v", err)
			http.Error(w, "edge not ready", http.StatusServiceUnavailable)
			return
		}
		snap = e.currentSnapshot()
	}

	host := normalizeHost(r.Host)
	cfg, ok := lookupHostConfig(snap.ByHost, host)
	if !ok {
		http.Error(w, "unknown host", http.StatusMisdirectedRequest)
		return
	}

	targets, strategy := selectTargets(cfg, host, r.URL.Path)
	if len(targets) == 0 {
		http.Error(w, "no upstream configured", http.StatusBadGateway)
		return
	}

	start := int(e.rrCounter.Add(1)-1) % len(targets)
	if strings.EqualFold(strategy, "failover") || len(targets) == 1 {
		start = 0
	}

	var lastErr error
	for attempt := 0; attempt < len(targets); attempt++ {
		idx := (start + attempt) % len(targets)
		target, err := url.Parse(targets[idx])
		if err != nil || (target.Scheme != "http" && target.Scheme != "https") || target.Host == "" {
			lastErr = fmt.Errorf("invalid upstream")
			continue
		}
		if err := e.serveTarget(w, r, target, cfg); err == nil {
			return
		} else {
			lastErr = err
		}
	}
	log.Printf("all upstreams failed host=%s err=%v", host, lastErr)
	http.Error(w, "upstream unavailable", http.StatusBadGateway)
}

func (e *edge) serveTarget(w http.ResponseWriter, r *http.Request, target *url.URL, cfg DynamicConfig) error {
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.Transport = e.transport
	proxy.FlushInterval = -1

	originalDirector := proxy.Director
	proxy.Director = func(req *http.Request) {
		originalDirector(req)
		if cfg.PreserveHost != nil && *cfg.PreserveHost {
			req.Host = r.Host
		}
		req.Header.Set("X-Forwarded-Host", r.Host)
		req.Header.Set("X-VeloFlux-Edge", e.cfg.NodeName)
		if id := e.currentNodeID(); id != "" {
			req.Header.Set("X-VeloFlux-Edge-ID", id)
		}
		if cfg.UpstreamUserAgent != nil && strings.TrimSpace(*cfg.UpstreamUserAgent) != "" {
			req.Header.Set("User-Agent", strings.TrimSpace(*cfg.UpstreamUserAgent))
		}
		if cfg.CustomDomainHeader != nil && strings.TrimSpace(*cfg.CustomDomainHeader) != "" {
			req.Header.Set(strings.TrimSpace(*cfg.CustomDomainHeader), normalizeHost(r.Host))
		}
	}

	proxy.ModifyResponse = func(resp *http.Response) error {
		resp.Header.Set("X-VeloFlux-Edge", e.cfg.NodeName)
		if id := e.currentNodeID(); id != "" {
			resp.Header.Set("X-VeloFlux-Edge-ID", id)
		}
		applyVercelCDNPolicy(resp, r)
		return nil
	}

	var proxyErr error
	proxy.ErrorHandler = func(_ http.ResponseWriter, _ *http.Request, err error) {
		proxyErr = err
	}
	proxy.ServeHTTP(w, r)
	return proxyErr
}

func applyVercelCDNPolicy(resp *http.Response, req *http.Request) {
	if resp == nil || req == nil {
		return
	}
	if req.Method != http.MethodGet && req.Method != http.MethodHead {
		return
	}
	if resp.StatusCode != http.StatusOK || len(resp.Header.Values("Set-Cookie")) > 0 {
		return
	}
	if strings.TrimSpace(resp.Header.Get("Vercel-CDN-Cache-Control")) != "" {
		return
	}

	policy := strings.TrimSpace(resp.Header.Get("VeloFlux-CDN-Cache-Control"))
	if policy == "" {
		policy = strings.TrimSpace(resp.Header.Get("Cloudflare-CDN-Cache-Control"))
	}
	if policy == "" {
		policy = strings.TrimSpace(resp.Header.Get("CDN-Cache-Control"))
	}
	if !cachePolicyPublic(policy) {
		return
	}

	resp.Header.Set("Vercel-CDN-Cache-Control", policy)
	if host := normalizeHost(req.Host); host != "" {
		resp.Header.Set("Vercel-Cache-Tag", "sendbot-viewer-"+sanitizeCacheTag(host))
	}
}

func cachePolicyPublic(policy string) bool {
	if strings.TrimSpace(policy) == "" {
		return false
	}
	public := false
	for _, raw := range strings.Split(policy, ",") {
		directive := strings.ToLower(strings.TrimSpace(strings.SplitN(raw, "=", 2)[0]))
		switch directive {
		case "private", "no-store":
			return false
		case "public":
			public = true
		}
	}
	return public
}

func sanitizeCacheTag(host string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(strings.TrimSpace(host)) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
		default:
			b.WriteByte('-')
		}
	}
	return strings.Trim(b.String(), "-")
}

func lookupHostConfig(configs map[string]DynamicConfig, host string) (DynamicConfig, bool) {
	host = normalizeHost(host)
	if host == "" {
		return DynamicConfig{}, false
	}
	if cfg, ok := configs[host]; ok {
		return cfg, true
	}

	bestLen := -1
	var best DynamicConfig
	found := false
	for rawPattern, cfg := range configs {
		pattern := normalizeHost(rawPattern)
		if pattern == "*" {
			if !found {
				best, bestLen, found = cfg, 0, true
			}
			continue
		}
		if !strings.HasPrefix(pattern, "*.") {
			continue
		}
		suffix := pattern[1:] // includes leading dot
		if !strings.HasSuffix(host, suffix) {
			continue
		}
		label := strings.TrimSuffix(host, suffix)
		if label == "" || strings.Contains(label, ".") {
			continue
		}
		if len(pattern) > bestLen {
			best, bestLen, found = cfg, len(pattern), true
		}
	}
	return best, found
}

type captureWriter struct {
	header http.Header
	body   bytes.Buffer
	status int
	err    error
}

func (w *captureWriter) Header() http.Header { return w.header }
func (w *captureWriter) WriteHeader(code int) {
	if w.status == 0 {
		w.status = code
	}
}
func (w *captureWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.body.Write(p)
}

func copyHeader(dst, src http.Header) {
	for k, vv := range src {
		if hopByHopHeader(k) {
			continue
		}
		for _, v := range vv {
			dst.Add(k, v)
		}
	}
}

func hopByHopHeader(k string) bool {
	switch strings.ToLower(k) {
	case "connection", "proxy-connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailer", "transfer-encoding", "upgrade":
		return true
	default:
		return false
	}
}

func selectTargets(cfg DynamicConfig, host, path string) ([]string, string) {
	for _, rule := range cfg.Routes {
		if pathMatches(rule.Path, path) && len(rule.Upstreams) > 0 {
			return cleanTargets(rule.Upstreams), rule.Strategy
		}
	}
	if cfg.HostUpstreams != nil {
		if targets := cleanTargets(cfg.HostUpstreams[host]); len(targets) > 0 {
			return targets, "round_robin"
		}
	}
	if cfg.DefaultUpstream != nil && strings.TrimSpace(*cfg.DefaultUpstream) != "" {
		return []string{strings.TrimSpace(*cfg.DefaultUpstream)}, "single"
	}
	return nil, ""
}

func pathMatches(pattern, path string) bool {
	pattern = strings.TrimSpace(pattern)
	if pattern == "" || pattern == "/" || pattern == "*" || pattern == "/*" {
		return true
	}
	if strings.HasSuffix(pattern, "/*") {
		return strings.HasPrefix(path, strings.TrimSuffix(pattern, "*"))
	}
	return path == pattern
}

func cleanTargets(in []string) []string {
	out := make([]string, 0, len(in))
	for _, s := range in {
		s = strings.TrimSpace(s)
		if s != "" {
			out = append(out, s)
		}
	}
	return out
}

func normalizeHost(raw string) string {
	raw = strings.ToLower(strings.TrimSpace(raw))
	if h, _, err := net.SplitHostPort(raw); err == nil {
		return strings.TrimSuffix(h, ".")
	}
	return strings.TrimSuffix(raw, ".")
}

func (e *edge) currentNodeID() string {
	if v := e.nodeID.Load(); v != nil {
		if s, ok := v.(string); ok {
			return strings.TrimSpace(s)
		}
	}
	return ""
}

func (e *edge) currentSnapshot() *Snapshot {
	if v := e.snapshot.Load(); v != nil {
		if s, ok := v.(*Snapshot); ok && s != nil {
			return s
		}
	}
	return &Snapshot{ByHost: map[string]DynamicConfig{}}
}
