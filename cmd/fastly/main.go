//go:build wasip1

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
	"net"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fastly/compute-sdk-go/configstore"
	"github.com/fastly/compute-sdk-go/fsthttp"
	"github.com/fastly/compute-sdk-go/secretstore"
)

const (
	controlBackend  = "veloflux_control_plane"
	configStoreName = "veloflux_config"
	secretStoreName = "veloflux_secrets"
	snapshotTTL     = 15 * time.Second
	heartbeatTTL    = 30 * time.Second
	maxRequestBody  = 10 << 20
	maxControlBody  = 4 << 20
	defaultControl  = "https://api.veloflux.io"
	defaultNodeName = "fastly-edge-primary"
	defaultCluster  = "clients-pro"
)

type Config struct {
	ControlPlaneURL string
	EnrollmentToken string
	NodeToken       string
	NodeID          string
	Cluster         string
	NodeName        string
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

type runtimeState struct {
	mu          sync.Mutex
	nodeID      atomic.Value
	snapshot    atomic.Value
	snapshotAt  atomic.Int64
	heartbeatAt atomic.Int64
	rrCounter   atomic.Uint64
}

var state runtimeState

func init() {
	state.nodeID.Store("")
	state.snapshot.Store(&Snapshot{
		ByHost:     map[string]DynamicConfig{},
		ByTenant:   map[string]DynamicConfig{},
		HostTenant: map[string]string{},
	})
}

func main() {
	fsthttp.ServeMany(handle, &fsthttp.ServeManyOptions{
		MaxRequests: 250,
		MaxLifetime: 5 * time.Minute,
		NextTimeout: 30 * time.Second,
	})
}

func handle(ctx context.Context, w fsthttp.ResponseWriter, r *fsthttp.Request) {
	switch r.URL.Path {
	case "/healthz":
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(fsthttp.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
		return
	case "/readyz":
		cfg, err := loadConfig()
		if err != nil {
			fsthttp.Error(w, "not-ready", fsthttp.StatusServiceUnavailable)
			return
		}
		if err := ensureState(ctx, cfg, true); err != nil || currentNodeID() == "" || len(currentSnapshot().ByHost) == 0 {
			fsthttp.Error(w, "not-ready", fsthttp.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-VeloFlux-Edge", cfg.NodeName)
		w.Header().Set("X-VeloFlux-Edge-ID", currentNodeID())
		w.WriteHeader(fsthttp.StatusOK)
		_, _ = w.Write([]byte("ready\n"))
		return
	}

	cfg, err := loadConfig()
	if err != nil {
		fsthttp.Error(w, "edge configuration unavailable", fsthttp.StatusServiceUnavailable)
		return
	}
	if err := ensureState(ctx, cfg, false); err != nil {
		fsthttp.Error(w, "edge not ready", fsthttp.StatusServiceUnavailable)
		return
	}
	if err := proxy(ctx, w, r, cfg); err != nil {
		fsthttp.Error(w, "upstream unavailable", fsthttp.StatusBadGateway)
	}
}

func loadConfig() (Config, error) {
	cfg := Config{
		ControlPlaneURL: defaultControl,
		Cluster:         defaultCluster,
		NodeName:        defaultNodeName,
	}
	if store, err := configstore.Open(configStoreName); err == nil {
		if v, err := store.Get("control_plane_url"); err == nil && strings.TrimSpace(v) != "" {
			cfg.ControlPlaneURL = strings.TrimRight(strings.TrimSpace(v), "/")
		}
		if v, err := store.Get("edge_cluster"); err == nil && strings.TrimSpace(v) != "" {
			cfg.Cluster = strings.TrimSpace(v)
		}
		if v, err := store.Get("edge_node_name"); err == nil && strings.TrimSpace(v) != "" {
			cfg.NodeName = strings.TrimSpace(v)
		}
		if v, err := store.Get("edge_node_id"); err == nil {
			cfg.NodeID = strings.TrimSpace(v)
		}
	}
	if raw, err := secretstore.Plaintext(secretStoreName, "edge_node_token"); err == nil {
		cfg.NodeToken = strings.TrimSpace(string(raw))
	}
	if raw, err := secretstore.Plaintext(secretStoreName, "edge_token"); err == nil {
		cfg.EnrollmentToken = strings.TrimSpace(string(raw))
	}
	if cfg.ControlPlaneURL == "" || !strings.HasPrefix(cfg.ControlPlaneURL, "https://") {
		return Config{}, errors.New("invalid control_plane_url")
	}
	if cfg.Cluster == "" {
		return Config{}, errors.New("edge_cluster is required")
	}
	if cfg.NodeName == "" {
		return Config{}, errors.New("edge_node_name is required")
	}
	if cfg.NodeToken == "" {
		return Config{}, errors.New("edge_node_token secret is required")
	}
	if currentNodeID() == "" && cfg.NodeID != "" {
		state.nodeID.Store(cfg.NodeID)
	}
	return cfg, nil
}

func ensureState(ctx context.Context, cfg Config, force bool) error {
	state.mu.Lock()
	defer state.mu.Unlock()

	if currentNodeID() == "" {
		if err := reattach(ctx, cfg); err != nil {
			if !errors.Is(err, errNodeNotFound) {
				return err
			}
			if cfg.EnrollmentToken == "" {
				return errors.New("managed node not found and edge_token secret is missing")
			}
			if err := register(ctx, cfg); err != nil {
				return err
			}
		}
	}

	now := time.Now()
	lastSync := time.Unix(state.snapshotAt.Load(), 0)
	if force || len(currentSnapshot().ByHost) == 0 || now.Sub(lastSync) >= snapshotTTL {
		if err := syncConfig(ctx, cfg); err != nil {
			if len(currentSnapshot().ByHost) == 0 {
				return err
			}
		}
	}

	lastHeartbeat := time.Unix(state.heartbeatAt.Load(), 0)
	if force || now.Sub(lastHeartbeat) >= heartbeatTTL {
		if err := heartbeat(ctx, cfg); err == nil {
			state.heartbeatAt.Store(now.Unix())
		}
	}
	return nil
}

var errNodeNotFound = errors.New("managed node not found")

func reattach(ctx context.Context, cfg Config) error {
	payload, _ := json.Marshal(identityRequest{ClusterName: cfg.Cluster, NodeName: cfg.NodeName})
	resp, err := controlRequest(ctx, cfg, fsthttp.MethodPost, "/api/edge/reattach", cfg.NodeToken, payload, "")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == fsthttp.StatusUnauthorized || resp.StatusCode == fsthttp.StatusNotFound {
		return errNodeNotFound
	}
	if resp.StatusCode != fsthttp.StatusOK {
		return fmt.Errorf("reattach failed: HTTP %d", resp.StatusCode)
	}
	var out identityResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&out); err != nil {
		return err
	}
	if strings.TrimSpace(out.NodeID) == "" {
		return errors.New("reattach returned empty node_id")
	}
	state.nodeID.Store(strings.TrimSpace(out.NodeID))
	return nil
}

func register(ctx context.Context, cfg Config) error {
	sum := sha256.Sum256([]byte(cfg.NodeToken))
	payload, _ := json.Marshal(registerRequest{
		ClusterName:  cfg.Cluster,
		NodeName:     cfg.NodeName,
		APITokenHash: hex.EncodeToString(sum[:]),
		IsBYO:        false,
		IsManaged:    true,
	})
	resp, err := controlRequest(ctx, cfg, fsthttp.MethodPost, "/api/edge/register", cfg.EnrollmentToken, payload, "")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == fsthttp.StatusConflict {
		return reattach(ctx, cfg)
	}
	if resp.StatusCode != fsthttp.StatusCreated && resp.StatusCode != fsthttp.StatusOK {
		return fmt.Errorf("registration failed: HTTP %d", resp.StatusCode)
	}
	var out identityResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&out); err != nil {
		return err
	}
	if strings.TrimSpace(out.NodeID) == "" {
		return errors.New("registration returned empty node_id")
	}
	state.nodeID.Store(strings.TrimSpace(out.NodeID))
	return nil
}

func syncConfig(ctx context.Context, cfg Config) error {
	nodeID := currentNodeID()
	if nodeID == "" {
		return errNodeNotFound
	}
	resp, err := controlRequest(ctx, cfg, fsthttp.MethodGet, "/api/edge/config", cfg.NodeToken, nil, nodeID)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != fsthttp.StatusOK {
		return fmt.Errorf("config sync failed: HTTP %d", resp.StatusCode)
	}
	var snap Snapshot
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxControlBody)).Decode(&snap); err != nil {
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
	for host, hostCfg := range snap.ByHost {
		normalized[normalizeHost(host)] = hostCfg
	}
	snap.ByHost = normalized
	state.snapshot.Store(&snap)
	state.snapshotAt.Store(time.Now().Unix())
	return nil
}

func heartbeat(ctx context.Context, cfg Config) error {
	nodeID := currentNodeID()
	if nodeID == "" {
		return errNodeNotFound
	}
	resp, err := controlRequest(ctx, cfg, fsthttp.MethodPost, "/api/edge/heartbeat", cfg.NodeToken, []byte("{}"), nodeID)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != fsthttp.StatusOK {
		return fmt.Errorf("heartbeat failed: HTTP %d", resp.StatusCode)
	}
	return nil
}

func controlRequest(ctx context.Context, cfg Config, method, path, token string, body []byte, nodeID string) (*fsthttp.Response, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := fsthttp.NewRequest(method, cfg.ControlPlaneURL+path, reader)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-VeloFlux-Managed-Ingress", "true")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if nodeID != "" {
		req.Header.Set("X-Edge-Node-ID", nodeID)
	}
	return req.Send(ctx, controlBackend)
}

func proxy(ctx context.Context, w fsthttp.ResponseWriter, r *fsthttp.Request, cfg Config) error {
	host := normalizeHost(r.Host)
	hostCfg, ok := lookupHostConfig(currentSnapshot().ByHost, host)
	if !ok {
		fsthttp.Error(w, "unknown host", fsthttp.StatusMisdirectedRequest)
		return nil
	}
	targets, strategy := selectTargets(hostCfg, host, r.URL.Path)
	if len(targets) == 0 {
		return errors.New("no upstream configured")
	}

	body, err := readRequestBody(r)
	if err != nil {
		fsthttp.Error(w, err.Error(), fsthttp.StatusRequestEntityTooLarge)
		return nil
	}

	start := int(state.rrCounter.Add(1)-1) % len(targets)
	if strings.EqualFold(strategy, "failover") || len(targets) == 1 {
		start = 0
	}

	var lastErr error
	for attempt := 0; attempt < len(targets); attempt++ {
		idx := (start + attempt) % len(targets)
		target, err := url.Parse(strings.TrimSpace(targets[idx]))
		if err != nil || target.Host == "" || (target.Scheme != "http" && target.Scheme != "https") {
			lastErr = errors.New("invalid upstream")
			continue
		}
		if attempt > 0 && !retryableMethod(r.Method) {
			break
		}
		if err := sendUpstream(ctx, w, r, body, target, hostCfg, cfg); err == nil {
			return nil
		} else {
			lastErr = err
		}
	}
	return lastErr
}

func sendUpstream(ctx context.Context, w fsthttp.ResponseWriter, incoming *fsthttp.Request, body []byte, target *url.URL, hostCfg DynamicConfig, cfg Config) error {
	targetURL := *incoming.URL
	targetURL.Scheme = target.Scheme
	targetURL.Host = target.Host
	targetURL.Path = joinURLPath(target.Path, incoming.URL.Path)

	targetHost := target.Hostname()
	targetPort := target.Port()
	backendTarget := targetHost
	if targetPort != "" {
		backendTarget = net.JoinHostPort(targetHost, targetPort)
	}
	hostOverride := target.Host
	if hostCfg.PreserveHost != nil && *hostCfg.PreserveHost {
		hostOverride = normalizeHost(incoming.Host)
	}

	backendName := backendName(target.Scheme, backendTarget, hostOverride)
	options := fsthttp.NewBackendOptions().
		HostOverride(hostOverride).
		PoolConnections(true).
		ConnectTimeout(5 * time.Second).
		FirstByteTimeout(30 * time.Second).
		BetweenBytesTimeout(30 * time.Second)
	if target.Scheme == "https" {
		options.UseSSL(true).SNIHostname(targetHost).CertHostname(targetHost)
	}
	if _, err := fsthttp.RegisterDynamicBackend(backendName, backendTarget, options); err != nil && !errors.Is(err, fsthttp.ErrBackendNameInUse) {
		return err
	}

	out, err := fsthttp.NewRequest(incoming.Method, targetURL.String(), bytes.NewReader(body))
	if err != nil {
		return err
	}
	out.Header.Reset(incoming.Header.Clone())
	out.Header.Set("X-Forwarded-Host", normalizeHost(incoming.Host))
	out.Header.Set("X-VeloFlux-Edge", cfg.NodeName)
	out.Header.Set("X-VeloFlux-Provider", "fastly")
	if id := currentNodeID(); id != "" {
		out.Header.Set("X-VeloFlux-Edge-ID", id)
	}
	if hostCfg.UpstreamUserAgent != nil && strings.TrimSpace(*hostCfg.UpstreamUserAgent) != "" {
		out.Header.Set("User-Agent", strings.TrimSpace(*hostCfg.UpstreamUserAgent))
	}
	if hostCfg.CustomDomainHeader != nil && strings.TrimSpace(*hostCfg.CustomDomainHeader) != "" {
		out.Header.Set(strings.TrimSpace(*hostCfg.CustomDomainHeader), normalizeHost(incoming.Host))
	}

	resp, err := out.Send(ctx, backendName)
	if err != nil {
		return err
	}
	w.Header().Reset(resp.Header)
	w.Header().Set("X-VeloFlux-Edge", cfg.NodeName)
	w.Header().Set("X-VeloFlux-Provider", "fastly")
	if id := currentNodeID(); id != "" {
		w.Header().Set("X-VeloFlux-Edge-ID", id)
	}
	w.WriteHeader(resp.StatusCode)
	if err := w.Append(resp.Body); err != nil {
		_ = resp.Body.Close()
		return err
	}
	return nil
}

func readRequestBody(r *fsthttp.Request) ([]byte, error) {
	if r.Body == nil {
		return nil, nil
	}
	defer r.Body.Close()
	limited := io.LimitReader(r.Body, maxRequestBody+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		return nil, err
	}
	if len(data) > maxRequestBody {
		return nil, fmt.Errorf("request body exceeds %d bytes", maxRequestBody)
	}
	return data, nil
}

func retryableMethod(method string) bool {
	switch strings.ToUpper(strings.TrimSpace(method)) {
	case fsthttp.MethodGet, fsthttp.MethodHead, fsthttp.MethodOptions:
		return true
	default:
		return false
	}
}

func backendName(scheme, target, hostOverride string) string {
	sum := sha256.Sum256([]byte(scheme + "|" + target + "|" + hostOverride))
	return "vf_" + hex.EncodeToString(sum[:8])
}

func joinURLPath(base, requestPath string) string {
	if base == "" || base == "/" {
		if requestPath == "" {
			return "/"
		}
		return requestPath
	}
	return strings.TrimRight(base, "/") + "/" + strings.TrimLeft(requestPath, "/")
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
		suffix := pattern[1:]
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
	for _, value := range in {
		value = strings.TrimSpace(value)
		if value != "" {
			out = append(out, value)
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

func currentNodeID() string {
	if v := state.nodeID.Load(); v != nil {
		if s, ok := v.(string); ok {
			return strings.TrimSpace(s)
		}
	}
	return ""
}

func currentSnapshot() *Snapshot {
	if v := state.snapshot.Load(); v != nil {
		if snap, ok := v.(*Snapshot); ok && snap != nil {
			return snap
		}
	}
	return &Snapshot{ByHost: map[string]DynamicConfig{}}
}
