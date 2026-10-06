// Command gateway is the llm-gateway router: an OpenAI/Anthropic-compatible
// load balancer in front of vLLM (or any OpenAI-compatible) workers.
package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/randzero/llm-gateway/internal/api"
	"github.com/randzero/llm-gateway/internal/control"
	"github.com/randzero/llm-gateway/internal/discovery"
	"github.com/randzero/llm-gateway/internal/observability"
	"github.com/randzero/llm-gateway/internal/otel"
	"github.com/randzero/llm-gateway/internal/policies"
	"github.com/randzero/llm-gateway/internal/prefix"
	"github.com/randzero/llm-gateway/internal/proxy"
	"github.com/randzero/llm-gateway/internal/state"
	"github.com/randzero/llm-gateway/internal/stateclient"
)

func main() {
	listen := flag.String("listen", ":8000", "listen address")
	policyName := flag.String("policy", "power_of_two", "load balancing policy")

	discoveryKind := flag.String("discovery", "static", "worker discovery: static | consul | dns | k8s")
	workersFlag := flag.String("workers", "", "static workers: url[,url...] or model=url[,...]")
	consulAddr := flag.String("consul-addr", os.Getenv("LLMGATEWAY_CONSUL_ADDR"), "Consul HTTP address, e.g. http://127.0.0.1:8500")
	consulServices := flag.String("consul-services", os.Getenv("LLMGATEWAY_CONSUL_SERVICES"), "comma-separated Consul service names (== models)")
	consulScheme := flag.String("consul-scheme", "http", "scheme for discovered worker URLs")
	dnsServices := flag.String("dns-services", "", "comma-separated model names to resolve via SRV (_<model>._tcp.<domain>)")
	dnsDomain := flag.String("dns-domain", "", "base domain for DNS/SRV discovery, e.g. svc.cluster.local")
	dnsScheme := flag.String("dns-scheme", "http", "scheme for DNS-discovered worker URLs")
	k8sNamespace := flag.String("k8s-namespace", "", "Kubernetes namespace (default: current)")
	k8sSelector := flag.String("k8s-selector", "", "EndpointSlice label selector, e.g. app=vllm")
	k8sPort := flag.String("k8s-port-name", "", "named port to use (default: the slice's first port)")
	k8sModel := flag.String("k8s-model", "", "fallback model when the slice has no model label")
	k8sScheme := flag.String("k8s-scheme", "http", "scheme for k8s-discovered worker URLs")

	healthInterval := flag.Duration("health-interval", 5*time.Second, "health probe interval")
	stateStore := flag.String("state-store", "inproc", "state store backend: inproc | consul")
	stateTTL := flag.Duration("state-ttl", 30*time.Second, "TTL for plugin-reported state")

	// Cluster mode: talk to the shared state service (which fronts Redis) for
	// both the state plane and the prefix index. See cmd/stated.
	stateServer := flag.String("state-server", os.Getenv("LLMGATEWAY_STATE_SERVER"), "shared state service URL, e.g. http://stated:8081")

	prefixIndex := flag.String("prefix-index", "none", "local prefix index backend: none | inproc (cluster uses --state-server)")
	hashEndpoint := flag.String("hash-endpoint", os.Getenv("LLMGATEWAY_HASH_ENDPOINT"), "engine block-hash endpoint (POST body -> {\"hashes\":[...]})")
	hashHeader := flag.String("hash-header", "X-KV-Hashes", "request header carrying a pre-computed hash chain")
	blockSize := flag.Int("block-size", 16, "engine KV block size (must match the engine)")
	otelEndpoint := flag.String("otel-endpoint", os.Getenv("LLMGATEWAY_OTEL_ENDPOINT"), "OTLP/HTTP traces endpoint, e.g. http://localhost:4318/v1/traces (empty = off)")

	requestTimeout := flag.Duration("request-timeout", 0, "per-request timeout (0 = none)")
	retries := flag.Int("retries", 1, "retry attempts on another worker")
	cbThreshold := flag.Int("cb-threshold", 5, "circuit breaker failure threshold")
	cbCooldown := flag.Duration("cb-cooldown", 10*time.Second, "circuit breaker cooldown")
	flag.Parse()

	var disc discovery.Discovery
	switch *discoveryKind {
	case "static":
		raw := firstNonEmpty(*workersFlag, os.Getenv("LLMGATEWAY_WORKERS"))
		if raw == "" {
			log.Fatal("static discovery: pass --workers url[,url...] or set LLMGATEWAY_WORKERS")
		}
		disc = discovery.NewStatic(parseStatic(raw))
	case "consul":
		services := splitCSV(*consulServices)
		if *consulAddr == "" || len(services) == 0 {
			log.Fatal("consul discovery: need --consul-addr and --consul-services")
		}
		disc = discovery.NewConsul(*consulAddr, services, *consulScheme, 3*time.Second)
	case "dns":
		services := splitCSV(*dnsServices)
		if *dnsDomain == "" || len(services) == 0 {
			log.Fatal("dns discovery: need --dns-domain and --dns-services")
		}
		disc = discovery.NewDNS(services, *dnsDomain, *dnsScheme, 3*time.Second)
	case "k8s":
		disc = discovery.NewK8s(discovery.K8sConfig{
			Namespace:     *k8sNamespace,
			LabelSelector: *k8sSelector,
			PortName:      *k8sPort,
			Model:         *k8sModel,
			Scheme:        *k8sScheme,
			Interval:      3 * time.Second,
		})
	default:
		log.Fatalf("unknown discovery %q (want static|consul|dns|k8s)", *discoveryKind)
	}

	policyReg := policies.NewRegistry()
	policyReg.Register(policies.NewRoundRobin())
	policyReg.Register(policies.NewRandom(time.Now().UnixNano()))
	policyReg.Register(policies.NewPowerOfTwo(time.Now().UnixNano()))
	policyReg.Register(policies.NewConsistentHash())
	policyReg.Register(policies.NewCacheAware(0))
	policyReg.Register(policies.NewLeastLatency(time.Now().UnixNano()))
	policy, ok := policyReg.Get(*policyName)
	if !ok {
		log.Fatalf("unknown policy %q; available: %v", *policyName, policyReg.Names())
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	registry := control.NewRegistry(nil)
	epCh, err := disc.Start(ctx)
	if err != nil {
		log.Fatalf("discovery start: %v", err)
	}
	go func() {
		for eps := range epCh {
			ws := make([]*control.Worker, 0, len(eps))
			for _, e := range eps {
				ws = append(ws, control.NewWorker(e.URL, e.Model, e.DPRank, e.PDRole, *cbThreshold, *cbCooldown))
			}
			registry.Sync(ws)
		}
	}()

	metrics := observability.New()
	client := &http.Client{Timeout: *requestTimeout}
	gateway := proxy.New(registry, policy, client, metrics, *retries)

	// Hashers for the request's block-hash chain (used on the prefix path).
	hashers := []prefix.Hasher{prefix.HeaderHasher{Header: *hashHeader}}
	if *hashEndpoint != "" {
		hashers = append(hashers, &prefix.HTTPHasher{URL: *hashEndpoint, Client: client})
	}

	var store state.StateStore
	var index prefix.Index

	if *stateServer != "" {
		// Cluster mode: the state service fronts Redis and serves both the state
		// plane and the prefix index. The router keeps no local state — one call
		// per request to /match (prefix + session).
		sc := stateclient.New(*stateServer, prefix.CompositeHasher{Hashers: hashers}, client)
		gateway = gateway.WithPrefix(sc)
		log.Printf("state-server=%s block-size=%d", *stateServer, *blockSize)
	} else {
		// Single-instance mode: in-proc (or consul) store + router-side approximation.
		switch *stateStore {
		case "inproc":
			store = state.NewInProcStore()
		case "consul":
			if *consulAddr == "" {
				log.Fatal("consul state store: need --consul-addr")
			}
			store = state.NewConsulStore(*consulAddr, "llm-gateway/")
		default:
			log.Fatalf("unknown state store %q (want inproc|consul)", *stateStore)
		}
		pluginProvider := state.NewPluginProvider(store)
		if err := pluginProvider.Start(ctx); err != nil {
			log.Fatalf("state provider start: %v", err)
		}
		approx := state.NewApproximateProvider(0)
		provider := state.NewCompositeProvider(pluginProvider, approx)
		gateway = gateway.WithProvider(provider).WithObserver(provider)

		switch *prefixIndex {
		case "none":
		case "inproc":
			index = prefix.NewInProcIndex()
		default:
			log.Fatalf("unknown prefix-index %q (want none|inproc; cluster uses --state-server)", *prefixIndex)
		}
		if index != nil {
			gateway = gateway.WithPrefix(prefix.NewPrefixMatcher(prefix.CompositeHasher{Hashers: hashers}, index, *blockSize, false))
		}
	}

	if *otelEndpoint != "" {
		gateway = gateway.WithTracer(otel.New(*otelEndpoint, "llm-gateway", client))
		log.Printf("otel-endpoint=%s", *otelEndpoint)
	}

	go registry.HealthLoop(ctx, *healthInterval, &http.Client{Timeout: 5 * time.Second})

	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics)
	if *stateServer == "" {
		mux.HandleFunc("/state", api.NewStateIngest(store, *stateTTL))
		if index != nil {
			mux.HandleFunc("/kv", api.NewKVIngest(index))
		}
	}
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	mux.Handle("/", gateway)

	srv := &http.Server{
		Addr:              *listen,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	log.Printf("llm-gateway: listen=%s discovery=%s policy=%s", *listen, *discoveryKind, *policyName)
	log.Fatal(srv.ListenAndServe())
}

// parseStatic parses "url" or "model=url" entries.
func parseStatic(raw string) []discovery.Endpoint {
	var out []discovery.Endpoint
	for _, entry := range splitCSV(raw) {
		model := ""
		if i := strings.Index(entry, "="); i >= 0 {
			model = strings.TrimSpace(entry[:i])
			entry = strings.TrimSpace(entry[i+1:])
		}
		if entry == "" {
			continue
		}
		out = append(out, discovery.Endpoint{URL: entry, Model: model})
	}
	return out
}

func splitCSV(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}
