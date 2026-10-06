package discovery

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"
)

// K8sConfig configures the Kubernetes discovery backend. Empty APIURL/Token
// fall back to the in-cluster service-account settings.
type K8sConfig struct {
	APIURL        string // e.g. https://10.0.0.1:443
	Token         string // service-account bearer token
	CAData        []byte // optional CA bundle for the API server
	Namespace     string // default "default"
	LabelSelector string // e.g. "app=vllm"
	PortName      string // optional named port (else the first port)
	Model         string // fallback model when the slice has no model label
	Scheme        string // discovered worker URL scheme (default "http")
	Interval      time.Duration
	Client        *http.Client // optional (tests); else built from CA/insecure
}

// K8s discovers workers from the cluster's EndpointSlices over the plain HTTP
// API (no client-go). The model comes from a label — `llm-gateway/model`, then
// the slice's `kubernetes.io/service-name`, then cfg.Model. Polls on an interval.
type K8s struct {
	cfg  K8sConfig
	http *http.Client
}

// NewK8s builds a Kubernetes discovery backend, filling in in-cluster defaults.
func NewK8s(cfg K8sConfig) *K8s {
	if cfg.Scheme == "" {
		cfg.Scheme = "http"
	}
	if cfg.Interval <= 0 {
		cfg.Interval = 3 * time.Second
	}
	if cfg.Namespace == "" {
		cfg.Namespace = "default"
	}
	if cfg.APIURL == "" {
		cfg.APIURL = inClusterAPI()
	}
	if cfg.Token == "" {
		cfg.Token = readFile(serviceAccountPath + "/token")
	}
	if len(cfg.CAData) == 0 {
		cfg.CAData = readFileBytes(serviceAccountPath + "/ca.crt")
	}
	hc := cfg.Client
	if hc == nil {
		hc = &http.Client{Timeout: 10 * time.Second, Transport: k8sTransport(cfg.CAData)}
	}
	return &K8s{cfg: cfg, http: hc}
}

const serviceAccountPath = "/var/run/secrets/kubernetes.io/serviceaccount"

// Start implements Discovery.
func (k *K8s) Start(ctx context.Context) (<-chan []Endpoint, error) {
	ch := make(chan []Endpoint, 1)
	go func() {
		defer close(ch)
		k.emit(ctx, ch)
		t := time.NewTicker(k.cfg.Interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				k.emit(ctx, ch)
			}
		}
	}()
	return ch, nil
}

// Close implements Discovery.
func (*K8s) Close() error { return nil }

type endpointSliceList struct {
	Items []struct {
		Metadata struct {
			Labels map[string]string `json:"labels"`
		} `json:"metadata"`
		Endpoints []struct {
			Addresses  []string `json:"addresses"`
			Conditions struct {
				Ready bool `json:"ready"`
			} `json:"conditions"`
		} `json:"endpoints"`
		Ports []struct {
			Name string `json:"name"`
			Port int    `json:"port"`
		} `json:"ports"`
	} `json:"items"`
}

func (k *K8s) emit(ctx context.Context, ch chan<- []Endpoint) {
	u := fmt.Sprintf("%s/apis/discovery.k8s.io/v1/namespaces/%s/endpointslices", k.cfg.APIURL, k.cfg.Namespace)
	if k.cfg.LabelSelector != "" {
		u += "?labelSelector=" + k.cfg.LabelSelector
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return
	}
	if k.cfg.Token != "" {
		req.Header.Set("Authorization", "Bearer "+k.cfg.Token)
	}
	resp, err := k.http.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return
	}
	var list endpointSliceList
	if json.NewDecoder(resp.Body).Decode(&list) != nil {
		return
	}
	out := make([]Endpoint, 0, 8)
	for _, item := range list.Items {
		model := item.Metadata.Labels["llm-gateway/model"]
		if model == "" {
			model = item.Metadata.Labels["kubernetes.io/service-name"]
		}
		if model == "" {
			model = k.cfg.Model
		}
		port := k.pickPort(item.Ports)
		if port == 0 {
			continue
		}
		for _, ep := range item.Endpoints {
			if !ep.Conditions.Ready {
				continue
			}
			for _, addr := range ep.Addresses {
				out = append(out, Endpoint{
					ID:    fmt.Sprintf("%s:%d", addr, port),
					URL:   fmt.Sprintf("%s://%s:%d", k.cfg.Scheme, addr, port),
					Model: model,
				})
			}
		}
	}
	select {
	case ch <- out:
	case <-ctx.Done():
	}
}

func (k *K8s) pickPort(ports []struct {
	Name string `json:"name"`
	Port int    `json:"port"`
}) int {
	if len(ports) == 0 {
		return 0
	}
	if k.cfg.PortName == "" {
		return ports[0].Port
	}
	for _, p := range ports {
		if p.Name == k.cfg.PortName {
			return p.Port
		}
	}
	return 0
}

func inClusterAPI() string {
	host, port := os.Getenv("KUBERNETES_SERVICE_HOST"), os.Getenv("KUBERNETES_SERVICE_PORT")
	if host == "" || port == "" {
		return ""
	}
	return "https://" + host + ":" + port
}

func k8sTransport(ca []byte) *http.Transport {
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if len(ca) > 0 {
		pool := x509.NewCertPool()
		if pool.AppendCertsFromPEM(ca) {
			tlsCfg.RootCAs = pool
		}
	} else {
		// No CA available (e.g. out of cluster): fall back to system roots.
		tlsCfg.RootCAs = nil
	}
	return &http.Transport{TLSClientConfig: tlsCfg}
}

func readFile(path string) string {
	b, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

func readFileBytes(path string) []byte {
	b, _ := os.ReadFile(path)
	return b
}
