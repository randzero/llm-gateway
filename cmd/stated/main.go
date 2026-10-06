// Command stated is the shared state service: it fronts Redis for the state
// plane (worker/session scalars) and the prefix index (block hash -> workers).
// Routers and engine plugins talk to it instead of Redis directly (design §13).
package main

import (
	"flag"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/randzero/llm-gateway/internal/prefix"
	"github.com/randzero/llm-gateway/internal/stateserver"
)

func main() {
	listen := flag.String("listen", ":8081", "listen address")
	redisAddrs := flag.String("redis-addrs", os.Getenv("LLMGATEWAY_REDIS_ADDRS"), "comma-separated Redis addresses")
	blockSize := flag.Int("block-size", 16, "engine KV block size")
	keyPrefix := flag.String("key-prefix", "lg:", "Redis key namespace")
	stateTTL := flag.Duration("state-ttl", 30*time.Second, "TTL for worker/session state")
	redisTTL := flag.Duration("redis-ttl", 10*time.Minute, "TTL for prefix-index entries")
	cacheSize := flag.Int("prefix-cache-size", 8192, "in-service hot-prefix cache entries")
	cacheTTL := flag.Duration("prefix-cache-ttl", 5*time.Second, "in-service hot-prefix cache TTL")
	flag.Parse()

	addrs := splitCSV(*redisAddrs)
	if len(addrs) == 0 {
		log.Fatal("stated: need --redis-addrs host:port[,host:port...]")
	}
	rdb := redis.NewUniversalClient(&redis.UniversalOptions{Addrs: addrs})
	// Hot-prefix cache in front of Redis: one copy shared by all routers.
	idx := prefix.NewCachingIndex(prefix.NewRedisIndex(rdb, *redisTTL), *cacheSize, *cacheTTL)
	srv := stateserver.New(rdb, idx, *keyPrefix, *blockSize, *stateTTL)

	httpSrv := &http.Server{
		Addr:              *listen,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	log.Printf("stated: listen=%s redis=%v block-size=%d", *listen, addrs, *blockSize)
	log.Fatal(httpSrv.ListenAndServe())
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
