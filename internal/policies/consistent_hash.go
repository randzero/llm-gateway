package policies

import (
	"context"
	"hash/crc32"
	"sort"
	"strconv"
)

// ConsistentHash routes requests with the same session/user/tenant identity to
// the same worker via a hash ring with virtual nodes. Requests without an
// identity fall back to the first eligible candidate.
type ConsistentHash struct{ replicas int }

// NewConsistentHash returns a consistent-hash policy.
func NewConsistentHash() *ConsistentHash { return &ConsistentHash{replicas: 100} }

// Name implements Policy.
func (*ConsistentHash) Name() string { return "consistent_hash" }

// Select implements Policy.
func (p *ConsistentHash) Select(_ context.Context, req *RequestMeta, cs []Candidate) (int, error) {
	h := HealthyIndices(cs)
	if len(h) == 0 {
		return -1, ErrNoCandidate
	}
	key := hashKey(req)
	if key == "" {
		return h[0], nil
	}

	type vnode struct {
		hash uint32
		idx  int
	}
	ring := make([]vnode, 0, len(h)*p.replicas)
	for _, idx := range h {
		id := cs[idx].ID
		for r := 0; r < p.replicas; r++ {
			ring = append(ring, vnode{crc32.ChecksumIEEE([]byte(id + "#" + strconv.Itoa(r))), idx})
		}
	}
	sort.Slice(ring, func(i, j int) bool { return ring[i].hash < ring[j].hash })

	kh := crc32.ChecksumIEEE([]byte(key))
	i := sort.Search(len(ring), func(i int) bool { return ring[i].hash >= kh })
	if i == len(ring) {
		i = 0
	}
	return ring[i].idx, nil
}

// hashKey extracts the routing identity, preferring session > user > tenant.
func hashKey(req *RequestMeta) string {
	switch {
	case req.SessionID != "":
		return "s:" + req.SessionID
	case req.UserID != "":
		return "u:" + req.UserID
	case req.TenantID != "":
		return "t:" + req.TenantID
	default:
		return ""
	}
}
