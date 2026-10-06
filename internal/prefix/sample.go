// Package prefix implements KV-cache-aware prefix routing. It holds the inverted
// index that maps a block hash to the workers holding that block, and answers
// "longest cached prefix" queries used to pick a worker.
//
// The inverted shape ("hash -> workers") keeps every value small (bounded by
// the worker count), so it never grows into a big key — unlike a forward layout
// ("worker -> its hashes"), whose value grows with a worker's cache. The index
// is populated by block add/remove events the engine reports, and queried per
// request with the request's block-hash chain.
package prefix

// SampleIndices returns the block indices to sample for a prompt of numBlocks
// full blocks: 0, 1, 2, 4, 8, 16, ... (exponential). This mirrors how an engine
// can expose a compact per-request hash list: ~log2(numBlocks) hashes covering
// every prefix length to within a factor of two, instead of one hash per block.
//
// The values are 0-based block indices; the corresponding prefix length is
// index+1 blocks. Only full blocks count (callers pass len(tokenIDs)/blockSize).
func SampleIndices(numBlocks int) []int {
	var out []int
	for k := 0; ; k++ {
		idx := k
		if k >= 2 {
			idx = 1 << (k - 1)
		}
		if idx >= numBlocks {
			break
		}
		out = append(out, idx)
	}
	return out
}
