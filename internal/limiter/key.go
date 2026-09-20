package limiter

import "strings"

// shortAlgorithm keeps keys compact without making them ambiguous.
var shortAlgorithm = map[string]string{
	AlgorithmSlidingWindow: "sw",
	AlgorithmTokenBucket:   "tb",
}

// Key builds the Redis key holding one client's state for one resource.
//
// The client ID is wrapped in a hash tag — rl:sw:{acme}:/v1/search — so that
// under Redis Cluster every key belonging to a client lands in the same slot.
// Nothing here needs cross-key transactions today, but a MULTI spanning slots
// is rejected outright, and discovering that after sharding is an unpleasant
// afternoon.
//
// The algorithm is part of the key so that switching a rule from sliding
// window to token bucket cannot read the other algorithm's incompatible state.
func Key(prefix, algorithm, client, resource string) string {
	short, ok := shortAlgorithm[algorithm]
	if !ok {
		short = algorithm
	}
	var b strings.Builder
	b.Grow(len(prefix) + len(short) + len(client) + len(resource) + 6)
	b.WriteString(prefix)
	b.WriteByte(':')
	b.WriteString(short)
	b.WriteString(":{")
	b.WriteString(client)
	b.WriteString("}:")
	b.WriteString(resource)
	return b.String()
}
