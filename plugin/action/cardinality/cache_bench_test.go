package cardinality

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func benchSetup(b *testing.B, suffixCount int) (*Cache, string) {
	seed := uint64(b.N)
	rng := rand.New(rand.NewSource(int64(seed)))

	b.Helper()
	cache := NewCache(time.Hour)
	prefix := makeRandStr(rng, 16)

	for i := 0; i < suffixCount; i++ {
		key := fmt.Sprintf("%s-%s", prefix, makeRandStr(rng, 16))
		cache.Set(prefix, key)
	}

	return cache, prefix
}

func makeRandStr(rng *rand.Rand, n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rng.Intn(len(letters))]
	}
	return string(b)
}

func BenchmarkCountPrefix_100(b *testing.B) {
	cache, prefix := benchSetup(b, 100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.CountPrefix(prefix)
	}
}

func BenchmarkCountPrefix_1k(b *testing.B) {
	cache, prefix := benchSetup(b, 1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.CountPrefix(prefix)
	}
}

func BenchmarkCountPrefix_10k(b *testing.B) {
	cache, prefix := benchSetup(b, 10000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.CountPrefix(prefix)
	}
}

func BenchmarkCountPrefix_100k(b *testing.B) {
	cache, prefix := benchSetup(b, 100000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.CountPrefix(prefix)
	}
}

func BenchmarkSet_100(b *testing.B) {
	cache, prefix := benchSetup(b, 100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Set(prefix, "new-"+fmt.Sprint(i))
	}
}

func BenchmarkSet_1k(b *testing.B) {
	cache, prefix := benchSetup(b, 1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Set(prefix, "new-"+fmt.Sprint(i))
	}
}

func BenchmarkSet_10k(b *testing.B) {
	cache, prefix := benchSetup(b, 10000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Set(prefix, "new-"+fmt.Sprint(i))
	}
}
