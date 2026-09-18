package runtime

import (
	"testing"

	"github.com/ozontech/file.d/plugin/action/transform/core"
	insaneJSON "github.com/ozontech/insane-json"
)

const benchEvent = `{"log":"2025-05-25 11:11:11 INFO billing request processed in 42ms","k8s_node":"node-17","k8s_pod_label_app":"billing","@lt":""}`

// The move-shaped assignments transform #1 performs on every event.
func benchMoves(b *testing.B, root *insaneJSON.Root) {
	seg := func(f string) core.Path {
		return core.Path{Segments: []core.Segment{core.FieldSeg(f)}}
	}
	for i := 0; i < b.N; i++ {
		// one iteration == one event: the pipeline decodes into the pooled root,
		// which resets the decoder's node counter, then the actions run
		if err := root.DecodeString(benchEvent); err != nil {
			b.Fatal(err)
		}
		tgt := NewRootTarget(root, "src", map[string]string{})
		src, _ := tgt.Get(seg("log"))
		_ = tgt.Set(seg("message"), src)
		src, _ = tgt.Get(seg("k8s_pod_label_app"))
		_ = tgt.Set(seg("service"), src)
		src, _ = tgt.Get(seg("k8s_node"))
		_ = tgt.Set(seg("host"), src)
		_ = tgt.Set(seg("@lt"), core.StringValue{V: "ok"})
		_ = tgt.Set(seg("level"), core.IntegerValue{V: 6})
	}
}

func BenchmarkSetMoves(b *testing.B) {
	root, err := insaneJSON.DecodeString(benchEvent)
	if err != nil {
		b.Fatal(err)
	}
	defer insaneJSON.Release(root)
	b.ReportAllocs()
	b.ResetTimer()
	benchMoves(b, root)
}
