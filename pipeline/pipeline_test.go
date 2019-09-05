package pipeline
//
//import (
//	"testing"
//
//	"github.com/stretchr/testify/assert"
//)
//
//func getPipeline() *Pipeline {
//	p := New("test_pipeline", 1, 1)
//
//	p.SetInputPlugin(&InputPluginDescription{Plugin: nil, Config: nil})
//	p.SetOutputPlugin(&OutputPluginDescription{Plugin: nil, Config: nil})
//
//	return p
//}
//
//func TestPutToCommitQueue(t *testing.T) {
//	pipeline := getPipeline()
//
//	pipeline.putToCommitQueue(&Event{seq: 5,})
//	pipeline.putToCommitQueue(&Event{seq: 4,})
//	pipeline.putToCommitQueue(&Event{seq: 3,})
//	pipeline.putToCommitQueue(&Event{seq: 2,})
//	pipeline.putToCommitQueue(&Event{seq: 1,})
//
//	assert.Equal(t, 5, len(pipeline.commitQueue), "wrong commit queue length")
//	assert.Equal(t, 1, int(pipeline.commitQueue[4].seq), "wrong commit queue event")
//	assert.Equal(t, 2, int(pipeline.commitQueue[3].seq), "wrong commit queue event")
//	assert.Equal(t, 3, int(pipeline.commitQueue[2].seq), "wrong commit queue event")
//	assert.Equal(t, 4, int(pipeline.commitQueue[1].seq), "wrong commit queue event")
//	assert.Equal(t, 5, int(pipeline.commitQueue[0].seq), "wrong commit queue event")
//
//	pipeline = getPipeline()
//
//	pipeline.putToCommitQueue(&Event{seq: 3,})
//	pipeline.putToCommitQueue(&Event{seq: 4,})
//	pipeline.putToCommitQueue(&Event{seq: 5,})
//	pipeline.putToCommitQueue(&Event{seq: 2,})
//	pipeline.putToCommitQueue(&Event{seq: 1,})
//
//	assert.Equal(t, 5, len(pipeline.commitQueue), "wrong commit queue length")
//	assert.Equal(t, 1, int(pipeline.commitQueue[4].seq), "wrong commit queue event")
//	assert.Equal(t, 2, int(pipeline.commitQueue[3].seq), "wrong commit queue event")
//	assert.Equal(t, 3, int(pipeline.commitQueue[2].seq), "wrong commit queue event")
//	assert.Equal(t, 4, int(pipeline.commitQueue[1].seq), "wrong commit queue event")
//	assert.Equal(t, 5, int(pipeline.commitQueue[0].seq), "wrong commit queue event")
//
//	pipeline = getPipeline()
//
//	pipeline.putToCommitQueue(&Event{seq: 4,})
//	pipeline.putToCommitQueue(&Event{seq: 3,})
//	pipeline.putToCommitQueue(&Event{seq: 2,})
//	pipeline.putToCommitQueue(&Event{seq: 1,})
//	pipeline.putToCommitQueue(&Event{seq: 5,})
//
//	assert.Equal(t, 5, len(pipeline.commitQueue), "wrong commit queue length")
//	assert.Equal(t, 1, int(pipeline.commitQueue[4].seq), "wrong commit queue event")
//	assert.Equal(t, 2, int(pipeline.commitQueue[3].seq), "wrong commit queue event")
//	assert.Equal(t, 3, int(pipeline.commitQueue[2].seq), "wrong commit queue event")
//	assert.Equal(t, 4, int(pipeline.commitQueue[1].seq), "wrong commit queue event")
//	assert.Equal(t, 5, int(pipeline.commitQueue[0].seq), "wrong commit queue event")
//}
