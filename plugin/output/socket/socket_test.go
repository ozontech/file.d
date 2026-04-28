package socket

import (
	"bufio"
	"bytes"
	"net"
	"testing"
	"time"

	"github.com/ozontech/file.d/metric"
	"github.com/ozontech/file.d/pipeline"
	insaneJSON "github.com/ozontech/insane-json"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func startTCPServer(t *testing.T, delimiter byte) (addr string, received chan []string) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	received = make(chan []string, 1)

	go func() {
		defer ln.Close()
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		var msgs []string
		reader := bufio.NewReader(conn)
		for {
			raw, err := reader.ReadBytes(delimiter)
			msg := bytes.TrimRight(raw, string(delimiter))
			if len(msg) > 0 {
				msgs = append(msgs, string(msg))
			}
			if err != nil {
				break
			}
		}
		received <- msgs
	}()

	t.Cleanup(func() { _ = ln.Close() })
	return ln.Addr().String(), received
}

func newBatch(t *testing.T, jsons ...string) *pipeline.Batch {
	t.Helper()

	events := make([]*pipeline.Event, 0, len(jsons))
	for _, j := range jsons {
		root, err := insaneJSON.DecodeBytes([]byte(j))
		require.NoError(t, err)
		events = append(events, &pipeline.Event{Root: root})
	}
	return pipeline.NewPreparedBatch(events)
}

func newPlugin(t *testing.T, network, addr string, delimiter byte) *Plugin {
	t.Helper()

	return &Plugin{
		logger: zap.NewExample(),
		config: &Config{
			Network:    network,
			Address:    addr,
			Delimiter:  string(delimiter),
			Delimiter_: delimiter,
		},
		avgEventSize: 1024,
	}
}

func TestOut_Reconnect(t *testing.T) {
	t.Parallel()

	addr, received := startTCPServer(t, '\n')
	plugin := newPlugin(t, "tcp", addr, '\n')

	workerData := pipeline.WorkerData(nil)

	err := plugin.out(&workerData, newBatch(t, `{"id":1}`))
	require.NoError(t, err)

	workerData.(*data).conn.Close()
	workerData.(*data).conn = nil

	addr2, received2 := startTCPServer(t, '\n')
	plugin.config.Address = addr2

	err = plugin.out(&workerData, newBatch(t, `{"id":2}`))
	require.NoError(t, err)

	workerData.(*data).conn.Close()

	msgs1 := <-received
	msgs2 := <-received2
	assert.Equal(t, []string{`{"id":1}`}, msgs1)
	assert.Equal(t, []string{`{"id":2}`}, msgs2)
}

func TestOut_DialError(t *testing.T) {
	t.Parallel()

	plugin := newPlugin(t, "tcp", ":9999", '\n')
	plugin.registerMetrics(metric.NewCtl("test", prometheus.NewRegistry(), time.Minute, 0))

	workerData := pipeline.WorkerData(nil)
	err := plugin.out(&workerData, newBatch(t, `{"msg":"test"}`))

	require.Error(t, err)
	assert.Nil(t, workerData.(*data).conn)
}

func TestWriteAll(t *testing.T) {
	t.Parallel()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	received := make(chan []byte, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		buf := new(bytes.Buffer)
		_, _ = buf.ReadFrom(conn)
		received <- buf.Bytes()
	}()

	conn, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)

	data := []byte(`{"msg":"test"}` + "\n")
	err = writeAll(conn, data)
	require.NoError(t, err)
	conn.Close()

	got := <-received
	assert.Equal(t, data, got)
}
