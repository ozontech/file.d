package socket

import (
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/plugin/output/devnull"
	"github.com/ozontech/file.d/test"
	"github.com/stretchr/testify/require"
)

func doTest(t *testing.T, config *Config, clients int, parallel bool) {
	p := test.NewPipeline(nil, "passive")

	test.NewConfig(config, nil)
	p.SetInput(&pipeline.InputPluginInfo{
		PluginStaticInfo: &pipeline.PluginStaticInfo{
			Config: config,
		},
		PluginRuntimeInfo: &pipeline.PluginRuntimeInfo{
			Plugin: &Plugin{},
		},
	})

	outPlugin, outCfg := devnull.Factory()
	output := outPlugin.(*devnull.Plugin)

	const eventsCount = 3

	wgOut := &sync.WaitGroup{}
	wgOut.Add(eventsCount * clients)
	outEvents := make(map[pipeline.SourceID][]string)
	output.SetOutFn(func(event *pipeline.Event) {
		outEvents[event.SourceID] = append(outEvents[event.SourceID], event.Root.EncodeToString())
		wgOut.Done()
	})

	p.SetOutput(&pipeline.OutputPluginInfo{
		PluginStaticInfo: &pipeline.PluginStaticInfo{
			Config: outCfg,
		},
		PluginRuntimeInfo: &pipeline.PluginRuntimeInfo{
			Plugin: output,
		},
	})

	p.Start()

	connWrite := func(conn net.Conn, wg *sync.WaitGroup) {
		_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))

		_, werr := conn.Write([]byte("{\"a\":1}\n"))
		require.NoError(t, werr)

		_, werr = conn.Write([]byte("{\"b\":2}\n{\"c\":3}"))
		require.NoError(t, werr)

		if wg != nil {
			wg.Done()
		}
	}

	if parallel {
		conns := make([]net.Conn, 0, clients)
		for i := 0; i < clients; i++ {
			conn, err := net.Dial(config.Network, config.Address)
			require.NoError(t, err)

			conns = append(conns, conn)
		}

		wgIn := &sync.WaitGroup{}
		wgIn.Add(clients)
		for _, conn := range conns {
			go connWrite(conn, wgIn)
		}
		wgIn.Wait()

		for _, conn := range conns {
			conn.Close()
		}
	} else {
		for i := 0; i < clients; i++ {
			conn, err := net.Dial(config.Network, config.Address)
			require.NoError(t, err)

			connWrite(conn, nil)

			conn.Close()
		}
	}

	wgWaitWithTimeout := func(wg *sync.WaitGroup, timeout time.Duration) bool {
		c := make(chan struct{})
		go func() {
			defer close(c)
			wg.Wait()
		}()
		select {
		case <-c:
			return false
		case <-time.After(timeout):
			return true
		}
	}

	timeouted := wgWaitWithTimeout(wgOut, 10*time.Second)
	p.Stop()

	require.False(t, timeouted, "timeouted")

	require.Equal(t, clients, len(outEvents), "wrong clients count")
	for _, events := range outEvents {
		require.Equal(t, eventsCount, len(events), "wrong events count")

		require.Equal(t, `{"a":1}`, events[0], "wrong event")
		require.Equal(t, `{"b":2}`, events[1], "wrong event")
		require.Equal(t, `{"c":3}`, events[2], "wrong event")
	}
}

func TestSocketTCP(t *testing.T) {
	t.Parallel()

	const clients = 4
	cfg := &Config{
		Network: networkTcp,
		Address: ":5001",
	}

	doTest(t, cfg, clients, true)
	doTest(t, cfg, clients, false)
}

func TestSocketUDP(t *testing.T) {
	t.Parallel()

	const clients = 4
	cfg := &Config{
		Network: networkUdp,
		Address: ":5002",
	}

	doTest(t, cfg, clients, true)
	doTest(t, cfg, clients, false)
}

func TestSocketUnix(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()

	doTest(t, &Config{
		Network: networkUnix,
		Address: filepath.Join(tmpDir, "filed.sock"),
	}, 1, false)
}
