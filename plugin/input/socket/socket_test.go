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

func doTest(t *testing.T, config *Config) {
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

	wgOut := &sync.WaitGroup{}
	wgOut.Add(3)
	outEvents := make([]string, 0)
	output.SetOutFn(func(event *pipeline.Event) {
		outEvents = append(outEvents, event.Root.EncodeToString())
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

	conn, err := net.Dial(config.Network, config.Address)
	require.NoError(t, err)

	wgIn := &sync.WaitGroup{}
	wgIn.Add(1)
	go func() {
		_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))

		_, werr := conn.Write([]byte("{\"a\":1}\n"))
		require.NoError(t, werr)

		_, werr = conn.Write([]byte("{\"b\":2}\n{\"c\":3}"))
		require.NoError(t, werr)

		wgIn.Done()
	}()

	wgIn.Wait()
	conn.Close()

	wgOut.Wait()
	p.Stop()

	require.Equal(t, 3, len(outEvents), "wrong events count")
	require.Equal(t, `{"a":1}`, outEvents[0], "wrong event")
	require.Equal(t, `{"b":2}`, outEvents[1], "wrong event")
	require.Equal(t, `{"c":3}`, outEvents[2], "wrong event")
}

func TestSocketTCP(t *testing.T) {
	t.Parallel()

	doTest(t, &Config{
		Network: networkTcp,
		Address: ":5001",
	})
}

func TestSocketUDP(t *testing.T) {
	t.Parallel()

	doTest(t, &Config{
		Network: networkUdp,
		Address: ":5002",
	})
}

func TestSocketUnix(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()

	doTest(t, &Config{
		Network: networkUnix,
		Address: filepath.Join(tmpDir, "filed.sock"),
	})
}
