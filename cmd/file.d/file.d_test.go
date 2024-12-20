//go:build e2e

package main

import (
	"fmt"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/pipeline"
	_ "github.com/ozontech/file.d/plugin/action/discard"
	_ "github.com/ozontech/file.d/plugin/action/json_decode"
	_ "github.com/ozontech/file.d/plugin/action/keep_fields"
	_ "github.com/ozontech/file.d/plugin/action/rename"
	_ "github.com/ozontech/file.d/plugin/action/throttle"
	_ "github.com/ozontech/file.d/plugin/input/fake"
	"github.com/ozontech/file.d/plugin/input/k8s/meta"
	_ "github.com/ozontech/file.d/plugin/output/devnull"
	_ "github.com/ozontech/file.d/plugin/output/kafka"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	jsons = []string{
		`{"log":"one\n","stream":"%s","service":"1"}`,
		`{"log":"one two\n","stream":"%s","service":"1"}`,
		`{"log":"log one two three\n","stream":"%s","service":"1"}`,
		`{"log":"dropped log one\n", "should_drop":"ok","stream":"%s","service":"1"}`,
		`{"log":"dropped log one two\n", "should_drop":"ok","stream":"%s","service":"1"}`,
		`{"log":"dropped log one three\n", "should_drop":"ok","stream":"%s","service":"1"}`,
		`{"log":"throttled log one\n", "throttle":"1","stream":"%s","service":"1"}`,
		`{"log":"throttled log one two\n", "throttle":"2","stream":"%s","service":"1"}`,
		`{"log":"throttled log one three\n", "throttle":"3","stream":"%s","service":"1"}`,
	}
	multilineJSON = `{"log":"log","stream":"%s","service":"1"}`

	panicContent = `panic: assignment to entry in nil map

goroutine 1 [running]:
example.com/tariffication/tarifficatorGoApi/services/cache.(*Cache).getGeoRules(0xc420438780, 0xef36b8, 0xc42bb7e600, 0xc42bb77ce0, 0x0, 0x0)
	/builds/tariffication/tarifficatorGoApi/services/cache/index.go:69 +0x538
example.com/tariffication/tarifficatorGoApi/services/cache.(*Cache).createAddressIndex(0xc420438780, 0x0, 0x0)
	/builds/tariffication/tarifficatorGoApi/services/cache/index.go:166 +0x5e
example.com/tariffication/tarifficatorGoApi/services/cache.(*Cache).createIndexes(0xc420438780, 0x0, 0xc44ec607d0)
	/builds/tariffication/tarifficatorGoApi/services/cache/index.go:211 +0x8a
example.com/tariffication/tarifficatorGoApi/services/cache.(*Cache).updateDbCache(0xc420438780, 0xc420438780, 0xc4200845c0)
	/builds/tariffication/tarifficatorGoApi/services/cache/cache.go:84 +0x182
example.com/tariffication/tarifficatorGoApi/services/cache.NewCache(0xc4200985f0, 0xc4203fdad0, 0xc420084440, 0x0, 0x0, 0x0)
	/builds/tariffication/tarifficatorGoApi/services/cache/cache.go:66 +0xa7
main.initialize(0xec3270, 0x1d, 0xee7133, 0x9e, 0xec53d7, 0x1f, 0xc40000000a, 0x14, 0xc420086e00)
	/builds/tariffication/tarifficatorGoApi/cmd/tarifficator/main.go:41 +0x389
main.main()
	/builds/tariffication/tarifficatorGoApi/cmd/tarifficator/main.go:65 +0x2ae
`
)

const testTime = 10 * time.Minute

// TestEndToEnd creates near-realistic workload and setups a complex pipeline.
// It's something like fuzz testing. file.d shouldn't crash/panic or hang for infinite time.
// E.g. keep this test running while you are sleeping :)
func TestEndToEnd(t *testing.T) {
	configFilename := "./../testdata/config/e2e.yaml"
	iterationInterval := time.Second * 10
	writerCount := 8
	fileCount := 8

	// we are very deterministic :)
	rand.Seed(0)

	// disable k8s environment
	meta.DisableMetaUpdates = true
	meta.MetaWaitTimeout = time.Millisecond
	meta.MaintenanceInterval = time.Millisecond * 100

	filesDir := t.TempDir()
	offsetsDir := t.TempDir()

	config := cfg.NewConfigFromFile([]string{configFilename})
	input := config.Pipelines["test"].Raw.Get("input")
	input.Set("watching_dir", filesDir)
	input.Set("offsets_file", filepath.Join(offsetsDir, "offsets.yaml"))

	fileD := fd.New(config, ":9000")
	fileD.Start()

	tm := time.Now()
	for {
		for i := 0; i < writerCount; i++ {
			go runWriter(filesDir, fileCount)
		}

		time.Sleep(iterationInterval)
		if time.Since(tm) > testTime {
			break
		}
	}
}

func runWriter(tempDir string, files int) {
	format := `{"log":"%s\n","stream":"stderr"}`
	panicLines := make([]string, 0)
	for _, line := range strings.Split(panicContent, "\n") {
		if line == "" {
			continue
		}
		panicLines = append(panicLines, fmt.Sprintf(format, line))
	}

	for i := 0; i < files; i++ {
		u1 := strings.ReplaceAll(uuid.NewV4().String(), "-", "")
		u2 := strings.ReplaceAll(uuid.NewV4().String(), "-", "")
		name := path.Join(tempDir, "pod_ns_container-"+u1+u2+".log")
		logFile, _ := os.Create(name)

		lines := 100000
		for l := 0; l < lines; l++ {
			for _, line := range panicLines {
				_, _ = logFile.WriteString(line)
				_, _ = logFile.Write([]byte{'\n'})
			}

			stream := "stderr"
			if rand.Int()%3 == 0 {
				stream = "stderr"
			}
			if rand.Int()%100 == 0 {
				for k := 0; k < 8; k++ {
					_, _ = fmt.Fprintf(logFile, multilineJSON, stream)
					_, _ = logFile.Write([]byte{'\n'})
				}
			}
			_, _ = fmt.Fprintf(logFile, jsons[rand.Intn(len(jsons))], stream)
			_, _ = logFile.Write([]byte{'\n'})
		}

		time.Sleep(time.Second * 1)
		_ = logFile.Close()
		err := os.Remove(name)
		if err != nil {
			panic(err.Error())
		}
	}
}

/*
Plugins registered automatically after importing by init() function:

	_ "github.com/ozontech/file.d/plugin/output/devnull"
	_ "github.com/ozontech/file.d/plugin/output/elasticsearch"

Moving plugin in sub dir in plugin dir will quit registration quietly.
To prevent this let's check that DefaultPluginRegistry contains all plugins.
Plugins "dmesg", "journalctl" linux based, they contain tag: //go:build linux.
We don't check them.
*/
func TestThatPluginsAreImported(t *testing.T) {
	action := []string{
		"add_host",
		"debug",
		"discard",
		"flatten",
		"json_decode",
		"keep_fields",
		"mask",
		"modify",
		"parse_es",
		"parse_re2",
		"remove_fields",
		"rename",
		"throttle",
	}
	for _, pluginName := range action {
		pluginInfo := fd.DefaultPluginRegistry.Get(pipeline.PluginKindAction, pluginName)
		require.NotNil(t, pluginInfo)
	}

	input := []string{
		"fake",
		"file",
		"http",
		"k8s",
		"kafka",
	}
	for _, pluginName := range input {
		pluginInfo := fd.DefaultPluginRegistry.Get(pipeline.PluginKindInput, pluginName)
		require.NotNil(t, pluginInfo)
	}

	output := []string{
		"devnull",
		"elasticsearch",
		"file",
		"gelf",
		"kafka",
		"s3",
		"splunk",
		"stdout",
	}
	for _, pluginName := range output {
		pluginInfo := fd.DefaultPluginRegistry.Get(pipeline.PluginKindOutput, pluginName)
		require.NotNil(t, pluginInfo)
	}
}

type testConfig struct {
	name       string
	kind       pipeline.PluginKind
	configJSON string
}

func TestConfigParseValid(t *testing.T) {
	testList := []testConfig{
		{
			name:       "file",
			kind:       pipeline.PluginKindInput,
			configJSON: `{"offsets_op":"tail","persistence_mode":"sync","watching_dir":"/var/", "offsets_file": "./offset.yaml"}`,
		},
		{
			name:       "http",
			kind:       pipeline.PluginKindInput,
			configJSON: `{"address": ":9001","emulate_mode":"elasticsearch"}`,
		},
		{
			name:       "k8s",
			kind:       pipeline.PluginKindInput,
			configJSON: `{"split_event_size":1000,"watching_dir":"/var/log/containers/","offsets_file":"/data/k8s-offsets.yaml"}`,
		},
		{
			name:       "gelf",
			kind:       pipeline.PluginKindOutput,
			configJSON: `{"endpoint":"graylog.svc.cluster.local:12201","reconnect_interval":"1m","default_short_message_value":"message isn't provided"}`,
		},
		{
			name:       "splunk",
			kind:       pipeline.PluginKindOutput,
			configJSON: `{"endpoint":"splunk_endpoint","token":"value_token"}`,
		},
	}
	for _, tl := range testList {
		tl := tl
		t.Run(tl.name, func(t *testing.T) {
			t.Parallel()
			pluginInfo := fd.DefaultPluginRegistry.Get(tl.kind, tl.name)
			_, err := pipeline.GetConfig(pluginInfo, []byte(tl.configJSON), map[string]int{"gomaxprocs": 1, "capacity": 64})
			assert.NoError(t, err, "shouldn't be an error")
		})
	}
}

func TestConfigParseInvalid(t *testing.T) {
	testList := []testConfig{
		{
			name:       "http",
			kind:       pipeline.PluginKindInput,
			configJSON: `{"address": ":9001","emulate_mode":"yes","un_exist_field":"bla-bla"}`,
		},
		{
			name:       "k8s",
			kind:       pipeline.PluginKindInput,
			configJSON: `{"split_event_size":pp,"watching_dir":"/var/log/containers/","offsets_file":"/data/k8s-offsets.yaml"}`,
		},
		{
			name:       "gelf",
			kind:       pipeline.PluginKindOutput,
			configJSON: `{"reconnect_interval_1":"1m","default_short_message_value":"message isn't provided"}`,
		},
		{
			name:       "http",
			kind:       pipeline.PluginKindInput,
			configJSON: `{"address": ":9001","emulate_mode":"yes"}`,
		},
		{
			name:       "file",
			kind:       pipeline.PluginKindInput,
			configJSON: `{"offsets_op":"tail","persistence_mode":"sync","watching_dir":"/var/"}`,
		},
	}
	for _, tl := range testList {
		tl := tl
		t.Run(tl.name, func(t *testing.T) {
			t.Parallel()
			pluginInfo := fd.DefaultPluginRegistry.Get(tl.kind, tl.name)
			_, err := pipeline.GetConfig(pluginInfo, []byte(tl.configJSON), map[string]int{"gomaxprocs": 1, "capacity": 64})
			assert.Error(t, err, "should be an error")
		})
	}
}
