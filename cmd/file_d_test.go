package main

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"time"

	uuid "github.com/satori/go.uuid"
	"gitlab.ozon.ru/sre/file-d/fd"
	_ "gitlab.ozon.ru/sre/file-d/plugin/action/discard"
	_ "gitlab.ozon.ru/sre/file-d/plugin/action/json_decode"
	"gitlab.ozon.ru/sre/file-d/plugin/action/k8s"
	_ "gitlab.ozon.ru/sre/file-d/plugin/action/keep_fields"
	_ "gitlab.ozon.ru/sre/file-d/plugin/action/rename"
	_ "gitlab.ozon.ru/sre/file-d/plugin/action/throttle"
	_ "gitlab.ozon.ru/sre/file-d/plugin/input/fake"
	_ "gitlab.ozon.ru/sre/file-d/plugin/input/file"
	_ "gitlab.ozon.ru/sre/file-d/plugin/output/devnull"
	_ "gitlab.ozon.ru/sre/file-d/plugin/output/kafka"
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

// TestEndToEnd creates near-realistic workload and setups a complex pipeline.
// It's something like fuzz testing. file-d shouldn't crash/panic or hang for infinite time.
// Keep this test running while you are sleeping is a very good idea :)
func TestEndToEnd(t *testing.T) {
	configFilename := "./../testdata/config/e2e.yaml"
	testTime := time.Minute
	iterationInterval := time.Second * 10
	writerCount := 8
	fileCount := 8

	// we are very deterministic :)
	rand.Seed(0)

	// disable k8s environment
	k8s.DisableMetaUpdates = true
	k8s.MetaWaitTimeout = time.Millisecond
	k8s.MaintenanceInterval = time.Millisecond * 100

	filesDir, _ := ioutil.TempDir("", "file-d")
	offsetsDir, _ := ioutil.TempDir("", "file-d")

	config := fd.NewConfigFromFile(configFilename)
	input := config.Pipelines["test"].Raw.Get("input")
	input.Set("watching_dir", filesDir)
	input.Set("offsets_file", filepath.Join(offsetsDir, "offsets.yaml"))

	fd := fd.New(config, ":9000")
	fd.Start()

	tm := time.Now()
	for {
		for i := 0; i < writerCount; i++ {
			go runWriter(filesDir, fileCount)
		}

		time.Sleep(iterationInterval)
		if time.Now().Sub(tm) > testTime {
			break
		}
	}
}

func runWriter(tempDir string, files int) {
	format := `{"log":"%s\n","stream":"stderr"}`
	panicLines := make([]string, 0, 0)
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
		file, _ := os.Create(name)

		lines := 100000
		for l := 0; l < lines; l++ {
			for _, line := range panicLines {
				_, _ = file.WriteString(line)
				_, _ = file.Write([]byte{'\n'})
			}

			stream := "stderr"
			if rand.Int()%3 == 0 {
				stream = "stderr"
			}
			if rand.Int()%100 == 0 {
				for k := 0; k < 8; k++ {
					_, _ = file.WriteString(fmt.Sprintf(multilineJSON, stream))
					_, _ = file.Write([]byte{'\n'})
				}
			}
			_, _ = file.WriteString(fmt.Sprintf(jsons[rand.Int()%len(jsons)], stream))
			_, _ = file.Write([]byte{'\n'})
		}

		time.Sleep(time.Second * 1)
		_ = file.Close()
		err := os.Remove(name)
		if err != nil {
			panic(err.Error())
		}
	}
}
