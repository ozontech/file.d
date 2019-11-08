package main

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	uuid "github.com/satori/go.uuid"
	"gitlab.ozon.ru/sre/filed/filed"
	_ "gitlab.ozon.ru/sre/filed/plugin/action/discard"
	_ "gitlab.ozon.ru/sre/filed/plugin/action/json_decode"
	"gitlab.ozon.ru/sre/filed/plugin/action/k8s"
	_ "gitlab.ozon.ru/sre/filed/plugin/action/keep_fields"
	_ "gitlab.ozon.ru/sre/filed/plugin/action/rename"
	_ "gitlab.ozon.ru/sre/filed/plugin/action/throttle"
	_ "gitlab.ozon.ru/sre/filed/plugin/input/fake"
	_ "gitlab.ozon.ru/sre/filed/plugin/input/file"
	_ "gitlab.ozon.ru/sre/filed/plugin/output/devnull"
	_ "gitlab.ozon.ru/sre/filed/plugin/output/kafka"
)

var jsons = []string{
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

var panicContent = `panic: assignment to entry in nil map

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

var multilineJson = `{"log":"log","stream":"%s","service":"1"}`

func gen(tempDir string, files int, wg *sync.WaitGroup) {
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
			//if rand.Int()%2 == 0 {
				for _, line := range panicLines {
					_, _ = file.WriteString(line)
					_, _ = file.Write([]byte{'\n'})
				}
				continue
			//}

			stream := "stderr"
			if rand.Int()%3 == 0 {
				stream = "stderr"
			}
			if rand.Int()%100 == 0 {
				for k := 0; k < 8; k++ {
					_, _ = file.WriteString(fmt.Sprintf(multilineJson, stream))
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

	wg.Done()
}

func TestEndToEnd(t *testing.T) {
	// we are very deterministic :)
	rand.Seed(0)

	k8s.DisableMetaUpdates = true
	k8s.MetaWaitTimeout = time.Millisecond
	k8s.MaintenanceInterval = time.Millisecond * 100

	filesDir, _ := ioutil.TempDir("", "filed")
	offsetsDir, _ := ioutil.TempDir("", "filed")

	config := filed.NewConfigFromFile("./../testdata/config/simple.yaml")
	input := config.Pipelines["test"].Raw.Get("input")
	input.Set("watching_dir", filesDir)
	input.Set("offsets_file", filepath.Join(offsetsDir, "offsets.yaml"))

	fd := filed.New(config, ":9000")
	fd.Start()

	wg := &sync.WaitGroup{}
	for {
		jobs := 8
		for i := 0; i < jobs; i++ {
			wg.Add(1)
			go gen(filesDir, 1, wg)
		}

		time.Sleep(time.Second * 10)
	}

	time.Sleep(time.Second * 1000)
	wg.Wait()
	fd.Stop()

	_ = os.RemoveAll(filesDir)
	_ = os.RemoveAll(offsetsDir)
}
