package main

import (
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
	//`{"log":"one\n","stream":"stderr"}`,

	//`{"log":"one","stream":"stdout"}`,
	//`{"log":"one two","stream":"stdout"}`,
	//`{"log":"log one two three","stream":"stdout"}`,
	//`{"log":"dropped log one", "should_drop":"ok","stream":"stdout"}`,
	//`{"log":"dropped log one two", "should_drop":"ok","stream":"stdout"}`,
	//`{"log":"dropped log one three", "should_drop":"ok","stream":"stdout"}`,
	//`{"log":"throttled log one", "throttle":"1","stream":"stdout"}`,
	//`{"log":"throttled log one two", "throttle":"2","stream":"stdout"}`,
	//`{"log":"throttled log one three", "throttle":"3","stream":"stdout"}`,

	`{"log":"one\n","stream":"stdout"}`,
	`{"log":"one two\n","stream":"stdout"}`,
	`{"log":"log one two three\n","stream":"stdout"}`,
	`{"log":"dropped log one\n", "should_drop":"ok","stream":"stdout"}`,
	`{"log":"dropped log one two\n", "should_drop":"ok","stream":"stdout"}`,
	`{"log":"dropped log one three\n", "should_drop":"ok","stream":"stdout"}`,
	`{"log":"throttled log one\n", "throttle":"1","stream":"stdout"}`,
	`{"log":"throttled log one two\n", "throttle":"2","stream":"stdout"}`,
	`{"log":"throttled log one three\n", "throttle":"3","stream":"stdout"}`,
}

var multilineJson = `{"log":"log","stream":"stdout"}`

func gen(tempDir string, files int, wg *sync.WaitGroup) {
	for i := 0; i < files; i++ {
		u := strings.ReplaceAll(uuid.NewV4().String(), "-", "")
		name := path.Join(tempDir, "pod_ns_container-"+u+u+".log")
		file, _ := os.Create(name)

		intervals := []int{0, 5, 15}
		interval := intervals[rand.Int()%len(intervals)]
		lines := 1000 // + rand.Int()%10000
		for l := 0; l < lines; l++ {
			if rand.Int()%2 == 0 {
				for k := 0; k < 3; k++ {
					_, _ = file.WriteString(multilineJson)
					_, _ = file.Write([]byte{'\n'})
				}
			}
			_, _ = file.WriteString(jsons[rand.Int()%len(jsons)])
			_, _ = file.Write([]byte{'\n'})
			time.Sleep(time.Duration(interval) * time.Millisecond)
		}

		_ = file.Close()
		//_ = os.Remove(name)
	}

	wg.Done()
}

func TestEndToEnd(t *testing.T) {
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
	//jobs := 16
	//files := 1
	//for {
	for i := 0; i < 15; i++ {
		wg.Add(1)
		go gen(filesDir, 5, wg)
	}
	//time.Sleep(time.Second * 5)
	//}

	time.Sleep(time.Second * 1000)
	wg.Wait()
	fd.Stop()

	_ = os.RemoveAll(filesDir)
	_ = os.RemoveAll(offsetsDir)
}
