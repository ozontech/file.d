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

var multilineJson = `{"log":"log","stream":"%s","service":"1"}`

func gen(tempDir string, files int, wg *sync.WaitGroup) {
	for i := 0; i < files; i++ {
		u1 := strings.ReplaceAll(uuid.NewV4().String(), "-", "")
		u2 := strings.ReplaceAll(uuid.NewV4().String(), "-", "")
		name := path.Join(tempDir, "pod_ns_container-"+u1+u2+".log")
		file, _ := os.Create(name)

		//intervals := []int{0, 5}
		//interval := intervals[rand.Int()%len(intervals)]
		lines := 100000
		for l := 0; l < lines; l++ {
			stream := "stdout"
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
			//time.Sleep(time.Duration(interval) * time.Millisecond)
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
		jobs := 64
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
