//go:build e2e_new

package e2e_test

import (
	"context"
	"log"
	"strconv"
	"testing"
	"time"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/e2e/file_clickhouse"
	"github.com/ozontech/file.d/e2e/file_file"
	"github.com/ozontech/file.d/e2e/http_file"
	"github.com/ozontech/file.d/e2e/join_throttle"
	"github.com/ozontech/file.d/e2e/kafka_auth"
	"github.com/ozontech/file.d/e2e/kafka_file"
	"github.com/ozontech/file.d/e2e/split_join"
	"github.com/ozontech/file.d/fd"
	_ "github.com/ozontech/file.d/plugin/action/add_file_name"
	_ "github.com/ozontech/file.d/plugin/action/add_host"
	_ "github.com/ozontech/file.d/plugin/action/convert_date"
	_ "github.com/ozontech/file.d/plugin/action/convert_log_level"
	_ "github.com/ozontech/file.d/plugin/action/debug"
	_ "github.com/ozontech/file.d/plugin/action/discard"
	_ "github.com/ozontech/file.d/plugin/action/flatten"
	_ "github.com/ozontech/file.d/plugin/action/join"
	_ "github.com/ozontech/file.d/plugin/action/join_template"
	_ "github.com/ozontech/file.d/plugin/action/json_decode"
	_ "github.com/ozontech/file.d/plugin/action/json_encode"
	_ "github.com/ozontech/file.d/plugin/action/keep_fields"
	_ "github.com/ozontech/file.d/plugin/action/mask"
	_ "github.com/ozontech/file.d/plugin/action/modify"
	_ "github.com/ozontech/file.d/plugin/action/parse_es"
	_ "github.com/ozontech/file.d/plugin/action/parse_http"
	_ "github.com/ozontech/file.d/plugin/action/parse_re2"
	_ "github.com/ozontech/file.d/plugin/action/remove_fields"
	_ "github.com/ozontech/file.d/plugin/action/rename"
	_ "github.com/ozontech/file.d/plugin/action/set_time"
	_ "github.com/ozontech/file.d/plugin/action/split"
	_ "github.com/ozontech/file.d/plugin/action/throttle"
	_ "github.com/ozontech/file.d/plugin/input/dmesg"
	_ "github.com/ozontech/file.d/plugin/input/fake"
	_ "github.com/ozontech/file.d/plugin/input/file"
	_ "github.com/ozontech/file.d/plugin/input/http"
	_ "github.com/ozontech/file.d/plugin/input/journalctl"
	_ "github.com/ozontech/file.d/plugin/input/k8s"
	_ "github.com/ozontech/file.d/plugin/input/kafka"
	_ "github.com/ozontech/file.d/plugin/output/clickhouse"
	_ "github.com/ozontech/file.d/plugin/output/devnull"
	_ "github.com/ozontech/file.d/plugin/output/elasticsearch"
	_ "github.com/ozontech/file.d/plugin/output/file"
	_ "github.com/ozontech/file.d/plugin/output/gelf"
	_ "github.com/ozontech/file.d/plugin/output/kafka"
	_ "github.com/ozontech/file.d/plugin/output/postgres"
	_ "github.com/ozontech/file.d/plugin/output/s3"
	_ "github.com/ozontech/file.d/plugin/output/splunk"
	_ "github.com/ozontech/file.d/plugin/output/stdout"
	"github.com/stretchr/testify/assert"
)

// e2eTest is the general interface for e2e tests
// Configure prepares config for your test
// Send sends message in pipeline and waits for the end of processing
// Validate validates result of the work
type e2eTest interface {
	Configure(t *testing.T, conf *cfg.Config, pipelineName string)
	Send(t *testing.T)
	Validate(t *testing.T)
}

type E2ETest struct {
	name    string
	cfgPath string
	e2eTest
}

func TestE2EStabilityWorkCase(t *testing.T) {
	testsList := []E2ETest{
		{
			name: "kafka_auth",
			e2eTest: &kafka_auth.Config{
				Brokers: []string{"localhost:9093"},
			},
			cfgPath: "./kafka_auth/config.yml",
		},
		{
			name: "file_file",
			e2eTest: &file_file.Config{
				Count:   10,
				Lines:   500,
				RetTime: "1s",
			},
			cfgPath: "./file_file/config.yml",
		},
		{
			name: "http_file",
			e2eTest: &http_file.Config{
				Count:   10,
				Lines:   500,
				RetTime: "1s",
			},
			cfgPath: "./http_file/config.yml",
		},
		{
			name: "kafka_file",
			e2eTest: &kafka_file.Config{
				Topics:    []string{"quickstart"},
				Brokers:   []string{"localhost:9092"},
				Count:     500,
				RetTime:   "1s",
				Partition: 4,
			},
			cfgPath: "./kafka_file/config.yml",
		},
		{
			name: "join_throttle",
			e2eTest: &join_throttle.Config{
				Count: 1000,
			},
			cfgPath: "./join_throttle/config.yml",
		},
		{
			name:    "split_join",
			e2eTest: &split_join.Config{},
			cfgPath: "./split_join/config.yml",
		},
		{
			name:    "file_clickhouse",
			e2eTest: &file_clickhouse.Config{},
			cfgPath: "./file_clickhouse/config.yml",
		},
	}

	for num, test := range testsList {
		test := test
		num := num
		t.Run(test.name, func(t *testing.T) {
			fd := startForTest(t, test, num)
			t.Parallel()
			test.Send(t)
			test.Validate(t)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			err := fd.Stop(ctx)
			cancel()
			assert.NoError(t, err)
		})
	}
}

func startForTest(t *testing.T, test E2ETest, num int) *fd.FileD {
	conf := cfg.NewConfigFromFile(test.cfgPath)
	if _, ok := conf.Pipelines[test.name]; !ok {
		log.Fatalf("pipeline name must be named the same as the name of the test")
	}
	test.Configure(t, conf, test.name)

	// for each file.d its own port
	filed := fd.New(conf, ":1508"+strconv.Itoa(num))
	filed.Start()
	return filed
}
