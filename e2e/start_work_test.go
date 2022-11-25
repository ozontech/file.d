//go:build e2e_new

package e2e_test

import (
	"log"
	"testing"
	"time"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/e2e/file_file"
	"github.com/ozontech/file.d/e2e/http_file"
	"github.com/ozontech/file.d/e2e/kafka_file"
	"github.com/ozontech/file.d/fd"
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
	_ "github.com/ozontech/file.d/plugin/action/parse_re2"
	_ "github.com/ozontech/file.d/plugin/action/remove_fields"
	_ "github.com/ozontech/file.d/plugin/action/rename"
	_ "github.com/ozontech/file.d/plugin/action/set_time"
	_ "github.com/ozontech/file.d/plugin/action/throttle"
	_ "github.com/ozontech/file.d/plugin/input/dmesg"
	_ "github.com/ozontech/file.d/plugin/input/fake"
	_ "github.com/ozontech/file.d/plugin/input/file"
	_ "github.com/ozontech/file.d/plugin/input/http"
	_ "github.com/ozontech/file.d/plugin/input/journalctl"
	_ "github.com/ozontech/file.d/plugin/input/k8s"
	_ "github.com/ozontech/file.d/plugin/input/kafka"
	_ "github.com/ozontech/file.d/plugin/output/devnull"
	_ "github.com/ozontech/file.d/plugin/output/elasticsearch"
	_ "github.com/ozontech/file.d/plugin/output/file"
	_ "github.com/ozontech/file.d/plugin/output/gelf"
	_ "github.com/ozontech/file.d/plugin/output/kafka"
	_ "github.com/ozontech/file.d/plugin/output/postgres"
	_ "github.com/ozontech/file.d/plugin/output/s3"
	_ "github.com/ozontech/file.d/plugin/output/splunk"
	_ "github.com/ozontech/file.d/plugin/output/stdout"
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
	}

	startForTest(t, testsList)

	for _, test := range testsList {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			test.Send(t)
			test.Validate(t)
		})
	}
}

func startForTest(t *testing.T, tests []E2ETest) *fd.FileD {
	configs := make([]*cfg.Config, len(tests))
	for i, test := range tests {
		conf := cfg.NewConfigFromFile(test.cfgPath)
		if _, ok := conf.Pipelines[test.name]; !ok {
			log.Fatalf("pipeline name must be named the same as the name of the test")
		}
		test.Configure(t, conf, test.name)
		configs[i] = conf
	}

	conf := mergeConfigs(configs)
	filed := fd.New(conf, "off")
	filed.Start()
	return filed
}

func mergeConfigs(configs []*cfg.Config) *cfg.Config {
	mergedConfig := new(cfg.Config)
	mergedConfig.Pipelines = make(map[string]*cfg.PipelineConfig)
	mergedConfig.PanicTimeout = time.Millisecond

	for _, config := range configs {
		for pipelineName, pipelineConfig := range config.Pipelines {
			if _, ok := mergedConfig.Pipelines[pipelineName]; ok {
				log.Fatalf(`pipeline with such name = %s alredy exist`, pipelineName)
			}
			mergedConfig.Pipelines[pipelineName] = pipelineConfig
		}
	}

	return mergedConfig
}
