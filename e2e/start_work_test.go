//go:build e2e_new

package e2e_test

import (
	"testing"

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

/*
General interface for e2e tests
AddConfigSettings - function that preparing а config for your test
Send - function that send message in pipeline and waits for the end of processing
Validate - function that validate result of the work
*/

type e2eTest interface {
	AddConfigSettings(t *testing.T, conf *cfg.Config, pipeName string)
	Send(t *testing.T)
	Validate(t *testing.T)
}

func startForTest(t *testing.T, e e2eTest, configPath, pipeName string) *fd.FileD {
	conf := cfg.NewConfigFromFile(configPath)
	e.AddConfigSettings(t, conf, pipeName)
	filed := fd.New(conf, "off")
	filed.Start()
	return filed
}

func TestE2EStabilityWorkCase(t *testing.T) {
	testsList := []struct {
		e2eTest
		cfgPath  string
		pipeName string
	}{
		{
			e2eTest: &file_file.Config{
				OutputNamePattern: "/file-d.log",
				FilePattern:       "pod_ns_container-*",
				Count:             10,
				Lines:             500,
				RetTime:           "1s",
			},
			cfgPath:  "./file_file/config.yml",
			pipeName: "test_file_file",
		},
		{
			e2eTest: &http_file.Config{
				OutputNamePattern: "/file-d.log",
				Count:             10,
				Lines:             500,
				RetTime:           "1s",
			},
			cfgPath:  "./http_file/config.yml",
			pipeName: "test_http_file",
		},
		{
			e2eTest: &kafka_file.Config{
				Topic:             "quickstart5",
				Broker:            "localhost:9092",
				OutputNamePattern: "/file-d.log",
				Lines:             500,
				RetTime:           "1s",
				Partition:         4,
			},
			cfgPath:  "./kafka_file/config.yml",
			pipeName: "test_kafka_file",
		},
	}

	for _, test := range testsList {
		startForTest(t, test.e2eTest, test.cfgPath, test.pipeName)
		test.Send(t)
		test.Validate(t)
	}
}
