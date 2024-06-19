package test

import (
	"reflect"
	"testing"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/plugin/action/add_file_name"
	"github.com/ozontech/file.d/plugin/action/add_host"
	"github.com/ozontech/file.d/plugin/action/convert_date"
	"github.com/ozontech/file.d/plugin/action/convert_log_level"
	"github.com/ozontech/file.d/plugin/action/convert_utf8_bytes"
	"github.com/ozontech/file.d/plugin/action/debug"
	"github.com/ozontech/file.d/plugin/action/discard"
	"github.com/ozontech/file.d/plugin/action/flatten"
	"github.com/ozontech/file.d/plugin/action/join"
	"github.com/ozontech/file.d/plugin/action/join_template"
	"github.com/ozontech/file.d/plugin/action/json_decode"
	"github.com/ozontech/file.d/plugin/action/json_encode"
	"github.com/ozontech/file.d/plugin/action/json_extract"
	"github.com/ozontech/file.d/plugin/action/keep_fields"
	"github.com/ozontech/file.d/plugin/action/mask"
	"github.com/ozontech/file.d/plugin/action/modify"
	"github.com/ozontech/file.d/plugin/action/move"
	"github.com/ozontech/file.d/plugin/action/parse_es"
	"github.com/ozontech/file.d/plugin/action/parse_re2"
	"github.com/ozontech/file.d/plugin/action/remove_fields"
	"github.com/ozontech/file.d/plugin/action/rename"
	"github.com/ozontech/file.d/plugin/action/set_time"
	"github.com/ozontech/file.d/plugin/action/split"
	"github.com/ozontech/file.d/plugin/action/throttle"
	"github.com/ozontech/file.d/plugin/input/fake"
	filein "github.com/ozontech/file.d/plugin/input/file"
	"github.com/ozontech/file.d/plugin/input/http"
	"github.com/ozontech/file.d/plugin/input/k8s"
	"github.com/ozontech/file.d/plugin/output/clickhouse"
	"github.com/ozontech/file.d/plugin/output/devnull"
	"github.com/ozontech/file.d/plugin/output/elasticsearch"
	fileout "github.com/ozontech/file.d/plugin/output/file"
	"github.com/ozontech/file.d/plugin/output/gelf"
	"github.com/ozontech/file.d/plugin/output/kafka"
	"github.com/ozontech/file.d/plugin/output/postgres"
	"github.com/ozontech/file.d/plugin/output/s3"
	"github.com/ozontech/file.d/plugin/output/splunk"
	"github.com/stretchr/testify/require"
)

func findStruct(t *testing.T, configs []any) {
	for _, config := range configs {
		valueInfo := reflect.ValueOf(config)
		typeInfo := valueInfo.Type()
		t.Log(typeInfo.PkgPath())

		for i := 0; i < valueInfo.NumField(); i++ {
			if valueInfo.Field(i).Kind() == reflect.Struct {
				t.Log(typeInfo.Field(i).Name)
			}
		}
	}
}

func TestFindStructs(t *testing.T) {
	t.Log("input configs")
	findStruct(t, []any{
		// check manually
		// dmesg.Config{},
		fake.Config{},
		filein.Config{},
		http.Config{},
		// check manually
		// journalctl.Config{},
		k8s.Config{},
		kafka.Config{},
	})

	t.Log("output configs")
	findStruct(t, []any{
		clickhouse.Config{},
		devnull.Config{},
		elasticsearch.Config{},
		fileout.Config{},
		gelf.Config{},
		kafka.Config{},
		postgres.Config{},
		s3.Config{},
		splunk.Config{},
	})

	t.Log("action configs")
	findStruct(t, []any{
		add_file_name.Config{},
		add_host.Config{},
		convert_date.Config{},
		convert_log_level.Config{},
		convert_utf8_bytes.Config{},
		debug.Config{},
		discard.Config{},
		flatten.Config{},
		join.Config{},
		join_template.Config{},
		json_decode.Config{},
		json_encode.Config{},
		json_extract.Config{},
		keep_fields.Config{},
		mask.Config{},
		// modify.Config{},
		move.Config{},
		parse_es.Config{},
		parse_re2.Config{},
		remove_fields.Config{},
		// rename.Config{},
		set_time.Config{},
		split.Config{},
		throttle.Config{},
	})

	// not structs; func Parse does nothing
	var err error
	err = cfg.Parse(&modify.Config{}, nil)
	require.NoError(t, err)
	err = cfg.Parse(&rename.Config{}, nil)
	require.NoError(t, err)
}
