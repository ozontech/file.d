package file

import (
	"os"
	"testing"

	"github.com/ozontech/file.d/metric"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_deleteOneOffsetByField(t *testing.T) {
	tests := []struct {
		name      string
		fieldName string
		fieldVal  uint64
		fileData  string
		wantData  string
	}{
		{
			name:      "should_ok_when_file_is_correct",
			fieldName: "inode",
			fieldVal:  1,
			fileData: `
- file: /home/root/data.json
  inode: 1
  source_id: 650127812
  streams:
    stdout: 0
    stderr: 2548
- file: /home/root/data.json
  inode: 6305666
  source_id: 852926884
  streams:
    not_set: 692`,
			wantData: `- file: /home/root/data.json
  inode: 6305666
  source_id: 852926884
  streams:
    not_set: 692
`,
		},
		{
			name:      "should_ok_when_inode_is_broken",
			fieldName: "source_id",
			fieldVal:  1,
			fileData: `
- file: /home/root/data.json
  inode: 1:j2lk34jdv234
  source_id: 1
  streams:
    stdout: 0
    stderr: 2548
- file: /home/root/data.json
  inode: 6305666
  source_id: 852926884
  streams:
    not_set: 692`,
			wantData: `- file: /home/root/data.json
  inode: 6305666
  source_id: 852926884
  streams:
    not_set: 692
`,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			fname := "./one_offset_test.yaml"
			err := os.WriteFile(fname, []byte(tt.fileData), os.ModePerm)
			require.NoError(t, err)
			defer func() {
				errR := os.Remove(fname)
				require.NoError(t, errR)
			}()
			ctl := metric.NewCtl("test", prometheus.NewRegistry())
			metrics := newOffsetDbMetricCollection(
				ctl.RegisterCounter("worker1", "help_test"),
			)
			o := newOffsetDB(fname, "", metrics)

			deleteOneOffsetByField(o, tt.fieldName, tt.fieldVal)

			require.FileExists(t, fname)
			got, err := os.ReadFile(fname)
			require.NoError(t, err)
			assert.Equal(t, tt.wantData, string(got))
		})
	}
}
