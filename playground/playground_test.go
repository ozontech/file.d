package playground

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ozontech/file.d/fd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestHandler(t *testing.T) {
	t.Parallel()

	h := NewHandler(fd.DefaultPluginRegistry, zap.NewNop())
	w := httptest.NewRecorder()
	const fdConfig = `{
  "events": [{"pipeline_kafka_topic":"obs-seq-db-logs","time":"2023-08-22T10:01:09.282965716Z","stream":"stdout","k8s_node":"kuber-node65329z501","k8s_namespace":"logging","k8s_pod":"seq-proxy-prod-6ccc888994-nkj94","k8s_container":"seq-proxy","k8s_pod_label_app":"seq-proxy-prod","zone":"z501","k8s_cluster":"obs","env":"infra-ts","level":"informational","ts":1692698469,"message":"search","req":{"query":"(access_token_leaked: \"jwt_token\")","offset":0,"size":2,"from":"2023-08-21 10:01:09","to":"2023-08-22 10:01:09","with_total":false,"explain":false,"agg_name":"","agg_field":"","agg_filter":"","interval":"0s"}}],
  "actions": [
    {"type": "modify", "from":"${req.from}", "to":"${req.to}"},
      {
        "type": "convert_date",
        "field": "from",
        "source_formats": ["rfc3339nano", "rfc3339", "2006-01-02 15:04:05"],
        "target_format": "rfc3339nano",
        "remove_on_fail": false
      },
      {
        "type": "convert_date",
        "field": "to",
        "source_formats": ["rfc3339nano", "rfc3339", "2006-01-02 15:04:05"],
        "target_format": "rfc3339nano",
        "remove_on_fail": false
      },
	  {
		"type": "keep_fields",
		"fields": ["from", "to"]
	  }
  ],
  "debug": false
}`

	h.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/api/play", strings.NewReader(fdConfig)))
	assert.Equal(t, http.StatusOK, w.Code)
	resp := DoActionsResponse{}
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	assert.True(t, resp.Stdout != "")
	assert.True(t, resp.Metrics != "")
	assert.True(t, len(resp.Result) == 1)

	result := struct {
		From string `json:"to"`
		To   string `json:"from"`
	}{}
	_ = json.Unmarshal(resp.Result[0].Event, &result)
	require.Equal(t, result.From, "2023-08-22T10:01:09Z")
	require.Equal(t, result.To, "2023-08-21T10:01:09Z")
}
