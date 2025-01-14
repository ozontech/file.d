package playground

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestHandler(t *testing.T) {
	t.Parallel()

	h := NewHandler(zap.NewNop())
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

	w := httptest.NewRecorder()
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

func TestHandlerBadRequest(t *testing.T) {
	t.Parallel()

	h := NewHandler(zap.NewNop())
	test := func(req DoActionsRequest, expErr string) {
		t.Helper()

		w := httptest.NewRecorder()
		reqRaw, _ := json.Marshal(req)
		h.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/api/play", bytes.NewReader(reqRaw)))
		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Equal(t, expErr, w.Body.String())
	}

	emptyEvents := []json.RawMessage{json.RawMessage("{}")}
	test(DoActionsRequest{}, "validate error: events count must be in range [1, 32] and actions count [0, 64]\n")

	test(DoActionsRequest{Events: emptyEvents, Actions: []json.RawMessage{[]byte(`{"type": "plugin that does not exist", "some": "args"}`)}},
		`do actions: can't find action plugin with type "plugin that does not exist"`+"\n")

	test(DoActionsRequest{Events: emptyEvents, Actions: []json.RawMessage{[]byte(`{"type": "clickhouse"}`)}},
		`do actions: can't find action plugin with type "clickhouse"`+"\n")

	test(DoActionsRequest{Events: emptyEvents, Actions: []json.RawMessage{[]byte(`{"type": "clickhouse"}`)}},
		`do actions: can't find action plugin with type "clickhouse"`+"\n")

	test(DoActionsRequest{Events: emptyEvents, Actions: []json.RawMessage{[]byte(`{"type": "convert_date", "some field value that does not exist": "123"}`)}},
		`do actions: wrong config for action 0/convert_date in pipeline "playground_4": json: unknown field "some field value that does not exist"`+"\n")

	test(DoActionsRequest{Events: []json.RawMessage{[]byte(`{ "field":{} }`)}, Actions: []json.RawMessage{[]byte(`{"type": "decode", "decoder": "nginx_error", "field": "field", "params": {"nginx_with_custom_fields":"decoder must call logger.Fatal()"}}`)}},
		`do actions: fatal: can't create nginx_error decoder: "error"="can't extract params: \"nginx_with_custom_fields\" must be bool"`+"\n")
}
