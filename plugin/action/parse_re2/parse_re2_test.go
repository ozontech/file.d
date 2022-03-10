package parse_re2

import (
	"sync"
	"testing"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/test"
	"github.com/stretchr/testify/assert"
)

func TestDecode(t *testing.T) {
	config := &Config{
		Field:  "log",
		Prefix: "prefix.",
		Re2:    "(?P<date>[\\d]{4}-[\\d]{2}-[\\d]{2} [\\d]{2}:[\\d]{2}:[\\d]{2} GMT) \\[(?P<pid>[\\d]+)\\] => \\[(?P<pid_message_number>[\\d-]+)\\] client=(?P<client>[^,]*),db=(?P<db>[^,]*),user=(?P<user>[^,]*) (LOG|HINT):  (?P<message>.+)",
	}
	p, input, output := test.NewPipelineMock(test.NewActionPluginStaticInfo(factory, config, pipeline.MatchModeAnd, nil, false))
	wg := &sync.WaitGroup{}
	wg.Add(1)

	err := cfg.Parse(config, nil)
	if err != nil {
		logger.Panicf("wrong config")
	}

	inEvents := 0
	input.SetInFn(func() {
		inEvents++
	})

	outEvents := make([]*pipeline.Event, 0)
	output.SetOutFn(func(e *pipeline.Event) {
		outEvents = append(outEvents, e)
		wg.Done()
	})

	input.In(0, "test.log", 0, []byte(`{"log":"2021-06-22 16:24:27 GMT [7291] => [2-1] client=test_client,db=test_db,user=test_user LOG:  listening on IPv4 address \"0.0.0.0\", port 5432"}`))

	wg.Wait()
	p.Stop()

	assert.Equal(t, 1, inEvents, "wrong in events count")
	assert.Equal(t, 1, len(outEvents), "wrong out events count")
	assert.Equal(t, `{"prefix.date":"2021-06-22 16:24:27 GMT","prefix.pid":"7291","prefix.pid_message_number":"2-1","prefix.client":"test_client","prefix.db":"test_db","prefix.user":"test_user","prefix.message":"listening on IPv4 address \"0.0.0.0\", port 5432"}`, outEvents[0].Root.EncodeToString(), "wrong out event")
}
