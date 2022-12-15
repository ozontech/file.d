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

func TestDecodeAccessLogsJira(t *testing.T) {
	config := &Config{
		Field: "log",
		Re2:   "(?P<ip>[\\d]{1,3}\\.[\\d]{1,3}\\.[\\d]{1,3}\\.[\\d]{1,3}) (?P<id>\\w+) (?P<username>(\\w+|-)) \\[(?P<date>[\\d]{2}/[[:alpha:]]{1,3}/[\\d]{4}:[\\d]{2}:[\\d]{2}:[\\d]{2} [+-][\\d]{4})\\] \"(?P<path>.+)\" (?P<num1>([\\d]+|-)) (?P<num2>([\\d]+|-)) (?P<num3>([\\d]+|-)) \"(?P<path_2>.+)\" \"(?P<browser>.+)\" \"(?P<username_2>.+)\"",
	}
	p, input, output := test.NewPipelineMock(test.NewActionPluginStaticInfo(factory, config, pipeline.MatchModeAnd, nil, false))
	wg := &sync.WaitGroup{}
	wg.Add(2)

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

	input.In(0, "test.log", 0, []byte(`{"log":"10.115.195.13 0x51320775x2 jira_robot [07/Nov/2022:00:00:00 +0300] \"GET /rest/api/2/issue/FRAUD-3847?fields=resolution HTTP/1.1\" 200 198 20 \"https://jit.o3.ru/secure/RapidBoard.jspa?rapidView=2701&selectedIssue=EXPC-3767&quickFilter=16465&quickFilter=15365\" \"Apache-HttpClient/4.5.13 (Java/11.0.9)\" \"nj56zg\""}`))
	input.In(0, "test.log", 0, []byte(`{"log":"10.115.195.12 0x51320774x2 ezabelin [07/Nov/2022:00:00:00 +0300] \"GET /rest/api/2/issue/RP-4977?fields=resolution HTTP/1.1\" 201 158 15 \"-\" \"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36\" \"1tmznt9\""}`))

	wg.Wait()
	p.Stop()

	assert.Equal(t, 2, inEvents, "wrong in events count")
	assert.Equal(t, 2, len(outEvents), "wrong out events count")
	assert.Equal(t, `{"ip":"10.115.195.13","id":"0x51320775x2","username":"jira_robot","date":"07/Nov/2022:00:00:00 +0300","path":"GET /rest/api/2/issue/FRAUD-3847?fields=resolution HTTP/1.1","num1":"200","num2":"198","num3":"20","path_2":"https://jit.o3.ru/secure/RapidBoard.jspa?rapidView=2701&selectedIssue=EXPC-3767&quickFilter=16465&quickFilter=15365","browser":"Apache-HttpClient/4.5.13 (Java/11.0.9)","username_2":"nj56zg"}`, outEvents[0].Root.EncodeToString(), "wrong out event")
	assert.Equal(t, `{"ip":"10.115.195.12","id":"0x51320774x2","username":"ezabelin","date":"07/Nov/2022:00:00:00 +0300","path":"GET /rest/api/2/issue/RP-4977?fields=resolution HTTP/1.1","num1":"201","num2":"158","num3":"15","path_2":"-","browser":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36","username_2":"1tmznt9"}`, outEvents[1].Root.EncodeToString(), "wrong out event")
}
