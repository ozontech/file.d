package parse_re2

import (
	"sync"
	"testing"

	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/test"
	"github.com/stretchr/testify/assert"
)

func TestDecode(t *testing.T) {
	config := test.NewConfig(&Config{
		Field:  "log",
		Prefix: "prefix.",
		Re2:    "(?P<date>[\\d]{4}-[\\d]{2}-[\\d]{2} [\\d]{2}:[\\d]{2}:[\\d]{2} GMT) \\[(?P<pid>[\\d]+)\\] => \\[(?P<pid_message_number>[\\d-]+)\\] client=(?P<client>[^,]*),db=(?P<db>[^,]*),user=(?P<user>[^,]*) (LOG|HINT):  (?P<message>.+)",
	}, nil)
	p, input, output := test.NewPipelineMock(test.NewActionPluginStaticInfo(factory, config, pipeline.MatchModeAnd, nil, false))
	wg := &sync.WaitGroup{}
	wg.Add(1)

	output.SetOutFn(func(e *pipeline.Event) {
		assert.Equal(t, `{"prefix.date":"2021-06-22 16:24:27 GMT","prefix.pid":"7291","prefix.pid_message_number":"2-1","prefix.client":"test_client","prefix.db":"test_db","prefix.user":"test_user","prefix.message":"listening on IPv4 address \"0.0.0.0\", port 5432"}`,
			e.Root.EncodeToString(), "wrong out event")
		wg.Done()
	})

	input.In(0, "test.log", test.Offset(0), []byte(`{"log":"2021-06-22 16:24:27 GMT [7291] => [2-1] client=test_client,db=test_db,user=test_user LOG:  listening on IPv4 address \"0.0.0.0\", port 5432"}`))

	wg.Wait()
	p.Stop()
}

func TestDecodeAccessLogsJira(t *testing.T) {
	config := test.NewConfig(&Config{
		Field: "message",
		Re2:   "(?P<origin_ip>[\\d]{1,3}\\.[\\d]{1,3}\\.[\\d]{1,3}\\.[\\d]{1,3}) (?P<request_id>\\w+) (?P<username>(\\w+|-)) \\[(?P<timestamp>[\\d]{2}/[[:alpha:]]{1,3}/[\\d]{4}:[\\d]{2}:[\\d]{2}:[\\d]{2} [+-][\\d]{4})\\] \"(?P<method_and_endpoint>.+)\" (?P<status_code>([\\d]+|-)) (?P<bytes_sent>([\\d]+|-)) (?P<processing_time>([\\d]+|-)) \"(?P<accessed_url>.+)\" \"(?P<client>.+)\" \"(?P<session_id>.+)\"",
	}, nil)
	p, input, output := test.NewPipelineMock(test.NewActionPluginStaticInfo(factory, config, pipeline.MatchModeAnd, nil, false))
	wg := &sync.WaitGroup{}
	wg.Add(2)

	outEvents := make([]string, 0, 2)
	output.SetOutFn(func(e *pipeline.Event) {
		outEvents = append(outEvents, e.Root.EncodeToString())
		wg.Done()
	})

	input.In(0, "test.log", test.Offset(0), []byte(`{"message":"10.115.195.13 0x51320775x2 jira_robot [07/Nov/2022:00:00:00 +0300] \"GET /rest/api/2/issue/FRAUD-3847?fields=resolution HTTP/1.1\" 200 198 20 \"https://jit.o3.ru/secure/RapidBoard.jspa?rapidView=2701&selectedIssue=EXPC-3767&quickFilter=16465&quickFilter=15365\" \"Apache-HttpClient/4.5.13 (Java/11.0.9)\" \"nj56zg\""}`))
	input.In(0, "test.log", test.Offset(0), []byte(`{"message":"10.115.195.12 0x51320774x2 ezabelin [07/Nov/2022:00:00:00 +0300] \"GET /rest/api/2/issue/RP-4977?fields=resolution HTTP/1.1\" 201 158 15 \"-\" \"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36\" \"1tmznt9\""}`))

	wg.Wait()
	p.Stop()

	assert.Equal(t, 2, len(outEvents), "wrong out events count")
	assert.Equal(t, `{"origin_ip":"10.115.195.13","request_id":"0x51320775x2","username":"jira_robot","timestamp":"07/Nov/2022:00:00:00 +0300","method_and_endpoint":"GET /rest/api/2/issue/FRAUD-3847?fields=resolution HTTP/1.1","status_code":"200","bytes_sent":"198","processing_time":"20","accessed_url":"https://jit.o3.ru/secure/RapidBoard.jspa?rapidView=2701&selectedIssue=EXPC-3767&quickFilter=16465&quickFilter=15365","client":"Apache-HttpClient/4.5.13 (Java/11.0.9)","session_id":"nj56zg"}`,
		outEvents[0], "wrong out event")
	assert.Equal(t, `{"origin_ip":"10.115.195.12","request_id":"0x51320774x2","username":"ezabelin","timestamp":"07/Nov/2022:00:00:00 +0300","method_and_endpoint":"GET /rest/api/2/issue/RP-4977?fields=resolution HTTP/1.1","status_code":"201","bytes_sent":"158","processing_time":"15","accessed_url":"-","client":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36","session_id":"1tmznt9"}`,
		outEvents[1], "wrong out event")
}
