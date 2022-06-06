package http

import (
	"bytes"
	"io"
	"io/ioutil"
	"net/http"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/test"
)

func getInputInfo() *pipeline.InputPluginInfo {
	input, _ := Factory()
	return &pipeline.InputPluginInfo{
		PluginStaticInfo: &pipeline.PluginStaticInfo{
			Type:    "",
			Factory: nil,
			Config:  &Config{Address: "off"},
		},
		PluginRuntimeInfo: &pipeline.PluginRuntimeInfo{
			Plugin: input,
			ID:     "",
		},
	}
}

func getInputInfoWithAddr() *pipeline.InputPluginInfo {
	config := &Config{Address: ":9200"}
	_ = cfg.Parse(config, map[string]int{"gomaxprocs": runtime.GOMAXPROCS(0)})
	input, _ := Factory()
	return &pipeline.InputPluginInfo{
		PluginStaticInfo: &pipeline.PluginStaticInfo{
			Type:    "http",
			Factory: nil,
			Config:  config,
		},
		PluginRuntimeInfo: &pipeline.PluginRuntimeInfo{
			Plugin: input,
			ID:     "http",
		},
	}
}

func TestProcessChunksMany(t *testing.T) {
	p, _, output := test.NewPipelineMock(nil, "passive")
	p.SetInput(getInputInfo())
	input := p.GetInput().(*Plugin)
	p.Start()

	wg := &sync.WaitGroup{}
	wg.Add(3)

	outEvents := make([]string, 0)
	output.SetOutFn(func(event *pipeline.Event) {
		outEvents = append(outEvents, event.Root.EncodeToString())
		wg.Done()
	})

	chunk := []byte(`{"a":"1"}
{"a":"2"}
{"a":"3"}
`)
	eventBuff := make([]byte, 0)
	eventBuff = input.processChunk(0, chunk, eventBuff)

	wg.Wait()
	p.Stop()

	assert.Equal(t, 3, len(outEvents), "wrong events count")
	assert.Equal(t, `{"a":"1"}`, outEvents[0], "wrong event")
	assert.Equal(t, `{"a":"2"}`, outEvents[1], "wrong event")
	assert.Equal(t, `{"a":"3"}`, outEvents[2], "wrong event")
	assert.Equal(t, 0, len(eventBuff), "wrong event")
}

func TestProcessChunksEventBuff(t *testing.T) {
	p, _, output := test.NewPipelineMock(nil, "passive")
	p.SetInput(getInputInfo())
	input := p.GetInput().(*Plugin)
	p.Start()

	wg := &sync.WaitGroup{}
	wg.Add(2)

	outEvents := make([]string, 0)
	output.SetOutFn(func(event *pipeline.Event) {
		outEvents = append(outEvents, event.Root.EncodeToString())
		wg.Done()
	})

	chunk := []byte(`{"a":"1"}
{"a":"2"}
{"a":"3"}`)
	eventBuff := make([]byte, 0)
	eventBuff = input.processChunk(0, chunk, eventBuff)

	wg.Wait()
	p.Stop()

	assert.Equal(t, 2, len(outEvents), "wrong out events count")
	assert.Equal(t, `{"a":"1"}`, outEvents[0], "wrong event")
	assert.Equal(t, `{"a":"2"}`, outEvents[1], "wrong event")
	assert.Equal(t, `{"a":"3"}`, string(eventBuff), "wrong event buffer")
}

func TestProcessChunksContinue(t *testing.T) {
	p, _, output := test.NewPipelineMock(nil, "passive")
	p.SetInput(getInputInfo())
	input := p.GetInput().(*Plugin)
	p.Start()

	wg := &sync.WaitGroup{}
	wg.Add(3)

	outEvents := make([]string, 0)
	output.SetOutFn(func(event *pipeline.Event) {
		outEvents = append(outEvents, event.Root.EncodeToString())
		wg.Done()
	})

	chunk := []byte(`"1"}
{"a":"2"}
{"a":"3"}
`)
	eventBuff := []byte(`{"a":`)
	eventBuff = input.processChunk(0, chunk, eventBuff)

	wg.Wait()
	p.Stop()

	assert.Equal(t, 3, len(outEvents), "wrong out events count")
	assert.Equal(t, `{"a":"1"}`, outEvents[0], "wrong event")
	assert.Equal(t, `{"a":"2"}`, outEvents[1], "wrong event")
	assert.Equal(t, `{"a":"3"}`, outEvents[2], "wrong event")
	assert.Equal(t, 0, len(eventBuff), "wrong event")
}

func TestProcessChunksContinueMany(t *testing.T) {
	p, _, output := test.NewPipelineMock(nil, "passive")
	p.SetInput(getInputInfo())
	input := p.GetInput().(*Plugin)
	p.Start()

	wg := &sync.WaitGroup{}
	wg.Add(1)

	outEvents := make([]string, 0)
	output.SetOutFn(func(event *pipeline.Event) {
		outEvents = append(outEvents, event.Root.EncodeToString())
		wg.Done()
	})

	eventBuff := []byte(``)

	eventBuff = input.processChunk(0, []byte(`{`), eventBuff)
	eventBuff = input.processChunk(0, []byte(`"a"`), eventBuff)
	eventBuff = input.processChunk(0, []byte(`:`), eventBuff)
	eventBuff = input.processChunk(0, []byte(`"1"}`+"\n"), eventBuff)

	wg.Wait()
	p.Stop()

	assert.Equal(t, 1, len(outEvents), "wrong events count")
	assert.Equal(t, `{"a":"1"}`, outEvents[0], "wrong event")
	assert.Equal(t, 0, len(eventBuff), "wrong event")
}

func BenchmarkHttpInputJson(b *testing.B) {
	const NumWorkers = 128
	const DocumentCount = 128 * 128 * 8

	json, err := ioutil.ReadFile("../../../testdata/json/light.json")
	if err != nil {
		panic(err)
	}

	client := &http.Client{
		Timeout: 1 * time.Second,
	}

	var worker = func(jobs <-chan struct{}) {
		for range jobs {
			body := bytes.NewReader(json)
			req, err := http.NewRequest(http.MethodPost, "http://localhost:9200", body)
			if err != nil {
				panic(err)
			}
			req.Header.Set("Content-Type", "application/json")
			resp, err := client.Do(req)
			if err != nil {
				panic(err)
			}
			_, _ = io.Copy(ioutil.Discard, resp.Body) // https://github.com/google/go-github/pull/317
			_ = resp.Body.Close()
			if resp.StatusCode != 200 {
				panic(resp.Status)
			}
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		p, _, output := test.NewPipelineMock(nil, "passive", "perf")

		p.SetInput(getInputInfoWithAddr())

		wg := &sync.WaitGroup{}
		wg.Add(DocumentCount * 2) // 2 rows in each file

		output.SetOutFn(func(event *pipeline.Event) {
			wg.Done()
		})

		jobs := make(chan struct{})
		for w := 1; w <= NumWorkers; w++ {
			go worker(jobs)
		}

		p.Start()
		time.Sleep(100 * time.Millisecond) // http listen start delay

		go func() {
			for j := 0; j < DocumentCount; j++ {
				jobs <- struct{}{}
			}
			close(jobs)
		}()

		wg.Wait()

		p.Stop()
	}
}

func TestPlugin_fetchTLSConfig(t *testing.T) {
	var (
		fakeCertPath = "/tmp/localhost.crt"
		fakeCert     = `
-----BEGIN CERTIFICATE-----
MIIDhDCCAmygAwIBAgIhANG4j9rHsCqX42dYqniv7bUWIakPRii44fVDlAFslA1N
MA0GCSqGSIb3DQEBBQUAMFYxCTAHBgNVBAYTADEJMAcGA1UECgwAMQkwBwYDVQQL
DAAxEDAOBgNVBAMMB296b24ucnUxDzANBgkqhkiG9w0BCQEWADEQMA4GA1UEAwwH
b3pvbi5ydTAeFw0yMjA2MDYxNjAyMDlaFw0zMjA2MDYxNjAyMDlaMEQxCTAHBgNV
BAYTADEJMAcGA1UECgwAMQkwBwYDVQQLDAAxEDAOBgNVBAMMB296b24ucnUxDzAN
BgkqhkiG9w0BCQEWADCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAMGr
dnRoQe4Dn+8GDx2jviswx0KFo7EGz9T+RtFQblTpgGcSWSx0irRk5ce/kDveDqYh
4ZgNBn4XGnDcogWJAGcq4XymCQq5jBHJ0q7n2GxZ+Tx5vlmyaDNG5zv1lLjgSl0P
22ClM8NM7hCnE3uZz90VVyQbZ9FFKFW2WX+3uJHJnwMtW+w/n8DnQOBlIAjyCRko
sRci9/dgaWFmx+Yscs1UPtG7pgEOO+6hiyFNgZEVGbgZv82jR4g8zr6SxcIT8ZLT
A4oOj+7GFSf8q6yNgp8bDNxbBS3rS2Vv6IrIjXsVfbilWGM3WkAVaOVD6OFJ65wc
AiggDm9oTFz0H2OOAZ0CAwEAAaNPME0wHQYDVR0OBBYEFHP9uHY/St1je+k0cZ/U
0ZO2KWJ8MB8GA1UdIwQYMBaAFHP9uHY/St1je+k0cZ/U0ZO2KWJ8MAsGA1UdEQQE
MAKCADANBgkqhkiG9w0BAQUFAAOCAQEANr3j2VVGCa87fHgJzs3k7IBq0VnXx1C1
tuWsJ96z0MPHcOD2g8OU2ZANBSGn/PhS71fqvRefr4wUKDLt947/Ycriai84lMv8
tWO5KLrHDGG8OuMy5orHVt2okcPFVsVmzJBxIv6fRtrzr152FvvRZegrsgnYloZL
rJdkHHl+9kHS02Nmf3+F6ANAg/jgnr0huJEt1xwB0YtKHTj346ksBP3OOyjolze9
J6xWsrb/fzFceDYNmIW5oyDm9j6ZxmgRa0turlo/pCp4gEojlyTGhg0HzxoPJmky
BQQbQaVBlv7XzP9xayJxOOtKiSXjv6OMFP1XFHZPzvx5/3VbVPrKsQ==
-----END CERTIFICATE-----`
		fakePrivateKey = `
-----BEGIN RSA PRIVATE KEY-----
MIIEowIBAAKCAQEAwat2dGhB7gOf7wYPHaO+KzDHQoWjsQbP1P5G0VBuVOmAZxJZ
LHSKtGTlx7+QO94OpiHhmA0GfhcacNyiBYkAZyrhfKYJCrmMEcnSrufYbFn5PHm+
WbJoM0bnO/WUuOBKXQ/bYKUzw0zuEKcTe5nP3RVXJBtn0UUoVbZZf7e4kcmfAy1b
7D+fwOdA4GUgCPIJGSixFyL392BpYWbH5ixyzVQ+0bumAQ477qGLIU2BkRUZuBm/
zaNHiDzOvpLFwhPxktMDig6P7sYVJ/yrrI2CnxsM3FsFLetLZW/oisiNexV9uKVY
YzdaQBVo5UPo4UnrnBwCKCAOb2hMXPQfY44BnQIDAQABAoIBAB62CnA2MjJEp+F2
9AGuvBLMRuTRHV6OpnlvoxpBJ2XWq1Js8fAfQPmPQHLW+U/NDESi/cunUR7AmiRI
kHbRPVuezKgAClIFj2UovHyY7lRsRh4lbh56MJTlCvkSnfVdN92fDJo8SEORmPTS
EcBZaUF1R17F68YfMeC5rGKY5y9S432LrsbMapfZhBOBLlSec4Xb1VRAy0OfLzka
2Gt9B4P4sHzzO6w+ODL/cedfZt687iCfaCzW3QwxSxnelFEb2GOD0KGhuIgN1Nkh
N+DsAMmjLk5cYwlRKbwv7zuohcSi7MAu/ziKcaKPTGi4kB1cWhVNIuVmz0Ow6lVM
b1gGfAECgYEA4+E8IEbMmgIpfMmgmYhY1DNcSxp7dqJBwx9i/lbF+OMWo7HFf9zS
3AlyO3uPXZwgY7DtN4wCYZNFaZxZ+ZiUbMUd/tFKJiFSgx/H1RzKZnP9L60s00t5
Zx/bIkKv4SnOkBEmYr0x3y6cIvdCRJpK3gDRNdTsiQ2wROp6EhmuV50CgYEA2ZGG
thkfL6oFqN5pu070qoBAdxSIerVQPWMugChP03G0omNggwvJQ1DBFIhJSYGzyy/E
vtZczX6ra1AF07H5Xd/R3Dnl2UkQYAo/0wX0dRczl4InB3YUKUkGjpiagQIs3obg
I8LGzB+gigcPG5cBbn9Q2mCVlN0W7X2qeiffMgECgYA9eNHuZwrkdLbaWc2//P7x
Z1V7UQ6DJywURdA/igrno+LEj70WS+x5vPaawy8ecnJuUhj7xgISblboMWw4H4fS
e64mwjB9brnCc31FqPmUf6+J/+46pX5/aiGD8XdehxeY13eCZUUhzoICEK9IsFKH
5rKJEgIoyo+FV7dMUK8uJQKBgQCL/eh5GTcI21Si8OGFhWtGnhlcxdh/ZFUJ+tx7
1/RtMDezWVBLYfURnE9wX7UQWCWQ8t6ckJ/MFdpExYvKSDUVIyQMmTB9HFcuBMpG
hQljohcvQK7OTTrxyawvap/XrMekM5LbT8PMqfkJdztQXFyudbtXmFgHHi9Xhsl/
qlIiAQKBgA9DhReh6kSqhP0ldvw9f5z2PrVJqGOaCgkRQ/dUuDhbvlgyXI0gS/Rj
cG6ys3uVh5sYM7eVdZDKrKg45cQddUkbZm07cymaJ0WSSVBvWcUPPZ3aBlMWY4Vj
rO3s7sbqs3cMwC5g9LjPsfieoZt9AppSrJeY+l74yNa/afqTscun
-----END RSA PRIVATE KEY-----`
	)

	fakeReadFile := func(name string) ([]byte, error) {
		if name == fakeCertPath {
			return []byte(fakeCert), nil
		}
		panic("invalid file name")
	}

	input := &Plugin{
		config: &Config{
			CACert:     fakeCertPath,   // path to the PEM encoded file
			PrivateKey: fakePrivateKey, // PEM encoded file
		},
	}

	tlsConfig, err := input.fetchTLSConfig(fakeReadFile)

	require.NoError(t, err)
	require.NotNil(t, tlsConfig)
	require.Len(t, tlsConfig.Certificates, 1)
}
