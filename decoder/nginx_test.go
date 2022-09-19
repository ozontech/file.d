package decoder

import (
	"bufio"
	"strings"
	"testing"

	insaneJSON "github.com/vitkovskii/insane-json"
)

const (
	nginxValid = `
2022/08/17 10:49:27 [error] 2725122#2725122: *792412315 lua udp socket read timed out, context: ngx.timer
2022/08/17 10:49:39 [error] 2725122#2725122: *792412315 [lua] dns.lua:152: dns_lookup(): failed to query the DNS server for service-meta-validator.monitoring.infra-ts.s.tldn:
2022/08/18 09:29:37 [error] 844935#844935: *44934601 upstream timed out (110: Operation timed out) while connecting to upstream, client: 10.125.172.251, server: mpm-youtube-downloader-38.name.tldn, request: "POST /download HTTP/1.1", upstream: "http://10.117.246.15:84/download", host: "mpm-youtube-downloader-38.name.tldn:84"
2022/08/18 09:38:24 [error] 1513920#1513920: *426522493 upstream timed out (110: Operation timed out) while reading response header from upstream, client: 10.133.184.214, server: eparser.name.tldn, request: "POST /ozon.opendatacollecting.eparser.EParser/GetImageBytes HTTP/2.0", upstream: "grpc://10.117.160.158:82", host: "eparser.name.tldn:82"
`

	// careful - some lines below contain spaces, they are not duplicate, do not remove them
	nginxInvalid = `
 
  
invalid
#invalid
2022/08/18 09:38:25
2022/08/18 09:38:25 message
2022/08/18 09:38:25 ] message
2022/08/18 09:38:25 [ message
2022/08/18 09:38:25 [] message
2022/08/18 09:38:25 [error]
`
)

func TestDecodeNginxPass(t *testing.T) {
	reader := strings.NewReader(nginxValid)
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		root := insaneJSON.Spawn()
		err := DecodeNginxError(root, line)
		if err != nil {
			t.Fatalf("error parsing line=%s, %s", line, err)
		}
	}
}

func TestDecodeNginxFields(t *testing.T) {
	root := insaneJSON.Spawn()
	err := DecodeNginxError(root, []byte("2022/08/18 09:36:34 [error] message"))
	if err != nil {
		t.Fatalf("error parsing: %s", err)
	}

	d := root.Dig("time")
	if d == nil || d.AsString() != "2022/08/18 09:36:34" {
		t.Fatal("incorrect time")
	}

	l := root.Dig("level")
	if l == nil || l.AsString() != "error" {
		t.Fatal("incorrect level")
	}

	m := root.Dig("message")
	if m == nil || m.AsString() != "message" {
		t.Fatal("incorrect message")
	}
}

func TestDecodeNginxFail(t *testing.T) {
	reader := strings.NewReader(nginxInvalid)
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		line := scanner.Bytes()
		root := insaneJSON.Spawn()
		err := DecodeNginxError(root, line)
		if err == nil {
			t.Fatalf("expecting error on line=%s", line)
		}
	}
}
