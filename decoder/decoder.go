package decoder

import (
	"fmt"

	insaneJSON "github.com/ozontech/insane-json"
)

type Params map[string]any

type Type int

const (
	NO Type = iota
	AUTO
	JSON
	RAW
	CRI
	POSTGRES
	NGINX_ERROR
	PROTOBUF
	SYSLOG_RFC3164
	SYSLOG_RFC5424
	CSV
)

type Decoder interface {
	Type() Type
	DecodeToJson(root *insaneJSON.Root, data []byte) error
	Decode(data []byte, args ...any) (any, error)
}

func TypeFromString(s string) Type {
	switch s {
	case "json":
		return JSON
	case "raw":
		return RAW
	case "cri":
		return CRI
	case "postgres":
		return POSTGRES
	case "nginx_error":
		return NGINX_ERROR
	case "protobuf":
		return PROTOBUF
	case "syslog_rfc3164":
		return SYSLOG_RFC3164
	case "syslog_rfc5424":
		return SYSLOG_RFC5424
	case "csv":
		return CSV
	case "auto":
		return AUTO
	default:
		return NO
	}
}

func New(t Type, params Params) (Decoder, error) {
	switch t {
	case JSON:
		return NewJsonDecoder(params)
	case NGINX_ERROR:
		return NewNginxErrorDecoder(params)
	case PROTOBUF:
		return NewProtobufDecoder(params)
	case SYSLOG_RFC3164:
		return NewSyslogRFC3164Decoder(params)
	case SYSLOG_RFC5424:
		return NewSyslogRFC5424Decoder(params)
	case CSV:
		return NewCSVDecoder(params)
	case RAW, CRI, POSTGRES, AUTO:
		return nil, nil
	default:
		return nil, fmt.Errorf("unknown decoder type: %v", t)
	}
}
