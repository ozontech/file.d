package global

import (
	"fmt"
)

const (
	ErrCodeConfigRead    = 100
	ErrCodeConfigYAML    = 101
	ErrCodeConfigConvert = 102
	ErrCodeConfigProcess = 103

	ErrCodeHttpLiveReadyEndpoint = 200

	ErrCodePluginRegistryAlreadyRegistered = 300
)

type Err struct {
	Message string
	Code    int
	Cause   error
}

func (e *Err) Error() string {
	suffix := ""
	if e.Cause != nil {
		suffix = ": " + e.Cause.Error()
	}
	return fmt.Sprintf("%s (code=%d) %s", e.Message, e.Code, suffix)
}

func New(code int, format string, a ...interface{}) error {
	return &Err{Message: fmt.Sprintf(format, a...), Code: code}
}

func Wrap(err error, code int, format string, a ...interface{}) error {
	if err == nil {
		panic("Why wrap nil?")
	}

	return &Err{
		Cause:   err,
		Message: fmt.Sprintf(format, a...),
		Code:    code,
	}
}
