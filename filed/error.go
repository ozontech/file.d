package filed

import (
	"fmt"
)

const (
	ErrCodeConfigReadFailed    = 100
	ErrCodeConfigWrongYAML     = 101
	ErrCodeConfigConvertFailed = 102
	ErrCodeConfigProcessFailed = 103

	ErrCodeHttpStartFailed = 200

	ErrCodePluginAlreadyRegistered = 300
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

func NewError(code int, format string, a ...interface{}) error {
	return &Err{Message: fmt.Sprintf(format, a...), Code: code}
}

func WrapError(err error, code int, format string, a ...interface{}) error {
	if err == nil {
		panic("Why wrap nil?")
	}

	return &Err{
		Cause:   err,
		Message: fmt.Sprintf(format, a...),
		Code:    code,
	}
}
