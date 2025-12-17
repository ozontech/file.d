package xoauth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"mime"
	"net"
	"net/http"
	"os"

	"golang.org/x/oauth2"
)

// error fields: https://datatracker.ietf.org/doc/html/rfc6749#section-5.2
type errorJSON struct {
	ErrorCode        string `json:"error"`
	ErrorDescription string `json:"error_description,omitempty"`
}

const (
	// issuer errors
	ecInvalidRequest       = "invalid_request"
	ecInvalidClient        = "invalid_client"
	ecInvalidGrant         = "invalid_grant"
	ecInvalidScope         = "invalid_scope"
	ecUnauthorizedClient   = "unauthorized_client"
	ecUnsupportedGrantType = "unsupported_grant_type"

	ecTimeout   = "timeout"
	ecNetwork   = "network_error"
	ecRateLimit = "rate_limit"

	ecUnknownServer = "unknown_server"
	ecUnknown       = "unknown"
)

type errorAuth struct {
	code    string
	message string
	cause   error
}

func (e *errorAuth) Error() string        { return e.message }
func (e *errorAuth) Code() string         { return e.code }
func (e *errorAuth) Unwrap() error        { return e.cause }
func (e *errorAuth) Is(target error) bool { return errors.Is(target, e.cause) }

func wrapErr(err error, code, prefix string) *errorAuth {
	return &errorAuth{
		code:    code,
		message: fmt.Sprintf("%s: %v", prefix, err),

		cause: err,
	}
}

func parseError(err error) *errorAuth {
	var retrieveErr *oauth2.RetrieveError
	if errors.As(err, &retrieveErr) {
		return parseRetrieveError(err, retrieveErr)
	}

	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) || errors.Is(err, os.ErrDeadlineExceeded) {
		return wrapErr(err, ecTimeout, "timeout")
	}

	var netError net.Error
	if errors.As(err, &netError) {
		return wrapErr(netError, ecNetwork, "network error")
	}

	return wrapErr(err, ecUnknown, "unknown error")
}

func parseRetrieveError(err error, retrieveErr *oauth2.RetrieveError) *errorAuth {
	content, _, _ := mime.ParseMediaType(retrieveErr.Response.Header.Get("Content-Type"))
	if content == "application/json" {
		var errJson errorJSON
		if err = json.Unmarshal(retrieveErr.Body, &errJson); err == nil {
			return wrapErr(retrieveErr, errJson.ErrorCode, errJson.ErrorDescription)
		}
	}

	if retrieveErr.Response.StatusCode == http.StatusTooManyRequests {
		return wrapErr(err, ecRateLimit, "rate limit")
	}

	return wrapErr(err, ecUnknownServer, err.Error())
}
