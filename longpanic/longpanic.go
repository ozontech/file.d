// package longpanic defines `Go` func that creates goroutine with defer
// that waits for somebody to call `RecoverFromPanic` or panics after timeout.
package longpanic

import (
	"time"

	"go.uber.org/atomic"

	"github.com/ozontech/file.d/logger"
)

// instance is a singleton with timeout that every `go func` call should use.
var instance *LongPanic = NewLongPanic(time.Minute)

// SetTimeout set the timeout after the program panics.
func SetTimeout(timeout time.Duration) {
	instance.timeout = timeout
}

// Go runs fn in a different goroutine with defer statement that:
// 1. Recovers from panic
// 2. Waits for somebody to call `RecoverFromPanic` or timeout
// 3. Panics if nobody calls `RecoverFromPanic`
func Go(fn func()) {
	instance.Go(fn)
}

// WithRecover runs fn with defer statement that:
// 1. Recovers from panic
// 2. Waits for somebody to call `RecoverFromPanic` or timeout
// 3. Panics if nobody calls `RecoverFromPanic`
func WithRecover(fn func()) {
	instance.WithRecover(fn)
}

// RecoverFromPanic is a signal to not wait for the panic and tries to continue the execution.
func RecoverFromPanic() {
	instance.RecoverFromPanic()
}

func SetOnPanicHandler(cb func(err error)) {
	instance.SetOnPanicHandler(cb)
}

// LongPanic is a struct that holds an atomic and a timeout after a defer fn will panic.
type LongPanic struct {
	shouldPanic *atomic.Bool
	timeout     time.Duration

	panicHandler func(err error)
}

// NewLongPanic creates LongPanic.
func NewLongPanic(timeout time.Duration) *LongPanic {
	return &LongPanic{
		shouldPanic: atomic.NewBool(false),
		timeout:     timeout,
	}
}

// Go runs fn in a different goroutine with defer statement that:
// 1. Recovers from panic
// 2. Waits for somebody to call `RecoverFromPanic` or timeout
// 3. Panics if nobody calls `RecoverFromPanic`
func (l *LongPanic) Go(fn func()) {
	go func() {
		defer l.recoverUntilTimeout()
		fn()
	}()
}

// WithRecover runs fn with defer statement that:
// 1. Recovers from panic
// 2. Waits for somebody to call `RecoverFromPanic` or timeout
// 3. Panics if nobody calls `RecoverFromPanic`
func (l *LongPanic) WithRecover(fn func()) {
	defer l.recoverUntilTimeout()
	fn()
}

// recover waits for somebody to reset the error plugin or panics after a timeout.
func (l *LongPanic) recoverUntilTimeout() {
	if err, ok := recover().(error); ok {
		if l.panicHandler != nil {
			l.panicHandler(err)
		}

		logger.Error(err.Error())
		logger.Error("wait for somebody to restart plugins via endpoint")

		l.shouldPanic.Store(true)
		t := time.Now()
		for {
			time.Sleep(10 * time.Millisecond)
			if !l.shouldPanic.Load() {
				logger.Error("panic recovered! Trying to continue execution...")

				return
			}
			if time.Since(t) > l.timeout {
				logger.Panic(err.Error())
			}
		}
	}
}

// RecoverFromPanic is a signal to not wait for the panic and tries to continue the execution.
func (l *LongPanic) RecoverFromPanic() {
	l.shouldPanic.Store(false)
}

// SetOnPanicHandler setups callback when panic happened.
func (l *LongPanic) SetOnPanicHandler(cb func(err error)) {
	l.panicHandler = cb
}
