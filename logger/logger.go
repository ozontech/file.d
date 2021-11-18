package logger

import (
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var Instance = zap.New(
	zapcore.NewCore(
		zapcore.NewConsoleEncoder(zapcore.EncoderConfig{
			// TimeKey:        "ts",
			LevelKey:       "level",
			NameKey:        "logger",
			CallerKey:      "caller",
			MessageKey:     "message",
			StacktraceKey:  "stacktrace",
			LineEnding:     zapcore.DefaultLineEnding,
			EncodeLevel:    zapcore.LowercaseLevelEncoder,
			EncodeTime:     zapcore.ISO8601TimeEncoder,
			EncodeDuration: zapcore.SecondsDurationEncoder,
			EncodeCaller:   zapcore.ShortCallerEncoder,
		}),
		zapcore.AddSync(os.Stdout),
		zap.NewAtomicLevelAt(zap.InfoLevel),
	),
).Sugar().Named("fd")

func Debug(args ...interface{}) {
	Instance.Debug(args...)
}

func Info(args ...interface{}) {
	Instance.Info(args...)
}

func Warn(args ...interface{}) {
	Instance.Warn(args...)
}

func Error(args ...interface{}) {
	Instance.Error(args...)
}

func Panic(args ...interface{}) {
	Instance.Panic(args...)
}

func Fatal(args ...interface{}) {
	Instance.Fatal(args...)
}

func Debugf(template string, args ...interface{}) {
	Instance.Debugf(template, args...)
}

func Infof(template string, args ...interface{}) {
	Instance.Infof(template, args...)
}

func Warnf(template string, args ...interface{}) {
	Instance.Warnf(template, args...)
}

func Errorf(template string, args ...interface{}) {
	Instance.Errorf(template, args...)
}

func Panicf(template string, args ...interface{}) {
	Instance.Panicf(template, args...)
}

func Fatalf(template string, args ...interface{}) {
	Instance.Fatalf(template, args...)
}
