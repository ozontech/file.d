package logger

import (
	"os"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var Instance *zap.SugaredLogger
var Level zap.AtomicLevel

const defaultLevel = zap.InfoLevel

func init() {
	var level zapcore.Level
	switch strings.ToLower(os.Getenv("LOG_LEVEL")) {
	case "debug":
		level = zap.DebugLevel
	case "info":
		level = zap.InfoLevel
	case "warn":
		level = zap.WarnLevel
	case "error":
		level = zap.ErrorLevel
	case "fatal":
		level = zap.FatalLevel
	default:
		level = defaultLevel
	}

	Level = zap.NewAtomicLevelAt(level)

	Instance = zap.New(
		zapcore.NewCore(
			zapcore.NewConsoleEncoder(zapcore.EncoderConfig{
				// TimeKey:        "ts",
				LevelKey:       "level",
				NameKey:        "Instance",
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
			Level,
		),
	).Sugar().Named("fd")

	Instance.Infof("Logger initialized with level: %s", level)
}

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
