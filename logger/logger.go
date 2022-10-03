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

func Debug(args ...any) {
	Instance.Debug(args...)
}

func Info(args ...any) {
	Instance.Info(args...)
}

func Warn(args ...any) {
	Instance.Warn(args...)
}

func Error(args ...any) {
	Instance.Error(args...)
}

func Panic(args ...any) {
	Instance.Panic(args...)
}

func Fatal(args ...any) {
	Instance.Fatal(args...)
}

func Debugf(template string, args ...any) {
	Instance.Debugf(template, args...)
}

func Infof(template string, args ...any) {
	Instance.Infof(template, args...)
}

func Warnf(template string, args ...any) {
	Instance.Warnf(template, args...)
}

func Errorf(template string, args ...any) {
	Instance.Errorf(template, args...)
}

func Panicf(template string, args ...any) {
	Instance.Panicf(template, args...)
}

func Fatalf(template string, args ...any) {
	Instance.Fatalf(template, args...)
}
