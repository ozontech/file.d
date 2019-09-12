package logger

import (
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var logger = zap.New(
	zapcore.NewCore(
		zapcore.NewConsoleEncoder(zapcore.EncoderConfig{
			//TimeKey:        "ts",
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
).Sugar()

func Debug(args ...interface{}) {
	logger.Debug(args...)
}

func Info(args ...interface{}) {
	logger.Info(args...)
}

func Warn(args ...interface{}) {
	logger.Warn(args...)
}

func Error(args ...interface{}) {
	logger.Error(args...)

}

func Panic(args ...interface{}) {
	logger.Panic(args...)

}

func Fatal(args ...interface{}) {
	logger.Fatal(args...)

}

func Debugf(template string, args ...interface{}) {
	logger.Debugf(template, args...)
}

func Infof(template string, args ...interface{}) {
	logger.Infof(template, args...)
}

func Warnf(template string, args ...interface{}) {
	logger.Warnf(template, args...)
}

func Errorf(template string, args ...interface{}) {
	logger.Errorf(template, args...)
}

func Panicf(template string, args ...interface{}) {
	logger.Panicf(template, args...)
}

func Fatalf(template string, args ...interface{}) {
	logger.Fatalf(template, args...)
}
