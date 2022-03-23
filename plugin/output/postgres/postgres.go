package postgres

import (
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx"
	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/stats"
	"go.uber.org/zap"
	"golang.org/x/net/context"
)

type PgxIface interface {
	Exec(sql string, arguments ...interface{}) (commandTag pgx.CommandTag, err error)
	Close() error
}

const (
	outPluginType = "postgres"
	subsystemName = "output_postgres"

	retryCount = 3

	// metrics
	discardedEventCounter = "event_discarded"
)

const (
	// minimum required types for now.
	unknownType pgType = iota
	pgString
	pgInt
	pgTimestamp
)

const (
	colTypeInt       = "int"
	colTypeString    = "string"
	colTypeTimestamp = "timestamp"
)

type Plugin struct {
	controller pipeline.OutputPluginController
	logger     *zap.SugaredLogger
	config     *Config
	batcher    *pipeline.Batcher
	ctx        context.Context
	cancelFunc context.CancelFunc

	queryBuilder PgQueryBuilder
	db           PgxIface
}

type column struct {
	Name    string
	ColType pgType
	Unique  bool
}

type pgType int

type ConfigColumn struct {
	Name       string `json:"name" required:"true"`
	ColumnType string `json:"type" required:"true" options:"int|string|bool|timestamp"`
	Unique     bool   `json:"unique" default:"false"`
}

type Config struct {
	//> In strict mode file.d will crash on events without required columns.
	// Otherwise events will be discarded.
	Strict bool `json:"strict" default:"false"` //*

	//> Db host.
	Host string `json:"host" required:"true"` //*

	//> Db port.
	Port uint16 `json:"port" required:"true"` //*

	//> Dbname in pg.
	DBname string `json:"dbname" required:"true"` //*

	//> Pg user name.
	Username string `json:"user" required:"true"` //*

	//> Pg user pwd.
	Pwd string `json:"password" required:"true"`

	//> Pg target table.
	Table string `json:"table" required:"true"` //*

	//> Array of DB columns. Each column have:
	// name, type (int, string, bool, timestamp - which int that will be converted to timestamptz of rfc3339)
	// and nullable options.
	Columns []ConfigColumn `json:"columns" required:"true" slice:"true"` //*

	//> Interval of creation new file
	RetentionInterval  cfg.Duration `json:"retention_interval" default:"1h" parse:"duration"` //*
	RetentionInterval_ time.Duration

	//> How much workers will be instantiated to send batches.
	WorkersCount  cfg.Expression `json:"workers_count" default:"gomaxprocs*4" parse:"expression"` //*
	WorkersCount_ int

	//> Maximum quantity of events to pack into one batch.
	BatchSize cfg.Expression `json:"batch_size" default:"capacity/2" parse:"expression"` //*
	//BatchSize  cfg.Expression `json:"batch_size" default:"capacity/2" parse:"expression"` //*
	BatchSize_ int

	//> After this timeout batch will be sent even if batch isn't completed.
	BatchFlushTimeout  cfg.Duration `json:"batch_flush_timeout" default:"20s" parse:"duration"` //*
	BatchFlushTimeout_ time.Duration
}

func init() {
	fd.DefaultPluginRegistry.RegisterOutput(&pipeline.PluginStaticInfo{
		Type:    outPluginType,
		Factory: Factory,
	})
}

func Factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) registerPluginMetrics() {
	stats.RegisterCounter(&stats.MetricDesc{
		Name:      discardedEventCounter,
		Subsystem: subsystemName,
		Help:      "Total pgsql discarded messages",
	})
}

func (p *Plugin) Start(config pipeline.AnyConfig, params *pipeline.OutputPluginParams) {
	p.controller = params.Controller
	p.logger = params.Logger
	p.config = config.(*Config)
	if len(p.config.Columns) == 0 {
		p.logger.Fatal("Can't start plugin, no fields in config")
	}
	p.registerPluginMetrics()

	queryBuilder, err := NewQueryBuilder(p.config.Columns, p.config.Table)
	if err != nil {
		p.logger.Fatal(err)
	}
	p.queryBuilder = queryBuilder

	db, err := pgx.Connect(pgx.ConnConfig{
		Host:     p.config.Host,
		Port:     p.config.Port,
		Database: p.config.DBname,
		Password: p.config.Pwd,
		User:     p.config.Username,
	})
	if err != nil {
		p.logger.Fatalf("can't connect to pgsql: %v", err)
	}
	p.db = db

	p.batcher = pipeline.NewBatcher(
		params.PipelineName,
		outPluginType,
		p.out,
		nil,
		p.controller,
		p.config.WorkersCount_,
		p.config.BatchSize_,
		p.config.BatchFlushTimeout_,
		0,
	)

	ctx, cancel := context.WithCancel(context.Background())
	p.ctx = ctx
	p.cancelFunc = cancel

	p.batcher.Start(ctx)
}

func (p *Plugin) Stop() {
	p.cancelFunc()
	p.batcher.Stop()
}

func (p *Plugin) Out(event *pipeline.Event) {
	p.batcher.Add(event)
}

func (p *Plugin) out(workerData *pipeline.WorkerData, batch *pipeline.Batch) {
	// Deduplicate events, pg can't do upsert with duplication.
	uniqueEventsMap := make(map[uint64]struct{}, len(batch.Events))
	uniqueEvents := make([]*pipeline.Event, 0, len(batch.Events))
	for _, event := range batch.Events {
		// s3url, domain

		// go throught unique cols: service + host
		// secondary batch: 1, 3, 2.

		_, ok := uniqueEventsMap[event.SeqID]
		if ok {
			logger.Infof("event duplicate: %v", event)
			continue
		}
		uniqueEventsMap[event.SeqID] = struct{}{}
		uniqueEvents = append(uniqueEvents, event)
	}

	builder := p.queryBuilder.GetInsertBuilder()
	for _, event := range uniqueEvents {
		lVals := make([]interface{}, 0, len(p.queryBuilder.GetPgFields()))
	LOOP_THROUGH_EVENT:
		for _, field := range p.queryBuilder.GetPgFields() {
			fNode, err := event.Root.DigStrict(field.Name)
			if err != nil {
				// In strict mode file.d fails, otherwise truncate bad event.
				if p.config.Strict {
					p.logger.Fatalf("Required field '%s' doesn't appear in event: %v", field.Name, string(event.Buf))
				}
				// todo log err.
				stats.GetCounter(subsystemName, discardedEventCounter).Inc()
				// stop loop for event
				break LOOP_THROUGH_EVENT
			}
			switch field.ColType {
			case pgString:
				// todo strict mode skip event.
				lVal, err := fNode.AsString()
				if err != nil {
					p.logger.Fatalf("can't get %s as string, err: %w", field.Name, err.Error())
				}
				lVals = append(lVals, lVal)
			case pgInt:
				lVal, err := fNode.AsInt()
				if err != nil {
					p.logger.Fatalf("can't get %s as int, err: %w", field.Name, err.Error())
				}
				lVals = append(lVals, lVal)
			case pgTimestamp:
				tint, err := fNode.AsInt()
				if err != nil {
					p.logger.Fatalf("can't get %s as int, err: %s", field.Name, err.Error())
				}
				lVal := time.Unix(int64(tint), 0).Format(time.RFC3339)
				lVals = append(lVals, lVal)
			case unknownType:
				fallthrough
			default:
				p.logger.Fatal("undefined col type: %d, col name: %s", field.ColType, field.Name)
			}
		}
		// If LOOP_THROUGH_EVENT was breaked values mustn't be added.
		if len(lVals) == len(p.queryBuilder.GetPgFields()) {
			builder = builder.Values(lVals...)
		}
	}

	builder = builder.Suffix(p.queryBuilder.GetPostfix()).PlaceholderFormat(sq.Dollar)

	query, args, err := builder.ToSql()
	if err != nil {
		p.logger.Fatalf("Invalid SQL. query: %s, args: %v, err: %v", query, args, err)
		return
	}

	for i := retryCount; i > 0; i-- {
		_, err = p.db.Exec(query, args...)
		if err != nil {
			time.Sleep(time.Millisecond * 50)
			continue
		}

		break
	}
	if err != nil {
		p.db.Close()
		p.logger.Fatalf("Invalid SQL. query: %s, args: %v, err: %v", query, args, err)
	}
}
