package postgres

import (
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx"
	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/pipeline"
	"go.uber.org/zap"
	"golang.org/x/net/context"
)

const retryCount = 3

type Plugin struct {
	controller pipeline.OutputPluginController
	logger     *zap.SugaredLogger
	config     *Config
	batcher    *pipeline.Batcher
	ctx        context.Context
	cancelFunc context.CancelFunc

	queryBuilder pgQueryBuilder
	db           *pgx.Conn
}

type column struct {
	Name    string
	ColType pgType
	Unique  bool
}

type pgType int

const (
	// minimum required types for now.
	unknownType pgType = iota
	pgString
	pgInt
	pgBool
	pgTimestamp
)

const (
	outPluginType = "postgres"
)

const (
	colTypeInt       = "int"
	colTypeString    = "string"
	colTypeBool      = "bool"
	colTypeTimestamp = "timestamp"
)

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

func (p *Plugin) Start(config pipeline.AnyConfig, params *pipeline.OutputPluginParams) {
	p.controller = params.Controller
	p.logger = params.Logger
	p.config = config.(*Config)
	if len(p.config.Columns) == 0 {
		p.logger.Fatal("Can't start plugin, no fields in config")
	}

	queryBuilder, err := newQueryBuilder(p.config.Columns, p.config.Table)
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

	p.batcher.Start()
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
		_, ok := uniqueEventsMap[event.SeqID]
		if ok {
			continue
		}
		uniqueEventsMap[event.SeqID] = struct{}{}
		uniqueEvents = append(uniqueEvents, event)
	}

	builder := p.queryBuilder.GetQueryBuilder()
	for _, event := range uniqueEvents {
		lVals := make([]interface{}, 0, len(p.queryBuilder.GetPgFields()))
		for _, field := range p.queryBuilder.GetPgFields() {
			fNode, err := event.Root.DigStrict(field.Name)
			if err != nil {
				// In strict mode file.d must fail, otherwise truncate bad event.
				if p.config.Strict {
					p.logger.Fatalf("Required field '%s' doesn't appear in event: %v", field.Name, string(event.Buf))
				}
				continue
			}
			switch field.ColType {
			case pgString:
				// TODO возможно не падать fatal, а пропускать сообщение в не strict mode?
				lVal, err := fNode.AsString()
				if err != nil {
					p.logger.Fatalf("can't get %s as string, err: %w", field.Name, err)
				}
				lVals = append(lVals, lVal)
			case pgInt:
				lVal, err := fNode.AsInt()
				if err != nil {
					p.logger.Fatalf("can't get %s as int, err: %w", field.Name, err)
				}
				lVals = append(lVals, lVal)
			case pgBool:
				lVal, err := fNode.AsBool()
				if err != nil {
					p.logger.Fatalf("can't get %s as bool, err: %w", field.Name, err)
				}
				lVals = append(lVals, lVal)
			case pgTimestamp:
				tint, err := fNode.AsInt()
				if err != nil {
					p.logger.Fatalf("can't get %s as int, err: %s", field.Name, err)
				}
				lVal := time.Unix(int64(tint), 0).Format(time.RFC3339)
				lVals = append(lVals, lVal)
			case unknownType:
				fallthrough
			default:
				p.logger.Fatal("undefined col type: %d, col name: %s", field.ColType, field.Name)
			}
		}
		builder = builder.Values(lVals...)
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
		p.logger.Fatalf("Invalid SQL. query: %s, args: %v, err: %v", query, args, err)
	}
}
