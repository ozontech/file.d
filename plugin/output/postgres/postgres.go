package postgres

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/cenkalti/backoff/v4"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/stats"
	insaneJSON "github.com/vitkovskii/insane-json"
	"go.uber.org/zap"
)

/*{ introduction
It sends the event batches to postgres db using pgx.
}*/

var (
	ErrEventDoesntHaveField   = errors.New("event doesn't have field")
	ErrEventFieldHasWrongType = errors.New("event field has wrong type")
)

type PgxIface interface {
	Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error)
	Close()
}

const (
	outPluginType = "postgres"
	subsystemName = "output_postgres"

	// metrics
	discardedEventCounter  = "event_discarded"
	duplicatedEventCounter = "event_duplicated"
)

type pgType int

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

	backoff      backoff.BackOff
	queryBuilder PgQueryBuilder
	pool         PgxIface
}

type ConfigColumn struct {
	Name       string `json:"name" required:"true"`
	ColumnType string `json:"type" required:"true" options:"int|string|bool|timestamp"`
	Unique     bool   `json:"unique" default:"false"`
}

//! config-params
//^ config-params
type Config struct {
	//> @3@4@5@6
	//>
	//> In strict mode file.d will crash on events without required columns.
	// Otherwise events will be discarded.
	Strict bool `json:"strict" default:"false"` //*

	//> @3@4@5@6
	//>
	//> DB host.
	Host string `json:"host" required:"true"` //*

	//> @3@4@5@6
	//>
	//> Db port.
	Port uint16 `json:"port" required:"true"` //*

	//> @3@4@5@6
	//>
	//> Dbname in pg.
	DBName string `json:"dbname" required:"true"` //*

	//> @3@4@5@6
	//>
	//> Pg user name.
	Username string `json:"user" required:"true"` //*

	//> @3@4@5@6
	//>
	//> Pg user pass.
	Pass string `json:"password" required:"true"` //*

	//> @3@4@5@6
	//>
	//> Pg target table.
	Table string `json:"table" required:"true"` //*

	//> @3@4@5@6
	//>
	//> Array of DB columns. Each column have:
	//> name, type (int, string, timestamp - which int that will be converted to timestamptz of rfc3339)
	//> and nullable options.
	Columns []ConfigColumn `json:"columns" required:"true" slice:"true"` //*

	//> @3@4@5@6
	//>
	//> Retries of insertion.
	Retry int `json:"retry" default:"3"` //*

	//> @3@4@5@6
	//>
	//> Retention milliseconds for retry to DB.
	Retention  cfg.Duration `json:"retention" default:"50ms" parse:"duration"` //*
	Retention_ time.Duration

	//> @3@4@5@6
	//>
	//> Timeout for DB requests in milliseconds.
	DBRequestTimeout  cfg.Duration `json:"db_request_timeout" default:"3000ms" parse:"duration"` //*
	DBRequestTimeout_ time.Duration

	//> @3@4@5@6
	//>
	//> Timeout for DB health check.
	DBHealthCheckPeriod  cfg.Duration `json:"db_health_check_period" default:"60s" parse:"duration"` //*
	DBHealthCheckPeriod_ time.Duration

	//> @3@4@5@6
	//>
	//> How much workers will be instantiated to send batches.
	WorkersCount  cfg.Expression `json:"workers_count" default:"gomaxprocs*4" parse:"expression"` //*
	WorkersCount_ int

	//> @3@4@5@6
	//>
	//> Maximum quantity of events to pack into one batch.
	BatchSize cfg.Expression `json:"batch_size" default:"capacity/4"  parse:"expression"` //*
	//BatchSize  cfg.Expression `json:"batch_size" default:"capacity/4" parse:"expression"` //*
	BatchSize_ int

	//> @3@4@5@6
	//>
	//> After this timeout batch will be sent even if batch isn't completed.
	BatchFlushTimeout  cfg.Duration `json:"batch_flush_timeout" default:"200ms" parse:"duration"` //*
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
	stats.RegisterCounter(&stats.MetricDesc{
		Name:      duplicatedEventCounter,
		Subsystem: subsystemName,
		Help:      "Total pgsql duplicated messages",
	})
}

func (p *Plugin) Start(config pipeline.AnyConfig, params *pipeline.OutputPluginParams) {
	p.controller = params.Controller
	p.logger = params.Logger
	p.config = config.(*Config)
	p.ctx = context.Background()
	if len(p.config.Columns) == 0 {
		p.logger.Fatal("Can't start plugin, no fields in config")
	}

	if p.config.Retry < 1 {
		p.logger.Fatal("'retry' can't be <1")
	}
	if p.config.Retention_ < 1 {
		p.logger.Fatal("'renetion' can't be <1")
	}
	if p.config.DBRequestTimeout_ < 1 {
		p.logger.Fatal("'db_request_timeout' can't be <1")
	}
	if p.config.DBHealthCheckPeriod_ < 1 {
		p.logger.Fatal("'db_health_check_period' can't be <1")
	}

	p.registerPluginMetrics()
	queryBuilder, err := NewQueryBuilder(p.config.Columns, p.config.Table)
	if err != nil {
		p.logger.Fatal(err)
	}
	p.queryBuilder = queryBuilder

	cfgString := fmt.Sprintf("user=%s password=%s host=%s port=%d dbname=%s pool_max_conns=1",
		p.config.Username,
		p.config.Pass,
		p.config.Host,
		p.config.Port,
		p.config.DBName)

	cfg, err := pgxpool.ParseConfig(cfgString)
	if err != nil {
		p.logger.Fatalf("can't create pgsql config: %v", err)
	}
	cfg.LazyConnect = false

	cfg.HealthCheckPeriod = p.config.DBHealthCheckPeriod_

	pool, err := pgxpool.ConnectConfig(p.ctx, cfg)
	if err != nil {
		p.logger.Fatalf("can't create pgsql pool: %v", err)
	}
	p.pool = pool

	stdBackoff := backoff.NewExponentialBackOff()
	stdBackoff.Multiplier = 1.2
	stdBackoff.RandomizationFactor = 0.25
	stdBackoff.InitialInterval = p.config.Retention_
	stdBackoff.MaxInterval = p.config.Retention_ * 2

	ctxBackoff := backoff.WithContext(stdBackoff, p.ctx)
	expBackoff := backoff.WithMaxRetries(ctxBackoff, uint64(p.config.Retry))

	p.backoff = expBackoff

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
	p.pool.Close()
}

func (p *Plugin) Out(event *pipeline.Event) {
	p.batcher.Add(event)
}

func (p *Plugin) out(_ *pipeline.WorkerData, batch *pipeline.Batch) {
	// _ *pipeline.WorkerData - doesn't required in this plugin, we can't parse
	// events for uniques through bytes.
	builder := p.queryBuilder.GetInsertBuilder()
	pgFields := p.queryBuilder.GetPgFields()
	uniqFields := p.queryBuilder.GetUniqueFields()

	// Deduplicate events, pg can't do upsert with duplication.
	uniqueEventsMap := make(map[string]struct{}, len(batch.Events))

	for _, event := range batch.Events {
		fieldValues, uniqueID, err := p.processEvent(event, pgFields, uniqFields)
		if err != nil {
			if errors.Is(err, ErrEventDoesntHaveField) {
				stats.GetCounter(subsystemName, discardedEventCounter).Inc()
				if p.config.Strict {
					p.logger.Fatal(err)
				}
				p.logger.Error(err)
			} else if errors.Is(err, ErrEventFieldHasWrongType) {
				stats.GetCounter(subsystemName, discardedEventCounter).Inc()
				if p.config.Strict {
					p.logger.Fatal(err)
				}
				p.logger.Error(err)
			} else if err != nil { // protection from foolproof.
				p.logger.Fatalf("undefined error: %w", err)
			}

			continue
		}

		// passes here only if event valid.
		if _, ok := uniqueEventsMap[uniqueID]; ok {
			stats.GetCounter(subsystemName, duplicatedEventCounter).Inc()
			p.logger.Infof("event duplicated. Fields: %v, values: %v", pgFields, fieldValues)
		} else {
			uniqueEventsMap[uniqueID] = struct{}{}
			builder = builder.Values(fieldValues...)
		}
	}

	builder = builder.Suffix(p.queryBuilder.GetPostfix()).PlaceholderFormat(sq.Dollar)

	// no valid events passed.
	if len(uniqueEventsMap) == 0 {
		return
	}

	query, args, err := builder.ToSql()
	if err != nil {
		p.logger.Fatalf("Invalid SQL. query: %s, args: %v, err: %v", query, args, err)
	}

	// Insert into pg with retry.
	err = backoff.Retry(func() error {
		ctx, cancel := context.WithTimeout(p.ctx, p.config.DBRequestTimeout_)
		defer cancel()

		p.logger.Info(query, args)
		pgCommResult, err := p.pool.Exec(ctx, query, args...)
		if err != nil {
			p.logger.Infof("pgCommResult: %v, err: %s", pgCommResult, err.Error())

			return err
		}
		return nil
	}, p.backoff)
	if err != nil {
		p.pool.Close()
		p.logger.Fatalf("Failed insert into %s. query: %s, args: %v, err: %v", p.config.Table, query, args, err)
	}
}

func (p *Plugin) processEvent(event *pipeline.Event, pgFields []column, uniqueFields map[string]pgType) (fieldValues []interface{}, uniqueID string, err error) {
	fieldValues = make([]interface{}, 0, len(pgFields))
	uniqueID = ""

	for _, field := range pgFields {
		fieldNode, err := event.Root.DigStrict(field.Name)
		if err != nil {
			return nil, "", fmt.Errorf("%w. required field %s", ErrEventDoesntHaveField, field.Name)
		}

		lVal, err := p.addFieldToValues(field, fieldNode)
		if err != nil {
			return nil, "", err
		}

		fieldValues = append(fieldValues, lVal)

		if _, ok := uniqueFields[field.Name]; ok {
			if field.ColType == pgInt || field.ColType == pgTimestamp {
				uniqueID += strconv.Itoa(lVal.(int))
			} else {
				uniqueID += lVal.(string)
			}
		}
	}

	return fieldValues, uniqueID, nil
}

func (p *Plugin) addFieldToValues(field column, sNode *insaneJSON.StrictNode) (interface{}, error) {
	switch field.ColType {
	case pgString:
		lVal, err := sNode.AsString()
		if err != nil {
			return nil, fmt.Errorf("%w, can't get %s as string, err: %s", ErrEventFieldHasWrongType, field.Name, err.Error())
		}
		return lVal, nil
	case pgInt:
		lVal, err := sNode.AsInt()
		if err != nil {
			return nil, fmt.Errorf("%w, can't get %s as int, err: %s", ErrEventFieldHasWrongType, field.Name, err.Error())
		}
		return lVal, nil
	case pgTimestamp:
		tint, err := sNode.AsInt()
		if err != nil {
			return nil, fmt.Errorf("%w, can't get %s as timestamp, err: %s", ErrEventFieldHasWrongType, field.Name, err.Error())
		}
		return time.Unix(int64(tint), 0).Format(time.RFC3339), nil
	case unknownType:
		fallthrough
	default:
		return nil, fmt.Errorf("%w, undefined col type: %d, col name: %s", ErrEventFieldHasWrongType, field.ColType, field.Name)
	}
}
