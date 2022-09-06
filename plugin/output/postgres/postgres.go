package postgres

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/metric"
	"github.com/ozontech/file.d/pipeline"
	insaneJSON "github.com/vitkovskii/insane-json"
	"go.uber.org/zap"
)

/*{ introduction
It sends the event batches to postgres db using pgx.
}*/

var (
	ErrEventDoesntHaveField             = errors.New("event doesn't have field")
	ErrEventFieldHasWrongType           = errors.New("event field has wrong type")
	ErrTimestampFromDistantPastOrFuture = errors.New("event field contains timestamp < 1970 or > 9000 year")
)

type PgxIface interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	Close()
}

const (
	outPluginType = "postgres"
	subsystemName = "output_postgres"

	// required for PgBouncers that doesn't support prepared statements.
	preferSimpleProtocol = pgx.QuerySimpleProtocol(true)

	// metric
	discardedEventCounter  = "event_discarded"
	duplicatedEventCounter = "event_duplicated"
	writtenEventCounter    = "event_written"

	nineThousandYear = 221842627200
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

	queryBuilder PgQueryBuilder
	pool         PgxIface
}

type ConfigColumn struct {
	Name       string `json:"name" required:"true"`
	ColumnType string `json:"type" required:"true" options:"int|string|bool|timestamp"`
	Unique     bool   `json:"unique" default:"false"`
}

// ! config-params
// ^ config-params
type Config struct {
	// > @3@4@5@6
	// >
	// > In strict mode file.d will crash on events without required columns.
	// Otherwise events will be discarded.
	Strict bool `json:"strict" default:"false"` // *

	// > @3@4@5@6
	// >
	// > PostgreSQL connection string in URL or DSN format.
	// >
	// > Example DSN:
	// >
	// > `user=user password=secret host=pg.example.com port=5432 dbname=mydb sslmode=disable pool_max_conns=10`
	ConnString string `json:"conn_string" required:"true"` // *

	// > @3@4@5@6
	// >
	// > Pg target table.
	Table string `json:"table" required:"true"` // *

	// > @3@4@5@6
	// >
	// > Array of DB columns. Each column have:
	// > name, type (int, string, timestamp - which int that will be converted to timestamptz of rfc3339)
	// > and nullable options.
	Columns []ConfigColumn `json:"columns" required:"true" slice:"true"` // *

	// > @3@4@5@6
	// >
	// > Retries of insertion.
	Retry int `json:"retry" default:"3"` // *

	// > @3@4@5@6
	// >
	// > Retention milliseconds for retry to DB.
	Retention  cfg.Duration `json:"retention" default:"50ms" parse:"duration"` // *
	Retention_ time.Duration

	// > @3@4@5@6
	// >
	// > Timeout for DB requests in milliseconds.
	DBRequestTimeout  cfg.Duration `json:"db_request_timeout" default:"3000ms" parse:"duration"` // *
	DBRequestTimeout_ time.Duration

	// > @3@4@5@6
	// >
	// > Timeout for DB health check.
	DBHealthCheckPeriod  cfg.Duration `json:"db_health_check_period" default:"60s" parse:"duration"` // *
	DBHealthCheckPeriod_ time.Duration

	// > @3@4@5@6
	// >
	// > How much workers will be instantiated to send batches.
	WorkersCount  cfg.Expression `json:"workers_count" default:"gomaxprocs*4" parse:"expression"` // *
	WorkersCount_ int

	// > @3@4@5@6
	// >
	// > Maximum quantity of events to pack into one batch.
	BatchSize cfg.Expression `json:"batch_size" default:"capacity/4"  parse:"expression"` // *
	//BatchSize  cfg.Expression `json:"batch_size" default:"capacity/4" parse:"expression"` // *
	BatchSize_ int

	// > @3@4@5@6
	// >
	// > A minimum size of events in a batch to send.
	// > If both batch_size and batch_size_bytes are set, they will work together.
	BatchSizeBytes  cfg.Expression `json:"batch_size_bytes" default:"0" parse:"expression"` // *
	BatchSizeBytes_ int

	// > @3@4@5@6
	// >
	// > After this timeout batch will be sent even if batch isn't completed.
	BatchFlushTimeout  cfg.Duration `json:"batch_flush_timeout" default:"200ms" parse:"duration"` // *
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
	metric.RegisterCounter(&metric.MetricDesc{
		Name:      discardedEventCounter,
		Subsystem: subsystemName,
		Help:      "Total pgsql discarded messages",
	})
	metric.RegisterCounter(&metric.MetricDesc{
		Name:      duplicatedEventCounter,
		Subsystem: subsystemName,
		Help:      "Total pgsql duplicated messages",
	})
	metric.RegisterCounter(&metric.MetricDesc{
		Name:      writtenEventCounter,
		Subsystem: subsystemName,
		Help:      "Total events written to pgsql",
	})
}

func (p *Plugin) Start(config pipeline.AnyConfig, params *pipeline.OutputPluginParams) {
	p.controller = params.Controller
	p.logger = params.Logger
	p.config = config.(*Config)
	p.ctx = context.Background()
	if len(p.config.Columns) == 0 {
		p.logger.Fatal("can't start plugin, no fields in config")
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

	pgCfg, err := p.parsePGConfig()
	if err != nil {
		p.logger.Fatalf("can't create pgsql config: %v", err)
	}

	pool, err := pgxpool.ConnectConfig(p.ctx, pgCfg)
	if err != nil {
		p.logger.Fatalf("can't create pgsql pool: %v", err)
	}
	p.pool = pool

	p.batcher = pipeline.NewBatcher(pipeline.BatcherOptions{
		PipelineName:   params.PipelineName,
		OutputType:     outPluginType,
		OutFn:          p.out,
		Controller:     p.controller,
		Workers:        p.config.WorkersCount_,
		BatchSizeCount: p.config.BatchSize_,
		BatchSizeBytes: p.config.BatchSizeBytes_,
		FlushTimeout:   p.config.BatchFlushTimeout_,
	})

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
				metric.GetCounter(subsystemName, discardedEventCounter).Inc()
				if p.config.Strict {
					p.logger.Fatal(err)
				}
				p.logger.Error(err)
			} else if errors.Is(err, ErrEventFieldHasWrongType) {
				metric.GetCounter(subsystemName, discardedEventCounter).Inc()
				if p.config.Strict {
					p.logger.Fatal(err)
				}
				p.logger.Error(err)
			} else if errors.Is(err, ErrTimestampFromDistantPastOrFuture) {
				metric.GetCounter(subsystemName, discardedEventCounter).Inc()
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
			metric.GetCounter(subsystemName, duplicatedEventCounter).Inc()
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

	var argsSliceInterface = make([]any, len(args)+1)

	argsSliceInterface[0] = preferSimpleProtocol
	for i := 1; i < len(args)+1; i++ {
		argsSliceInterface[i] = args[i-1]
	}

	// Insert into pg with retry.
	for i := p.config.Retry; i > 0; i-- {
		err = p.try(query, argsSliceInterface)
		if err != nil {
			p.logger.Errorf("can't exec query: %s", err.Error())
			time.Sleep(p.config.Retention_)
			continue
		}
		metric.GetCounter(subsystemName, writtenEventCounter).Add(float64(len(uniqueEventsMap)))
		break
	}

	if err != nil {
		p.pool.Close()
		p.logger.Fatalf("failed insert into %s. query: %s, args: %v, err: %v", p.config.Table, query, args, err)
	}
}

func (p *Plugin) try(query string, argsSliceInterface []any) error {
	ctx, cancel := context.WithTimeout(p.ctx, p.config.DBRequestTimeout_)
	defer cancel()

	rows, err := p.pool.Query(ctx, query, argsSliceInterface...)
	if err != nil {
		return err
	}

	defer rows.Close()

	return err
}

func (p *Plugin) processEvent(event *pipeline.Event, pgFields []column, uniqueFields map[string]pgType) (fieldValues []any, uniqueID string, err error) {
	fieldValues = make([]any, 0, len(pgFields))
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

func (p *Plugin) addFieldToValues(field column, sNode *insaneJSON.StrictNode) (any, error) {
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
		// if timestamp < 1970-01-00-00 or > 9000-01-00-00
		if tint < 0 || tint > nineThousandYear {
			return nil, fmt.Errorf("%w, %s", ErrTimestampFromDistantPastOrFuture, field.Name)
		}
		return time.Unix(int64(tint), 0).Format(time.RFC3339), nil
	case unknownType:
		fallthrough
	default:
		return nil, fmt.Errorf("%w, undefined col type: %d, col name: %s", ErrEventFieldHasWrongType, field.ColType, field.Name)
	}
}

func (p *Plugin) parsePGConfig() (*pgxpool.Config, error) {
	pgCfg, err := pgxpool.ParseConfig(p.config.ConnString)
	if err != nil {
		return nil, err
	}

	pgCfg.LazyConnect = false
	pgCfg.HealthCheckPeriod = p.config.DBHealthCheckPeriod_

	return pgCfg, nil
}
