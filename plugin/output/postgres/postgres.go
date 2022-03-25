package postgres

import (
	"fmt"
	"strconv"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx"
	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/stats"
	insaneJSON "github.com/vitkovskii/insane-json"
	"go.uber.org/zap"
	"golang.org/x/net/context"
)

/*{ introduction
It sends the event batches to postgres db using pgx.
}*/

type PgxIface interface {
	Exec(sql string, arguments ...interface{}) (commandTag pgx.CommandTag, err error)
	Close()
}

const (
	outPluginType = "postgres"
	subsystemName = "output_postgres"

	// metrics
	discardedEventCounter  = "event_discarded"
	duplicatedEventCounter = "event_duplicated"
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

type column struct {
	Name    string
	ColType pgType
	Unique  bool
}

type pgType int

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
	//> Db host.
	Host string `json:"host" required:"true"` //*

	//> @3@4@5@6
	//>
	//> Db port.
	Port uint16 `json:"port" required:"true"` //*

	//> @3@4@5@6
	//>
	//> Dbname in pg.
	DBname string `json:"dbname" required:"true"` //*

	//> @3@4@5@6
	//>
	//> Pg user name.
	Username string `json:"user" required:"true"` //*

	//> @3@4@5@6
	//>
	//> Pg user pwd.
	Pwd string `json:"password" required:"true"` //*

	//> @3@4@5@6
	//>
	//> Pg target table.
	Table string `json:"table" required:"true"` //*

	//> @3@4@5@6
	//>
	//> Retries of insertion.
	Retry int `json:"retry" default:"3"` //*

	//> @3@4@5@6
	//>
	//> Array of DB columns. Each column have:
	//> name, type (int, string, bool, timestamp - which int that will be converted to timestamptz of rfc3339)
	//> and nullable options.
	Columns []ConfigColumn `json:"columns" required:"true" slice:"true"` //*

	//> @3@4@5@6
	//>
	//> Interval of creation new file
	RetentionInterval  cfg.Duration `json:"retention_interval" default:"1h" parse:"duration"` //*
	RetentionInterval_ time.Duration

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
	if len(p.config.Columns) == 0 {
		p.logger.Fatal("Can't start plugin, no fields in config")
	}

	if p.config.Retry < 1 {
		p.logger.Fatal("Retry count can't be <1")
	}

	p.registerPluginMetrics()
	queryBuilder, err := NewQueryBuilder(p.config.Columns, p.config.Table)
	if err != nil {
		p.logger.Fatal(err)
	}
	p.queryBuilder = queryBuilder

	pool, err := pgx.NewConnPool(pgx.ConnPoolConfig{
		ConnConfig: pgx.ConnConfig{
			Host:     p.config.Host,
			Port:     p.config.Port,
			Database: p.config.DBname,
			Password: p.config.Pwd,
			User:     p.config.Username,
			// to work with
			PreferSimpleProtocol: true,
		},
	})
	if err != nil {
		p.logger.Fatalf("can't create pgsql pool: %v", err)
	}
	p.pool = pool

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
	builder := p.queryBuilder.GetInsertBuilder()
	pgFields := p.queryBuilder.GetPgFields()
	uniqueFields := p.queryBuilder.GetUniqueFields()

	// Deduplicate events, pg can't do upsert with duplication.
	uniqueEventsMap := make(map[string]struct{}, len(batch.Events))
	var haveValidEvents bool

	for _, event := range batch.Events {
		lVals := make([]interface{}, 0, len(pgFields))
		uniqueID := ""

	LOOP_THROUGH_EVENT:
		for _, field := range pgFields {
			fNode, err := event.Root.DigStrict(field.Name)
			if err != nil {
				// In strict mode file.d fails, otherwise truncate bad event.
				if p.config.Strict {
					p.logger.Fatalf("Required field '%s' doesn't appear in event: %s", field.Name, string(event.Buf))
				}

				p.logger.Error("Required field '%s' doesn't appear in event: %s", field.Name, string(event.Buf))
				stats.GetCounter(subsystemName, discardedEventCounter).Inc()
				// stop loop for event
				break LOOP_THROUGH_EVENT
			}

			lVal, err := p.addFieldToValues(field, fNode)
			if err != nil {
				if p.config.Strict {
					p.logger.Fatal(err)
				}
				p.logger.Error(err)
				stats.GetCounter(subsystemName, discardedEventCounter).Inc()
				break LOOP_THROUGH_EVENT
			}

			lVals = append(lVals, lVal)

			if _, ok := uniqueFields[field.Name]; ok {
				if field.ColType == pgInt || field.ColType == pgTimestamp {
					uniqueID += strconv.Itoa(lVal.(int))
				} else {
					uniqueID += lVal.(string)
				}
			}
		}

		// If LOOP_THROUGH_EVENT was breaked values mustn't be added.
		if len(lVals) == len(pgFields) {
			if _, ok := uniqueEventsMap[uniqueID]; ok {
				stats.GetCounter(subsystemName, duplicatedEventCounter).Inc()
				p.logger.Infof("event duplicated. Fields: %v, values: %v", pgFields, lVals)
			} else {
				uniqueEventsMap[uniqueID] = struct{}{}
				haveValidEvents = true
				builder = builder.Values(lVals...)
			}
		}
	}

	builder = builder.Suffix(p.queryBuilder.GetPostfix()).PlaceholderFormat(sq.Dollar)

	// no valid events passed in non-strict mode.
	if !haveValidEvents {
		return
	}
	query, args, err := builder.ToSql()
	if err != nil {
		p.logger.Fatalf("Invalid SQL. query: %s, args: %v, err: %v", query, args, err)
		return
	}

	// Insert into pg with retry.
	for i := p.config.Retry; i > 0; i-- {
		p.logger.Info(query, args)
		_, err = p.pool.Exec(query, args...)
		if err != nil {
			time.Sleep(time.Millisecond * 50)
			continue
		}

		break
	}
	if err != nil {
		p.pool.Close()
		p.logger.Fatalf("Failed insert into %s. query: %s, args: %v, err: %v", p.config.Table, query, args, err)
	}
}

func (p *Plugin) addFieldToValues(field column, sNode *insaneJSON.StrictNode) (interface{}, error) {
	switch field.ColType {
	case pgString:
		lVal, err := sNode.AsString()
		if err != nil {
			return nil, fmt.Errorf("can't get %s as string, err: %s", field.Name, err.Error())
		}
		return lVal, nil
	case pgInt:
		lVal, err := sNode.AsInt()
		if err != nil {
			return nil, fmt.Errorf("can't get %s as int, err: %s", field.Name, err.Error())
		}
		return lVal, nil
	case pgTimestamp:
		tint, err := sNode.AsInt()
		if err != nil {
			return nil, fmt.Errorf("can't get %s as timestamp, err: %s", field.Name, err.Error())
		}
		return time.Unix(int64(tint), 0).Format(time.RFC3339), nil
	case unknownType:
		fallthrough
	default:
		return nil, fmt.Errorf("undefined col type: %d, col name: %s", field.ColType, field.Name)
	}
}
