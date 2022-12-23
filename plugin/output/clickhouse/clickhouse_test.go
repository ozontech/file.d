package clickhouse

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/metric"
	"github.com/ozontech/file.d/pipeline"
	mock_ch "github.com/ozontech/file.d/plugin/output/clickhouse/mock"
	"github.com/stretchr/testify/require"
	insaneJSON "github.com/vitkovskii/insane-json"
)

func TestPrivateOut(t *testing.T) {
	testLogger := logger.Instance

	root := insaneJSON.Spawn()
	defer insaneJSON.Release(root)

	columns := []ConfigColumn{
		{
			Name:       "str_uni_1",
			ColumnType: "string",
		},
		{
			Name:       "int_uni_1",
			ColumnType: "int",
		},
		{
			Name:       "int_1",
			ColumnType: "int",
		},
		{
			Name:       "timestamp_1",
			ColumnType: "timestamp",
		},
	}

	strUniValue := "str_uni_1_value"
	intUniValue := 11
	intValue := 10
	timestampValue := 100

	root.AddField(columns[0].Name).MutateToString(strUniValue)
	root.AddField(columns[1].Name).MutateToInt(intUniValue)
	root.AddField(columns[2].Name).MutateToInt(intValue)
	root.AddField(columns[3].Name).MutateToInt(timestampValue)

	table := "table1"

	config := Config{
		Columns: columns,
		Retry:   3,
	}

	ctl := gomock.NewController(t)
	defer ctl.Finish()
	mockdb := mock_ch.NewMockDBIface(ctl)
	db := mockdb

	ctx := context.Background()
	var ctxMock = reflect.TypeOf((*context.Context)(nil)).Elem()

	mockdb.EXPECT().ExecContext(
		gomock.AssignableToTypeOf(ctxMock),
		"INSERT INTO table1 (str_uni_1,int_uni_1,int_1,timestamp_1) VALUES (?,?,?,?)",
		[]any{strUniValue, intUniValue, intValue, time.Unix(int64(timestampValue), 0).Format(time.RFC3339)},
	).Return(&resultForTest{}, nil).Times(1)

	builder, err := NewQueryBuilder(columns, table)
	require.NoError(t, err)

	p := &Plugin{
		config:       &config,
		queryBuilder: builder,
		pool:         db,
		logger:       testLogger,
		ctx:          ctx,
	}

	p.RegisterMetrics(metric.New("test"))

	batch := &pipeline.Batch{Events: []*pipeline.Event{{Root: root}}}
	p.out(nil, batch)
}

func TestPrivateOutWithRetry(t *testing.T) {
	testLogger := logger.Instance

	root := insaneJSON.Spawn()
	defer insaneJSON.Release(root)

	columns := []ConfigColumn{
		{
			Name:       "str_uni_1",
			ColumnType: "string",
		},
		{
			Name:       "int_1",
			ColumnType: "int",
		},
		{
			Name:       "timestamp_1",
			ColumnType: "timestamp",
		},
	}

	strUniValue := "str_uni_1_value"
	intValue := 10
	timestampValue := 100

	root.AddField(columns[0].Name).MutateToString(strUniValue)
	root.AddField(columns[1].Name).MutateToInt(intValue)
	root.AddField(columns[2].Name).MutateToInt(timestampValue)

	table := "table1"

	config := Config{
		Columns: columns,
		Retry:   3,
	}

	ctl := gomock.NewController(t)
	defer ctl.Finish()
	mockdb := mock_ch.NewMockDBIface(ctl)
	db := mockdb

	ctx := context.Background()
	var ctxMock = reflect.TypeOf((*context.Context)(nil)).Elem()

	mockdb.EXPECT().ExecContext(
		gomock.AssignableToTypeOf(ctxMock),
		"INSERT INTO table1 (str_uni_1,int_1,timestamp_1) VALUES (?,?,?)",
		[]any{strUniValue, intValue, time.Unix(int64(timestampValue), 0).Format(time.RFC3339)},
	).Return(&resultForTest{}, errors.New("someError")).Times(2)
	mockdb.EXPECT().ExecContext(
		gomock.AssignableToTypeOf(ctxMock),
		"INSERT INTO table1 (str_uni_1,int_1,timestamp_1) VALUES (?,?,?)",
		[]any{strUniValue, intValue, time.Unix(int64(timestampValue), 0).Format(time.RFC3339)},
	).Return(&resultForTest{}, nil).Times(1)

	builder, err := NewQueryBuilder(columns, table)
	require.NoError(t, err)

	p := &Plugin{
		config:       &config,
		queryBuilder: builder,
		pool:         db,
		logger:       testLogger,
		ctx:          ctx,
	}

	p.RegisterMetrics(metric.New("test"))

	batch := &pipeline.Batch{Events: []*pipeline.Event{{Root: root}}}
	p.out(nil, batch)
}

func TestPrivateOutNoGoodEvents(t *testing.T) {
	testLogger := logger.Instance

	root := insaneJSON.Spawn()
	defer insaneJSON.Release(root)

	columns := []ConfigColumn{
		{
			Name:       "str_uni_1",
			ColumnType: "string",
		},
		{
			Name:       "int_1",
			ColumnType: "int",
		},
		{
			Name:       "timestamp_1",
			ColumnType: "timestamp",
		},
	}

	strUniValue := "str_uni_1_value"
	intValue := 10

	// timestamp valur wasn't sent.
	root.AddField(columns[0].Name).MutateToString(strUniValue)
	root.AddField(columns[1].Name).MutateToInt(intValue)

	table := "table1"

	config := Config{
		Columns: columns,
		Retry:   3,
	}

	builder, err := NewQueryBuilder(columns, table)
	require.NoError(t, err)
	
	p := &Plugin{
		config:       &config,
		queryBuilder: builder,
		logger:       testLogger,
	}
	
	p.RegisterMetrics(metric.New("test"))
	
	batch := &pipeline.Batch{Events: []*pipeline.Event{{Root: root}}}
	p.out(nil, batch)
}

func TestPrivateOutWrongTypeInField(t *testing.T) {
	testLogger := logger.Instance

	root := insaneJSON.Spawn()
	defer insaneJSON.Release(root)

	columns := []ConfigColumn{
		{
			Name:       "str_uni_1",
			ColumnType: "string",
		},
		{
			Name:       "int_uni_1",
			ColumnType: "int",
		},
		{
			Name:       "int_1",
			ColumnType: "int",
		},
		{
			Name:       "timestamp_1",
			ColumnType: "timestamp",
		},
	}

	strUniValue := "str_uni_1_value"
	intUniValue := 11
	intValue := 10
	timestampValue := "100"

	root.AddField(columns[0].Name).MutateToString(strUniValue)
	root.AddField(columns[1].Name).MutateToInt(intUniValue)
	root.AddField(columns[2].Name).MutateToInt(intValue)
	// instead of 100 sender put "100" to json. Message'll be truncated.
	root.AddField(columns[3].Name).MutateToString(timestampValue)

	table := "table1"

	config := Config{Columns: columns, Retry: 3}

	ctl := gomock.NewController(t)
	defer ctl.Finish()

	builder, err := NewQueryBuilder(columns, table)
	require.NoError(t, err)

	p := &Plugin{
		config:       &config,
		queryBuilder: builder,
		logger:       testLogger,
	}

	p.RegisterMetrics(metric.New("test"))

	batch := &pipeline.Batch{Events: []*pipeline.Event{{Root: root}}}
	p.out(nil, batch)
}

func TestPrivateOutFewUniqueEventsYetWithBadEvents(t *testing.T) {
	testLogger := logger.Instance

	root := insaneJSON.Spawn()
	defer insaneJSON.Release(root)

	secondUniqueRoot := insaneJSON.Spawn()
	defer insaneJSON.Release(secondUniqueRoot)

	badRoot := insaneJSON.Spawn()
	defer insaneJSON.Release(badRoot)

	columns := []ConfigColumn{
		{
			Name:       "str_uni_1",
			ColumnType: "string",
		},
		{
			Name:       "int_uni_1",
			ColumnType: "int",
		},
		{
			Name:       "int_1",
			ColumnType: "int",
		},
		{
			Name:       "timestamp_1",
			ColumnType: "timestamp",
		},
	}

	strUniValue := "str_uni_1_value"
	intUniValue := 11
	intValue := 10
	timestampValue := 100

	secStrUniValue := "str_uni_1_value____"
	secIntUniValue := 11000
	secIntValue := 10999
	secTimestampValue := 1008

	badTimestampValue := "100"

	root.AddField(columns[0].Name).MutateToString(strUniValue)
	root.AddField(columns[1].Name).MutateToInt(intUniValue)
	root.AddField(columns[2].Name).MutateToInt(intValue)
	root.AddField(columns[3].Name).MutateToInt(timestampValue)

	secondUniqueRoot.AddField(columns[0].Name).MutateToString(secStrUniValue)
	secondUniqueRoot.AddField(columns[1].Name).MutateToInt(secIntUniValue)
	secondUniqueRoot.AddField(columns[2].Name).MutateToInt(secIntValue)
	secondUniqueRoot.AddField(columns[3].Name).MutateToInt(secTimestampValue)

	badRoot.AddField(columns[0].Name).MutateToString(strUniValue)
	badRoot.AddField(columns[1].Name).MutateToInt(intUniValue)
	badRoot.AddField(columns[2].Name).MutateToInt(intValue)
	// instead of 100 sender put "100" to json. Message'll be truncated.
	badRoot.AddField(columns[3].Name).MutateToString(badTimestampValue)

	table := "table1"

	config := Config{
		Columns: columns,
		Retry:   3,
	}

	ctl := gomock.NewController(t)
	defer ctl.Finish()
	mockdb := mock_ch.NewMockDBIface(ctl)
	db := mockdb

	ctx := context.Background()
	var ctxMock = reflect.TypeOf((*context.Context)(nil)).Elem()

	mockdb.EXPECT().ExecContext(
		gomock.AssignableToTypeOf(ctxMock),
		"INSERT INTO table1 (str_uni_1,int_uni_1,int_1,timestamp_1) VALUES (?,?,?,?),(?,?,?,?)",
		[]any{strUniValue, intUniValue, intValue, time.Unix(int64(timestampValue), 0).Format(time.RFC3339),
			secStrUniValue, secIntUniValue, secIntValue, time.Unix(int64(secTimestampValue), 0).Format(time.RFC3339)},
	).Return(&resultForTest{}, nil).Times(1)

	builder, err := NewQueryBuilder(columns, table)
	require.NoError(t, err)

	p := &Plugin{
		config:       &config,
		queryBuilder: builder,
		pool:         db,
		logger:       testLogger,
		ctx:          ctx,
	}

	p.RegisterMetrics(metric.New("test"))

	batch := &pipeline.Batch{Events: []*pipeline.Event{
		{Root: root},
		{Root: secondUniqueRoot},
		{Root: badRoot},
	}}
	p.out(nil, batch)
}

func TestPrivateOutEventWithTimestring(t *testing.T) {
	testLogger := logger.Instance

	root := insaneJSON.Spawn()
	defer insaneJSON.Release(root)

	columns := []ConfigColumn{
		{
			Name:       "time",
			ColumnType: "timestring",
		},
	}

	strTimeValue := "2022-12-11T20:20:10.037011974Z"

	root.AddField(columns[0].Name).MutateToString(strTimeValue)

	table := "table1"

	config := Config{
		Columns: columns,
		Retry:   3,
	}

	ctl := gomock.NewController(t)
	defer ctl.Finish()
	mockdb := mock_ch.NewMockDBIface(ctl)
	db := mockdb

	ctx := context.Background()
	var ctxMock = reflect.TypeOf((*context.Context)(nil)).Elem()

	mockdb.EXPECT().ExecContext(
		gomock.AssignableToTypeOf(ctxMock),
		"INSERT INTO table1 (time,timens) VALUES (?,?)",
		[]any{"2022-12-11T20:20:10", int32(37011974)},
	).Return(&resultForTest{}, nil).Times(1)

	builder, err := NewQueryBuilder(columns, table)
	require.NoError(t, err)

	p := &Plugin{
		config:       &config,
		queryBuilder: builder,
		pool:         db,
		logger:       testLogger,
		ctx:          ctx,
	}

	p.RegisterMetrics(metric.New("test"))

	batch := &pipeline.Batch{Events: []*pipeline.Event{
		{Root: root},
	}}
	p.out(nil, batch)
}

// TODO replace with gomock
type resultForTest struct{}

func (r resultForTest) LastInsertId() (int64, error)                    { return 1, nil }
func (r resultForTest) RowsAffected() (int64, error)                    { return 1, nil }
