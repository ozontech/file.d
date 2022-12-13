package clickhouse

import (
	"errors"
	"testing"

	sq "github.com/Masterminds/squirrel"
	"github.com/stretchr/testify/require"
)

func TestNewQueryBuilderError(t *testing.T) {
	cases := []struct {
		name    string
		cfgCols []ConfigColumn
		table   string
		err     error
	}{
		{
			name:    "empty columns",
			cfgCols: []ConfigColumn{},
			table:   "secret",
			err:     ErrNoColumns,
		},
		{
			name:    "empty table",
			cfgCols: []ConfigColumn{{ColumnType: "string"}},
			table:   "",
			err:     ErrEmptyTableName,
		},
		{
			name:    "invalid field type",
			cfgCols: []ConfigColumn{{Name: "col_name", ColumnType: "invalid_type"}},
			table:   "test_table",
			err:     errors.New("invalid pg type: invalid_type"),
		},
	}

	for _, tCase := range cases {
		t.Run(tCase.name, func(t *testing.T) {
			queryBuilder, err := NewQueryBuilder(tCase.cfgCols, tCase.table)
			require.Error(t, err)
			require.EqualError(t, tCase.err, err.Error())
			require.Nil(t, queryBuilder)
		})
	}
}

func TestNewQueryBuilderSuccess(t *testing.T) {
	cases := []struct {
		name            string
		table           string
		returnedBuilder sq.InsertBuilder
		returnedPostfix string
		cfgColumns      []ConfigColumn
	}{
		{
			name:  "no unique columns",
			table: "yet_another_table",
			returnedBuilder: sq.Insert("yet_another_table").
				Columns("some_string_col", "some_int_col", "some_timestamp_col", "some_other_string_col"),
			returnedPostfix: "",
			cfgColumns: []ConfigColumn{
				{
					Name:       "some_string_col",
					ColumnType: "string",
					Unique:     false,
				},
				{
					Name:       "some_int_col",
					ColumnType: "int",
					Unique:     false,
				},
				{
					Name:       "some_timestamp_col",
					ColumnType: "timestamp",
					Unique:     false,
				},
				{
					Name:       "some_other_string_col",
					ColumnType: "string",
					Unique:     false,
				},
			},
		},
		{
			name:  "one unique column",
			table: "yet_another_table_with_unique_col",
			returnedBuilder: sq.Insert("yet_another_table_with_unique_col").
				Columns("uni_str_col", "int_col", "timestamp_col", "other_string_col"),
			returnedPostfix: "ON CONFLICT(uni_str_col) DO UPDATE SET " +
				"int_col=EXCLUDED.int_col,timestamp_col=EXCLUDED.timestamp_col," +
				"other_string_col=EXCLUDED.other_string_col",
			cfgColumns: []ConfigColumn{
				{
					Name:       "uni_str_col",
					ColumnType: "string",
					Unique:     true,
				},
				{
					Name:       "int_col",
					ColumnType: "int",
					Unique:     false,
				},
				{
					Name:       "timestamp_col",
					ColumnType: "timestamp",
					Unique:     false,
				},
				{
					Name:       "other_string_col",
					ColumnType: "string",
					Unique:     false,
				},
			},
		},
		{
			name:  "many unique columns",
			table: "yet_another_table_with_many_unique_cols",
			returnedBuilder: sq.Insert("yet_another_table_with_many_unique_cols").
				Columns("uni_str_col", "int_col", "timestamp_col", "other_string_col", "other_timestamp_col"),
			returnedPostfix: "ON CONFLICT(uni_str_col,int_col,timestamp_col,other_string_col) DO UPDATE SET " +
				"other_timestamp_col=EXCLUDED.other_timestamp_col",
			cfgColumns: []ConfigColumn{
				{
					Name:       "uni_str_col",
					ColumnType: "string",
					Unique:     true,
				},
				{
					Name:       "int_col",
					ColumnType: "int",
					Unique:     true,
				},
				{
					Name:       "timestamp_col",
					ColumnType: "timestamp",
					Unique:     true,
				},
				{
					Name:       "other_string_col",
					ColumnType: "string",
					Unique:     true,
				},
				{
					Name:       "other_timestamp_col",
					ColumnType: "timestamp",
					Unique:     false,
				},
			},
		},
		{
			name:  "all columns unique",
			table: "yet_another_table_with_all_unique_cols",
			returnedBuilder: sq.Insert("yet_another_table_with_all_unique_cols").
				Columns("uni_str_col", "int_col", "timestamp_col", "other_string_col", "other_timestamp_col"),
			returnedPostfix: "ON CONFLICT (uni_str_col,int_col,timestamp_col,other_string_col,other_timestamp_col) DO NOTHING",
			cfgColumns: []ConfigColumn{
				{
					Name:       "uni_str_col",
					ColumnType: "string",
					Unique:     true,
				},
				{
					Name:       "int_col",
					ColumnType: "int",
					Unique:     true,
				},
				{
					Name:       "timestamp_col",
					ColumnType: "timestamp",
					Unique:     true,
				},
				{
					Name:       "other_string_col",
					ColumnType: "string",
					Unique:     true,
				},
				{
					Name:       "other_timestamp_col",
					ColumnType: "timestamp",
					Unique:     true,
				},
			},
		},
	}

	for _, tCase := range cases {
		t.Run(tCase.name, func(t *testing.T) {
			queryBuilder, err := NewQueryBuilder(tCase.cfgColumns, tCase.table)
			require.NoError(t, err)
			require.NotNil(t, queryBuilder)

			builder := queryBuilder.(*pgQueryBuilder)

			require.Equal(t, tCase.returnedBuilder, builder.queryBuilder)
			require.Equal(t, tCase.returnedPostfix, builder.postfix)
		})
	}
}

func TestGetClickhouseFields(t *testing.T) {
	columns := []ConfigColumn{
		{
			Name:       "uni_str_col",
			ColumnType: "string",
			Unique:     true,
		},
		{
			Name:       "int_col",
			ColumnType: "int",
			Unique:     false,
		},
	}
	table := "some_table"

	expectedPgColumns := []column{
		{
			Name:    columns[0].Name,
			ColType: pgString,
			Unique:  columns[0].Unique,
		},
		{
			Name:    columns[1].Name,
			ColType: pgInt,
			Unique:  columns[1].Unique,
		},
	}

	queryBuilder, err := NewQueryBuilder(columns, table)
	require.NoError(t, err)
	require.NotNil(t, queryBuilder)

	pgColumns := queryBuilder.GetClickhouseFields()
	require.Equal(t, expectedPgColumns, pgColumns)
}

func TestGetInsertBuilder(t *testing.T) {
	columns := []ConfigColumn{
		{
			Name:       "uni_str_col",
			ColumnType: "string",
			Unique:     true,
		},
		{
			Name:       "int_col",
			ColumnType: "int",
			Unique:     false,
		},
	}
	table := "some_table"

	expectedInsertBuilder := sq.Insert(table).Columns(columns[0].Name, columns[1].Name)

	queryBuilder, err := NewQueryBuilder(columns, table)
	require.NoError(t, err)
	require.NotNil(t, queryBuilder)

	insertBuilder := queryBuilder.GetInsertBuilder()
	require.Equal(t, expectedInsertBuilder, insertBuilder)
}

func TestGetPostfixBuilder(t *testing.T) {
	columns := []ConfigColumn{
		{
			Name:       "uni_str_col",
			ColumnType: "string",
			Unique:     true,
		},
		{
			Name:       "int_col",
			ColumnType: "int",
			Unique:     false,
		},
	}
	table := "some_table"

	expectedPostfix := "ON CONFLICT(uni_str_col) DO UPDATE SET int_col=EXCLUDED.int_col"

	queryBuilder, err := NewQueryBuilder(columns, table)
	require.NoError(t, err)
	require.NotNil(t, queryBuilder)

	postfix := queryBuilder.GetPostfix()
	require.Equal(t, expectedPostfix, postfix)
}

func TestGetUniqueFields(t *testing.T) {
	columns := []ConfigColumn{
		{
			Name:       "uni_str_col",
			ColumnType: "string",
			Unique:     true,
		},
		{
			Name:       "uni_int_col",
			ColumnType: "int",
			Unique:     true,
		},
		{
			Name:       "uni_timestamp_col",
			ColumnType: "timestamp",
			Unique:     true,
		},
		{
			Name:       "int_col",
			ColumnType: "int",
			Unique:     false,
		},
	}
	table := "some_table"

	expectedUniqueFields := map[string]pgType{
		"uni_str_col":       pgString,
		"uni_int_col":       pgInt,
		"uni_timestamp_col": pgTimestamp,
	}

	queryBuilder, err := NewQueryBuilder(columns, table)
	require.NoError(t, err)
	require.NotNil(t, queryBuilder)

	uniqueFields := queryBuilder.GetUniqueFields()
	require.Equal(t, expectedUniqueFields, uniqueFields)
}
