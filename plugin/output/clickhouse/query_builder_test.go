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
			err:     errors.New("invalid ch type: invalid_type"),
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
		cfgColumns      []ConfigColumn
	}{
		{
			name:  "no unique columns",
			table: "yet_another_table",
			returnedBuilder: sq.Insert("yet_another_table").
				Columns("some_string_col", "some_int_col", "some_timestamp_col", "some_other_string_col"),
			cfgColumns: []ConfigColumn{
				{
					Name:       "some_string_col",
					ColumnType: "string",
				},
				{
					Name:       "some_int_col",
					ColumnType: "int",
				},
				{
					Name:       "some_timestamp_col",
					ColumnType: "timestamp",
				},
				{
					Name:       "some_other_string_col",
					ColumnType: "string",
				},
			},
		},
		{
			name:  "one unique column",
			table: "yet_another_table_with_unique_col",
			returnedBuilder: sq.Insert("yet_another_table_with_unique_col").
				Columns("uni_str_col", "int_col", "timestamp_col", "other_string_col"),
			cfgColumns: []ConfigColumn{
				{
					Name:       "uni_str_col",
					ColumnType: "string",
				},
				{
					Name:       "int_col",
					ColumnType: "int",
				},
				{
					Name:       "timestamp_col",
					ColumnType: "timestamp",
				},
				{
					Name:       "other_string_col",
					ColumnType: "string",
				},
			},
		},
		{
			name:  "many unique columns",
			table: "yet_another_table_with_many_unique_cols",
			returnedBuilder: sq.Insert("yet_another_table_with_many_unique_cols").
				Columns("uni_str_col", "int_col", "timestamp_col", "other_string_col", "other_timestamp_col"),
			cfgColumns: []ConfigColumn{
				{
					Name:       "uni_str_col",
					ColumnType: "string",
				},
				{
					Name:       "int_col",
					ColumnType: "int",
				},
				{
					Name:       "timestamp_col",
					ColumnType: "timestamp",
				},
				{
					Name:       "other_string_col",
					ColumnType: "string",
				},
				{
					Name:       "other_timestamp_col",
					ColumnType: "timestamp",
				},
			},
		},
		{
			name:  "all columns unique",
			table: "yet_another_table_with_all_unique_cols",
			returnedBuilder: sq.Insert("yet_another_table_with_all_unique_cols").
				Columns("uni_str_col", "int_col", "timestamp_col", "other_string_col", "other_timestamp_col"),
			cfgColumns: []ConfigColumn{
				{
					Name:       "uni_str_col",
					ColumnType: "string",
				},
				{
					Name:       "int_col",
					ColumnType: "int",
				},
				{
					Name:       "timestamp_col",
					ColumnType: "timestamp",
				},
				{
					Name:       "other_string_col",
					ColumnType: "string",
				},
				{
					Name:       "other_timestamp_col",
					ColumnType: "timestamp",
				},
			},
		},
	}

	for _, tCase := range cases {
		t.Run(tCase.name, func(t *testing.T) {
			queryBuilder, err := NewQueryBuilder(tCase.cfgColumns, tCase.table)
			require.NoError(t, err)
			require.NotNil(t, queryBuilder)

			builder := queryBuilder.(*chQueryBuilder)

			require.Equal(t, tCase.returnedBuilder, builder.queryBuilder)
		})
	}
}

func TestGetClickhouseFields(t *testing.T) {
	columns := []ConfigColumn{
		{
			Name:       "uni_str_col",
			ColumnType: "string",
		},
		{
			Name:       "int_col",
			ColumnType: "int",
		},
	}
	table := "some_table"

	expectedChColumns := []column{
		{
			Name:    columns[0].Name,
			ColType: chString,
		},
		{
			Name:    columns[1].Name,
			ColType: chInt,
		},
	}

	queryBuilder, err := NewQueryBuilder(columns, table)
	require.NoError(t, err)
	require.NotNil(t, queryBuilder)

	chColumns := queryBuilder.GetClickhouseFields()
	require.Equal(t, expectedChColumns, chColumns)
}

func TestGetInsertBuilder(t *testing.T) {
	columns := []ConfigColumn{
		{
			Name:       "uni_str_col",
			ColumnType: "string",
		},
		{
			Name:       "int_col",
			ColumnType: "int",
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
