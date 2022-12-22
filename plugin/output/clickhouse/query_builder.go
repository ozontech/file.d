package clickhouse

import (
	"errors"
	"fmt"

	sq "github.com/Masterminds/squirrel"
)

var ErrNoColumns = errors.New("no pg columns in config")
var ErrEmptyTableName = errors.New("table name can't be empty string")

type column struct {
	Name    string
	ColType chType
}

type ClickhouseQueryBuilder interface {
	GetClickhouseFields() []column
	GetInsertBuilder() sq.InsertBuilder
}

type chQueryBuilder struct {
	fields       []column
	queryBuilder sq.InsertBuilder
}

// NewQueryBuilder returns new instance of builder.
func NewQueryBuilder(cfgColumns []ConfigColumn, table string) (ClickhouseQueryBuilder, error) {
	qb := &chQueryBuilder{}

	if len(cfgColumns) == 0 {
		return nil, ErrNoColumns
	}
	if table == "" {
		return nil, ErrEmptyTableName
	}

	chFields, err := qb.initClickhouseFields(cfgColumns)
	qb.fields = chFields
	if err != nil {
		return nil, err
	}
	query := qb.createQuery(chFields, table)
	qb.queryBuilder = query

	return qb, nil
}

// GetClickhouseFields returns actucal ch columns.
func (qb *chQueryBuilder) GetClickhouseFields() []column {
	return qb.fields
}

// GetInsertBuilder returns base builder with with table name and column names.
func (qb *chQueryBuilder) GetInsertBuilder() sq.InsertBuilder {
	return qb.queryBuilder
}

func (qb *chQueryBuilder) initClickhouseFields(cfgColumns []ConfigColumn) ([]column, error) {
	chFields := make([]column, 0, len(cfgColumns))
	for _, col := range cfgColumns {
		var colType chType
		switch col.ColumnType {
		case colTypeInt:
			colType = chInt
		case colTypeString:
			colType = chString
		case colTypeTimestamp:
			colType = chTimestamp
		case colTypeTimestring:
			colType = chTimestring
		default:
			return nil, fmt.Errorf("invalid ch type: %v", col.ColumnType)
		}

		chFields = append(chFields, column{
			Name:    col.Name,
			ColType: colType,
		})
	}

	return chFields, nil
}

func (qb *chQueryBuilder) createQuery(chFields []column, table string) (sq.InsertBuilder) {
	fieldsName := make([]string, 0, len(chFields))
	for _, field := range chFields {
		fieldsName = append(fieldsName, field.Name)
	}

	return sq.Insert(table).Columns(fieldsName...)
}
