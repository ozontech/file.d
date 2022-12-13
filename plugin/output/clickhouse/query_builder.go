package clickhouse

import (
	"errors"
	"fmt"
	"strings"

	sq "github.com/Masterminds/squirrel"
)

var ErrNoColumns = errors.New("no pg columns in config")
var ErrEmptyTableName = errors.New("table name can't be empty string")

type column struct {
	Name    string
	ColType pgType
	Unique  bool
}

type PgQueryBuilder interface {
	GetPgFields() []column
	GetUniqueFields() map[string]pgType
	GetInsertBuilder() sq.InsertBuilder
	GetPostfix() string
}

const (
	doNothingPostfix = "ON CONFLICT (%s) DO NOTHING"
	doUpdatePostfix  = "ON CONFLICT(%s) DO UPDATE SET %s"
)

type pgQueryBuilder struct {
	fields       []column
	uniqFields   map[string]pgType
	queryBuilder sq.InsertBuilder
	postfix      string
}

// NewQueryBuilder returns new instance of builder.
func NewQueryBuilder(cfgColumns []ConfigColumn, table string) (PgQueryBuilder, error) {
	qb := &pgQueryBuilder{}

	if len(cfgColumns) == 0 {
		return nil, ErrNoColumns
	}
	if table == "" {
		return nil, ErrEmptyTableName
	}

	pgFields, uniqueColumns, err := qb.initPgFields(cfgColumns)
	qb.fields = pgFields
	if err != nil {
		return nil, err
	}
	qb.uniqFields = uniqueColumns
	query, postfix := qb.createQuery(pgFields, table)
	qb.queryBuilder = query
	qb.postfix = postfix

	return qb, nil
}

// GetPgFields returns actucal pg columns.
func (qb *pgQueryBuilder) GetPgFields() []column {
	return qb.fields
}

// GetInsertBuilder returns base builder with with table name and column names.
func (qb *pgQueryBuilder) GetInsertBuilder() sq.InsertBuilder {
	return qb.queryBuilder
}

// GetPostfix returns postfix: "ON CONFLICT" clause.
func (qb *pgQueryBuilder) GetPostfix() string {
	return qb.postfix
}

// GetUniqueFields returns unique fields.
func (qb *pgQueryBuilder) GetUniqueFields() map[string]pgType {
	return qb.uniqFields
}

func (qb *pgQueryBuilder) initPgFields(cfgColumns []ConfigColumn) ([]column, map[string]pgType, error) {
	pgFields := make([]column, 0, len(cfgColumns))
	uniqFields := make(map[string]pgType)
	for _, col := range cfgColumns {
		var colType pgType
		switch col.ColumnType {
		case colTypeInt:
			colType = pgInt
		case colTypeString:
			colType = pgString
		case colTypeTimestamp:
			colType = pgTimestamp
		default:
			return nil, nil, fmt.Errorf("invalid pg type: %v", col.ColumnType)
		}

		pgFields = append(pgFields, column{
			Name:    col.Name,
			ColType: colType,
			Unique:  col.Unique,
		})
		if col.Unique {
			uniqFields[col.Name] = colType
		}
	}

	return pgFields, uniqFields, nil
}

func (qb *pgQueryBuilder) createQuery(pgFields []column, table string) (sq.InsertBuilder, string) {
	postfix := ""
	uniqFields := []string{}
	updateableFields := make([]string, 0, len(pgFields))
	fieldsName := make([]string, 0, len(pgFields))
	for _, field := range pgFields {
		fieldsName = append(fieldsName, field.Name)
		if field.Unique {
			uniqFields = append(uniqFields, field.Name)
		} else {
			updateableFields = append(updateableFields, field.Name)
		}
	}
	if len(uniqFields) > 0 && len(updateableFields) > 0 {
		// ON CONFLICT (col1unique, col3unique) DO UPDATE SET col1updateable=EXCLUDED.col1updateable
		updatePostfix := make([]string, 0, len(updateableFields))

		uniquePostfixString := strings.Join(uniqFields, ",")

		for _, field := range updateableFields {
			updatePostfix = append(updatePostfix, field+"=EXCLUDED."+field)
		}
		updatePostfixString := strings.Join(updatePostfix, ",")

		postfix = fmt.Sprintf(doUpdatePostfix, uniquePostfixString, updatePostfixString)
	} else if len(uniqFields) > 0 {
		// ON CONFLICT (col1, col2, col3) DO NOTHING
		uniquePostfixString := strings.Join(uniqFields, ",")
		postfix = fmt.Sprintf(doNothingPostfix, uniquePostfixString)
	}

	return sq.Insert(table).Columns(fieldsName...), postfix
}
