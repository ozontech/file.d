package postgres

import (
	"errors"
	"fmt"
	"strings"

	sq "github.com/Masterminds/squirrel"
)

var ErrNoColumns = errors.New("no pg columns in config")
var ErrEmptyTableName = errors.New("table name can't be empty string")

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
	uniqueFields map[string]pgType
	queryBuilder sq.InsertBuilder
	postfix      string
}

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
	qb.uniqueFields = uniqueColumns
	query, postfix := qb.createQuery(pgFields, table)
	qb.queryBuilder = query
	qb.postfix = postfix

	return qb, nil
}

// Returns actucal pg columns.
func (qb *pgQueryBuilder) GetPgFields() []column {
	return qb.fields
}

// Returns base builder with with table name and column names.
func (qb *pgQueryBuilder) GetInsertBuilder() sq.InsertBuilder {
	return qb.queryBuilder
}

// Returns postfix: "ON CONFLICT" clause.
func (qb *pgQueryBuilder) GetPostfix() string {
	return qb.postfix
}

func (qb *pgQueryBuilder) GetUniqueFields() map[string]pgType {
	return qb.uniqueFields
}

func (qb *pgQueryBuilder) initPgFields(cfgColumns []ConfigColumn) ([]column, map[string]pgType, error) {
	pgFields := make([]column, 0, len(cfgColumns))
	uniqueFields := make(map[string]pgType)
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
			uniqueFields[col.Name] = colType
		}
	}

	return pgFields, uniqueFields, nil
}

func (qb *pgQueryBuilder) createQuery(pgFields []column, table string) (sq.InsertBuilder, string) {
	postfix := ""
	uniqueFields := []string{}
	updateableFields := []string{}
	fieldsName := []string{}
	for _, field := range pgFields {
		fieldsName = append(fieldsName, field.Name)
		if field.Unique {
			uniqueFields = append(uniqueFields, field.Name)
		} else {
			updateableFields = append(updateableFields, field.Name)
		}
	}
	if len(uniqueFields) > 0 && len(updateableFields) > 0 {
		// ON CONFLICT (col1unique, 6tgyu, col3unique) DO UPDATE SET col1updateable=EXCLUDED.col1updateable
		uniquePostfix := make([]string, 0, len(uniqueFields))
		updatePostfix := make([]string, 0, len(updateableFields))

		uniquePostfix = append(uniquePostfix, uniqueFields...)
		uniquePostfixString := strings.Join(uniquePostfix, ",")

		for _, field := range updateableFields {
			updatePostfix = append(updatePostfix, field+"=EXCLUDED."+field)
		}
		updatePostfixString := strings.Join(updatePostfix, ",")

		postfix = fmt.Sprintf(doUpdatePostfix, uniquePostfixString, updatePostfixString)
	} else if len(uniqueFields) > 0 {
		// ON CONFLICT (col1, col2, col3) DO NOTHING
		uniquePostfix := make([]string, 0, len(uniqueFields))
		uniquePostfix = append(uniquePostfix, uniqueFields...)
		uniquePostfixString := strings.Join(uniquePostfix, ",")
		postfix = fmt.Sprintf(doNothingPostfix, uniquePostfixString)
	}

	return sq.Insert(table).Columns(fieldsName...), postfix
}
