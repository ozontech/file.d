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
	GetInsertBuilder() sq.InsertBuilder
	GetPostfix() string
	GetUniqueFields() []string
}

const (
	doNothingPostfix = "ON CONFLICT (%s) DO NOTHING"
)

type pgQueryBuilder struct {
	columns      []column
	queryBuilder sq.InsertBuilder
	postfix      string
	uniqueFields []string
}

func NewQueryBuilder(cfgColumns []ConfigColumn, table string) (PgQueryBuilder, error) {
	qb := &pgQueryBuilder{}

	if len(cfgColumns) == 0 {
		return nil, ErrNoColumns
	}
	if table == "" {
		return nil, ErrEmptyTableName
	}

	pgFields, uniqueFields, err := qb.initPgFields(cfgColumns)
	qb.columns = pgFields
	if err != nil {
		return nil, err
	}
	qb.uniqueFields = uniqueFields
	query, postfix := qb.createQuery(pgFields, table)
	qb.queryBuilder = query
	qb.postfix = postfix

	return qb, nil
}

// Returns actucal pg columns.
func (qb *pgQueryBuilder) GetPgFields() []column {
	return qb.columns
}

// Returns base builder with with table name and column names.
func (qb *pgQueryBuilder) GetInsertBuilder() sq.InsertBuilder {
	return qb.queryBuilder
}

// Returns postfix: "ON CONFLICT" clause.
func (qb *pgQueryBuilder) GetPostfix() string {
	return qb.postfix
}

func (qb *pgQueryBuilder) GetUniqueFields() []string {
	return qb.uniqueFields
}

func (qb *pgQueryBuilder) initPgFields(cfgColumns []ConfigColumn) ([]column, []string, error) {
	pgColumns := make([]column, 0, len(cfgColumns))
	uniqueFields := []string{}
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

		pgColumns = append(pgColumns, column{
			Name:    col.Name,
			ColType: colType,
			Unique:  col.Unique,
		})
		if col.Unique {
			uniqueFields = append(uniqueFields, col.Name)
		}
	}

	return pgColumns, uniqueFields, nil
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

		postfix += "ON CONFLICT(" + uniquePostfixString + ") DO UPDATE SET " + updatePostfixString
	} else if len(uniqueFields) > 0 {
		// ON CONFLICT (col1, col2, col3) DO NOTHING
		uniquePostfix := make([]string, 0, len(uniqueFields))
		uniquePostfix = append(uniquePostfix, uniqueFields...)
		uniquePostfixString := strings.Join(uniquePostfix, ",")
		postfix = fmt.Sprintf(doNothingPostfix, uniquePostfixString)
	}

	return sq.Insert(table).Columns(fieldsName...), postfix
}
