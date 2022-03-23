package postgres

import (
	"fmt"

	sq "github.com/Masterminds/squirrel"
)

type pgQueryBuilder interface {
	GetPgFields() []column
	GetQueryBuilder() sq.InsertBuilder
	GetPostfix() string
	GetUniqueFields() []string
}

type queryBuilder struct {
	columns      []column
	queryBuilder sq.InsertBuilder
	postfix      string
	uniqueFields []string
}

func newQueryBuilder(cfgColumns []ConfigColumn, table string) (pgQueryBuilder, error) {
	qb := &queryBuilder{}

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
func (qb *queryBuilder) GetPgFields() []column {
	return qb.columns
}

// Returns base builder with with table name and column names.
func (qb *queryBuilder) GetQueryBuilder() sq.InsertBuilder {
	return qb.queryBuilder
}

// Returns postfix: "ON CONFLICT" clause.
func (qb *queryBuilder) GetPostfix() string {
	return qb.postfix
}

func (qb *queryBuilder) GetUniqueFields() []string {
	return qb.uniqueFields
}

func (qb *queryBuilder) initPgFields(cfgColumns []ConfigColumn) ([]column, []string, error) {
	pgColumns := make([]column, 0, len(cfgColumns))
	uniqueFields := []string{}
	for _, col := range cfgColumns {
		var colType pgType
		switch col.ColumnType {
		case colTypeInt:
			colType = pgInt
		case colTypeString:
			colType = pgString
		case colTypeBool:
			colType = pgBool
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

func (qb *queryBuilder) createQuery(pgFields []column, table string) (sq.InsertBuilder, string) {
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
	if len(uniqueFields) > 0 {
		postfix = "ON CONFLICT("
		for i, field := range uniqueFields {
			postfix += field
			if i < len(uniqueFields)-1 {
				postfix += ","
			}
		}
		postfix += ") DO UPDATE SET "
		for i, field := range updateableFields {
			postfix += field + "=EXCLUDED." + field
			if i < len(updateableFields)-1 {
				postfix += ","
			}
		}
	}

	return sq.Insert(table).Columns(fieldsName...), postfix
}
