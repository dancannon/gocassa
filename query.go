package gocassa

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/dancannon/gocassa/encoding"
)

type QueryType uint8

const (
	ReadQueryType QueryType = iota
	DeleteQueryType
	UpdateQueryType
	InsertQueryType
)

type QueryGenerator interface {
	GenerateStatement() (stmt string, values []interface{}, err error)
}

type RawQuery struct {
	Statement string
	Values    []interface{}
}

func (q RawQuery) GenerateStatement() (stmt string, values []interface{}, err error) {
	return q.Statement, q.Values, nil
}

type Query struct {
	table           *Table
	queryType       QueryType
	rowSpecificaton []Relation
	values          map[string]interface{}
}

func NewQuery(table *Table, queryType QueryType) Query {
	return Query{
		table:     table,
		queryType: queryType,
	}
}

func (q Query) Where(filter Relation) Query {
	q.rowSpecificaton = append(q.rowSpecificaton, filter)

	return q
}

func (q Query) SetValues(m map[string]interface{}) Query {
	q.values = m
	return q
}

func (q Query) GenerateStatement() (stmt string, values []interface{}, err error) {
	switch q.queryType {
	case UpdateQueryType:
		buf := new(bytes.Buffer)
		buf.WriteString(fmt.Sprintf("UPDATE %s.%s ", q.table.keyspace.Name(), q.table.Name()))

		buf.WriteString("SET ")
		i := 0
		for k, v := range q.values {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(k + " = ?")
			values = append(values, v)
			i++
		}

		if len(q.rowSpecificaton) > 0 {
			buf.WriteString(" WHERE ")
			for i, r := range q.rowSpecificaton {
				if i > 0 {
					buf.WriteString(" AND ")
				}
				s, v := r.cql()
				buf.WriteString(s)
				if r.op == in {
					values = append(values, v)
					continue
				}
				values = append(values, v...)
			}
		}

		return buf.String(), values, nil
	default:
		return "", nil, fmt.Errorf("Unsupported query type %v", q.queryType)
	}
}

func toMap(v interface{}) map[string]interface{} {
	var m map[string]interface{}
	switch v := v.(type) {
	case map[string]interface{}:
		m = v
	default:
		m = encoding.StructToMap(v)
	}

	fields := make(map[string]interface{}, len(m))
	for k, v := range m {
		fields[strings.ToLower(k)] = v
	}

	return fields
}

func removeFields(m map[string]interface{}, s []string) map[string]interface{} {
	keys := map[string]bool{}
	for _, v := range s {
		keys[v] = true
	}
	ret := map[string]interface{}{}
	for k, v := range m {
		if !keys[k] {
			ret[k] = v
		}
	}
	return ret
}

func transformFields(m map[string]interface{}) map[string]interface{} {
	return m
}
