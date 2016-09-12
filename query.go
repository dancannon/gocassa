package gocassa

import (
	"bytes"
	"strconv"
	"strings"

	"github.com/dancannon/gocassa/encoding"
)

// TODO: Add IF conditions to DELETE/UPDATE
// TODO: Add IF EXISTS to INSERT/UPDATE

type QueryType uint8

const (
	SelectQueryType QueryType = iota
	InsertQueryType
	UpdateQueryType
	DeleteQueryType
)

type QueryGenerator interface {
	GenerateStatement() (stmt string, values []interface{})
	WithOptions(options QueryOptions) QueryGenerator
	Options() QueryOptions
}

type RawQuery struct {
	statement string
	values    []interface{}
	options   QueryOptions
}

func NewRawQuery(stmt string, values []interface{}) RawQuery {
	return RawQuery{
		statement: stmt,
		values:    values,
	}
}

func (q RawQuery) GenerateStatement() (stmt string, values []interface{}) {
	return q.statement, q.values
}

func (q RawQuery) WithOptions(options QueryOptions) QueryGenerator {
	q.options = options
	return q
}

func (q RawQuery) Options() QueryOptions {
	return q.options
}

type Query struct {
	table      *Table
	queryType  QueryType
	relations  []Relation
	selections []Selection
	orderings  []Ordering
	limit      int
	values     map[string]interface{}
	options    QueryOptions
}

func NewQuery(table *Table, queryType QueryType) Query {
	return Query{
		table:     table,
		queryType: queryType,
	}
}

func (q Query) WithOptions(options QueryOptions) QueryGenerator {
	q.options = options
	return q
}

func (q Query) Options() QueryOptions {
	return q.options
}

func (q Query) Where(relations ...Relation) Query {
	q.relations = append(q.relations, relations...)

	return q
}

func (q Query) Fields(selections ...Selection) Query {
	q.selections = append(q.selections, selections...)
	return q
}

func (q Query) OrderBy(orderings ...Ordering) Query {
	q.orderings = append(q.orderings, orderings...)
	return q
}

func (q Query) Limit(limit int) Query {
	q.limit = limit
	return q
}

func (q Query) Values(m map[string]interface{}) Query {
	q.values = m
	return q
}

func (q Query) GenerateStatement() (stmt string, values []interface{}) {
	switch q.queryType {
	case SelectQueryType:
		return q.generateSelectStatement()
	case InsertQueryType:
		return q.generateInsertStatement()
	case UpdateQueryType:
		return q.generateUpdateStatement()
	case DeleteQueryType:
		return q.generateDeleteStatement()
	default:
		return "", nil
	}
}

func (q Query) generateSelectStatement() (string, []interface{}) {

	buf := new(bytes.Buffer)
	values := []interface{}{}

	buf.WriteString("SELECT ")
	if len(q.selections) > 0 {
		values = append(values, q.addSelectionsToStatement(buf)...)
	} else {
		buf.WriteString("*")
	}
	buf.WriteString(" FROM ")
	buf.WriteString(q.table.keyspace.Name())
	buf.WriteString(".")
	buf.WriteString(q.table.Name())
	values = append(values, q.addWhereToStatement(buf)...)
	values = append(values, q.addOrderByToStatement(buf)...)
	values = append(values, q.addLimitToStatement(buf)...)
	if q.options.AllowFiltering {
		buf.WriteString(" ALLOW FILTERING")
	}

	return buf.String(), values
}

func (q Query) generateInsertStatement() (string, []interface{}) {
	buf := new(bytes.Buffer)
	values := []interface{}{}

	buf.WriteString("INSERT INTO ")
	buf.WriteString(q.table.keyspace.Name())
	buf.WriteString(".")
	buf.WriteString(q.table.Name())
	buf.WriteString(" (")
	values = append(values, q.addValueNamesToStatement(buf)...)
	buf.WriteString(") VALUES (")
	values = append(values, q.addValuesToStatement(buf)...)
	buf.WriteString(")")
	values = append(values, q.addOptionsToStatement(buf)...)

	return buf.String(), values
}

func (q Query) generateUpdateStatement() (string, []interface{}) {
	buf := new(bytes.Buffer)
	values := []interface{}{}

	buf.WriteString("UPDATE ")
	buf.WriteString(q.table.keyspace.Name())
	buf.WriteString(".")
	buf.WriteString(q.table.Name())
	buf.WriteString(" SET ")
	values = append(values, q.addAssignmentsToStatement(buf)...)
	values = append(values, q.addWhereToStatement(buf)...)
	values = append(values, q.addOptionsToStatement(buf)...)

	return buf.String(), values
}

func (q Query) generateDeleteStatement() (string, []interface{}) {
	buf := new(bytes.Buffer)
	values := []interface{}{}

	buf.WriteString("DELETE ")
	if len(q.selections) > 0 {
		values = append(values, q.addSelectionsToStatement(buf)...)
		buf.WriteString(" ")
	}
	buf.WriteString("FROM ")
	buf.WriteString(q.table.keyspace.Name())
	buf.WriteString(".")
	buf.WriteString(q.table.Name())
	values = append(values, q.addWhereToStatement(buf)...)

	return buf.String(), values
}

func (q Query) addSelectionsToStatement(buf *bytes.Buffer) []interface{} {
	for i, f := range q.selections {
		if i > 0 {
			buf.WriteString(",")
		}

		buf.WriteString(f.generateCQL())
	}

	return nil
}

func (q Query) addValueNamesToStatement(buf *bytes.Buffer) []interface{} {
	values := []interface{}{}

	// TODO: Sort values if required (for testing)

	i := 0
	for k, v := range q.values {
		if i > 0 {
			buf.WriteString(",")
		}

		buf.WriteString(k)
		values = append(values, v)
		i++
	}

	return values
}

func (q Query) addValuesToStatement(buf *bytes.Buffer) []interface{} {
	i := 0
	for range q.values {
		if i > 0 {
			buf.WriteString(",")
		}
		buf.WriteString("?")
		i++
	}

	return nil
}

func (q Query) addAssignmentsToStatement(buf *bytes.Buffer) []interface{} {
	values := []interface{}{}

	i := 0
	for k, v := range q.values {
		if i > 0 {
			buf.WriteString(",")
		}

		if mod, ok := v.(Modifier); ok {
			stmt, vals := mod.generateCQL(k)
			buf.WriteString(stmt)
			values = append(values, vals...)
		} else {
			buf.WriteString(k + " = ?")
			values = append(values, v)
		}
		i++
	}

	return values
}

func (q Query) addWhereToStatement(buf *bytes.Buffer) []interface{} {
	values := []interface{}{}

	if len(q.relations) > 0 {
		buf.WriteString(" WHERE ")
		for i, r := range q.relations {
			if i > 0 {
				buf.WriteString(" AND ")
			}
			cql, vals := r.generateCQL()
			buf.WriteString(cql)
			if r.relationType == relationTypeIN {
				values = append(values, vals)
			} else {
				values = append(values, vals...)
			}
		}
	}

	return values
}

func (q Query) addOrderByToStatement(buf *bytes.Buffer) []interface{} {
	values := []interface{}{}

	if len(q.options.Orderings) > 0 {
		q.orderings = q.options.Orderings
	}

	if len(q.orderings) > 0 {
		buf.WriteString(" ORDER BY ")
		for i, ordering := range q.orderings {
			if i > 0 {
				buf.WriteString(",")
			}

			buf.WriteString(ordering.Column)
			buf.WriteString(" ")
			buf.WriteString(ordering.Direction.String())
		}
	}

	return values
}

func (q Query) addOptionsToStatement(buf *bytes.Buffer) []interface{} {
	timestamp := q.options.Timestamp.UnixNano() / 1000000
	ttl := int64(q.options.TTL.Seconds())

	if timestamp > 0 && ttl > 0 {
		buf.WriteString(" USING TIMESTAMP ")
		buf.WriteString(strconv.FormatInt(timestamp, 10))
		buf.WriteString(" AND TTL ")
		buf.WriteString(strconv.FormatInt(ttl, 10))
	} else if timestamp > 0 {
		buf.WriteString(" USING TIMESTAMP ")
		buf.WriteString(strconv.FormatInt(timestamp, 10))
	} else if ttl > 0 {
		buf.WriteString(" USING TTL ")
		buf.WriteString(strconv.FormatInt(ttl, 10))
	}

	return nil
}

func (q Query) addLimitToStatement(buf *bytes.Buffer) []interface{} {
	if q.options.Limit > 0 {
		q.limit = q.options.Limit
	}

	if q.limit > 0 {
		buf.WriteString(" LIMIT ?")

		return []interface{}{q.limit}
	}

	return nil
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
