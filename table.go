package gocassa

import (
	"fmt"
	"sort"
	"strings"

	"github.com/davecgh/go-spew/spew"
)

type TableOptions struct {
	CompactStorage   bool
	ClusteringOrders []ClusteringOrder
	Comment          string
}

// The Table type is the lowest level type included in the package and allows any
// type of table to be created using any combination of partition and clustering
// keys. Generally you will not need to use this type however if you need more
// control over your data structures or are creating a new table type then this
// type should be suitable.
type Table struct {
	keyspace          *Keyspace
	name              string
	partitionKeys     []string
	clusteringColumns []string
	documentValue     interface{}
	documentFields    []tableField
	options           TableOptions
}

// NewTable creates a new table with the keys and fields specified, see the Table
// type definition for more information.
func NewTable(
	keyspace *Keyspace,
	name string,
	documentValue interface{},
	partitionKeys, clusteringColumns []string,
	options *TableOptions,
) *Table {
	if options == nil {
		options = &TableOptions{}
	}

	// Convert all keys to lower case
	for i, k := range partitionKeys {
		partitionKeys[i] = strings.ToLower(k)
	}
	for i, k := range clusteringColumns {
		clusteringColumns[i] = strings.ToLower(k)
	}

	return &Table{
		keyspace:          keyspace,
		name:              name,
		partitionKeys:     partitionKeys,
		clusteringColumns: clusteringColumns,
		documentValue:     documentValue,
		documentFields:    documentFields(documentValue),
		options:           *options,
	}
}

func (t *Table) Name() string {
	return t.name
}

// CreateStatement returns a CQL which will create the current table if it
// does not already exist.
func (t *Table) CreateStatement() string {
	// Build columns
	columns := make([]string, len(t.documentFields))
	for i, field := range t.documentFields {
		columns[i] = fmt.Sprintf("%s %s", field.name, field.typeName)
	}

	// Build primary key
	primaryKey := ""
	if len(t.partitionKeys) > 1 && len(t.clusteringColumns) > 0 {
		primaryKey = fmt.Sprintf("PRIMARY KEY ((%s),%s)", strings.Join(t.partitionKeys, ","), strings.Join(t.clusteringColumns, ","))
	} else if len(t.partitionKeys) == 1 && len(t.clusteringColumns) > 0 {
		primaryKey = fmt.Sprintf("PRIMARY KEY (%s,%s)", t.partitionKeys[0], strings.Join(t.clusteringColumns, ","))
	} else if len(t.partitionKeys) > 1 && len(t.clusteringColumns) == 0 {
		primaryKey = fmt.Sprintf("PRIMARY KEY ((%s))", strings.Join(t.partitionKeys, ","))
	} else if len(t.partitionKeys) == 1 && len(t.clusteringColumns) == 0 {
		primaryKey = fmt.Sprintf("PRIMARY KEY (%s)", t.partitionKeys[0])
	}

	// Add primary key to column definitions and join together
	columnDefinitions := strings.Join(append(columns, primaryKey), ",")

	// Build properties
	properties := []string{}
	if t.options.CompactStorage {
		properties = append(properties, "COMPACT STORAGE")
	}
	if len(t.options.ClusteringOrders) > 0 {
		orders := []string{}
		for _, order := range t.options.ClusteringOrders {
			orders = append(orders, fmt.Sprintf("%s %s", order.Column, order.Direction))
		}
		sort.Strings(orders)
		properties = append(properties, fmt.Sprintf("CLUSTERING ORDER (%s)", strings.Join(orders, ",")))
	}
	if t.options.Comment != "" {
		properties = append(properties, fmt.Sprintf("comment = '%v'", t.options.Comment))
	}

	stmt := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s.%s (%s)",
		t.keyspace.Name(), t.Name(),
		columnDefinitions,
	)

	if len(properties) > 0 {
		stmt = fmt.Sprintf("%s WITH %s", stmt, strings.Join(properties, " AND "))
	}

	return stmt
}

// Create attempts to create the current table if it does not already exist.
func (t *Table) Create() error {
	return t.keyspace.QueryExecutor().Execute(RawQuery{
		Statement: t.CreateStatement(),
	}, Options{})
}

// DropStatement returns a CQL which will delete the current table if it
// exists
func (t *Table) DropStatement() string {
	return fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", t.keyspace.Name(), t.Name())
}

// Drop attempts to delete the current table if it exists
func (t *Table) Drop() error {
	return t.keyspace.QueryExecutor().Execute(RawQuery{
		Statement: t.DropStatement(),
	}, Options{})
}

func (t *Table) Set(v interface{}) error {
	fields := transformFields(toMap(v))
	updateFields := removeFields(fields, append(t.partitionKeys, t.clusteringColumns...))

	var q Query
	if len(updateFields) == 0 {
		q = NewQuery(t, InsertQueryType).SetValues(fields)
	} else {
		q = NewQuery(t, UpdateQueryType).SetValues(updateFields)
		spew.Dump(fields)
		for _, k := range append(t.partitionKeys, t.clusteringColumns...) {
			spew.Dump(k)
			q = q.Where(Eq(k, fields[k]))
		}
	}

	return t.keyspace.QueryExecutor().Execute(q, Options{})
}
