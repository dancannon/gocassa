package gocassa

import (
	"fmt"
	"sort"
	"strings"
)

type TableOptions struct {
	CompactStorage   bool
	ClusteringOrders []ClusteringOrder
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
		primaryKey = fmt.Sprintf("PRIMARY KEY %s", t.partitionKeys[0])
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
	}, nil)
}

// DropStatement returns a CQL which will delete the current table if it
// exists
func (t *Table) DropStatement() string {
	return fmt.Sprintf("DROP TABLE IF EXISTS %s", t.Name())
}

// Drop attempts to delete the current table if it exists
func (t *Table) Drop() error {
	return t.keyspace.QueryExecutor().Execute(RawQuery{
		Statement: t.DropStatement(),
	}, nil)
}

// import (
// 	"bytes"
// 	"fmt"
// 	"reflect"
// 	"strconv"
// 	"strings"

// 	r "github.com/hailocab/gocassa/reflect"
// )

// type t struct {
// 	keySpace *k
// 	info     *tableInfo
// 	options  Options
// }

// // Contains mostly analyzed information about the entity
// type tableInfo struct {
// 	keyspace, name string
// 	marshalSource  interface{}
// 	fieldSource    map[string]interface{}
// 	keys           Keys
// 	fieldNames     map[string]struct{} // This is here only to check containment
// 	fields         []string
// 	fieldValues    []interface{}
// }

// func newTableInfo(keyspace, name string, keys Keys, entity interface{}, fieldSource map[string]interface{}) *tableInfo {
// 	cinf := &tableInfo{
// 		keyspace:      keyspace,
// 		name:          name,
// 		marshalSource: entity,
// 		keys:          keys,
// 		fieldSource:   fieldSource,
// 	}
// 	fields := []string{}
// 	values := []interface{}{}
// 	for k, v := range fieldSource {
// 		fields = append(fields, k)
// 		values = append(values, v)
// 	}
// 	cinf.fieldNames = map[string]struct{}{}
// 	for _, v := range fields {
// 		cinf.fieldNames[v] = struct{}{}
// 	}
// 	cinf.fields = fields
// 	cinf.fieldValues = values
// 	return cinf
// }

// func (t *t) zero() interface{} {
// 	return reflect.New(reflect.TypeOf(t.info.marshalSource)).Interface()
// }

// // Since we cant have Map -> [(k, v)] we settle for Map -> ([k], [v])
// // #tuplelessLifeSucks
// func keyValues(m map[string]interface{}) ([]string, []interface{}) {
// 	keys := []string{}
// 	values := []interface{}{}
// 	for k, v := range m {
// 		keys = append(keys, k)
// 		values = append(values, v)
// 	}
// 	return keys, values
// }

// func toMap(i interface{}) (m map[string]interface{}, ok bool) {
// 	switch v := i.(type) {
// 	case map[string]interface{}:
// 		m, ok = v, true
// 	default:
// 		m, ok = r.StructToMap(i)
// 	}

// 	return
// }

// func (t t) Where(rs ...Relation) Filter {
// 	return filter{
// 		t:  t,
// 		rs: rs,
// 	}
// }

// func (t t) generateFieldNames(sel []string) string {
// 	xs := make([]string, len(t.info.fields))
// 	if len(sel) > 0 {
// 		xs = sel
// 	} else {
// 		for i, v := range t.info.fields {
// 			xs[i] = strings.ToLower(v)
// 		}
// 	}
// 	return strings.Join(xs, ", ")
// }

// func relations(keys Keys, m map[string]interface{}) []Relation {
// 	ret := []Relation{}
// 	for _, v := range append(keys.PartitionKeys, keys.ClusteringColumns...) {
// 		ret = append(ret, Eq(v, m[v]))
// 	}
// 	return ret
// }

// func removeFields(m map[string]interface{}, s []string) map[string]interface{} {
// 	keys := map[string]bool{}
// 	for _, v := range s {
// 		v = strings.ToLower(v)
// 		keys[v] = true
// 	}
// 	ret := map[string]interface{}{}
// 	for k, v := range m {
// 		k = strings.ToLower(k)
// 		if !keys[k] {
// 			ret[k] = v
// 		}
// 	}
// 	return ret
// }

// func transformFields(m map[string]interface{}) {
// 	for k, v := range m {
// 		switch t := v.(type) {
// 		case Counter:
// 			m[k] = CounterIncrement(int(t))
// 		}
// 	}
// }

// // INSERT INTO Hollywood.NerdMovies (user_uuid, fan)
// //   VALUES ('cfd66ccc-d857-4e90-b1e5-df98a3d40cd6', 'johndoe')
// //
// // Gotcha: primkey must be first
// func insertStatement(keySpaceName, cfName string, fieldNames []string, opts Options) string {
// 	placeHolders := make([]string, len(fieldNames))
// 	for i := 0; i < len(fieldNames); i++ {
// 		placeHolders[i] = "?"
// 	}
// 	lowerFieldNames := make([]string, len(fieldNames))
// 	for i, v := range fieldNames {
// 		lowerFieldNames[i] = strings.ToLower(v)
// 	}

// 	buf := new(bytes.Buffer)
// 	buf.WriteString(fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%s)",
// 		keySpaceName,
// 		cfName,
// 		strings.Join(lowerFieldNames, ", "),
// 		strings.Join(placeHolders, ", ")))

// 	// Apply options
// 	if opts.TTL != 0 {
// 		buf.WriteString(" USING TTL ")
// 		buf.WriteString(strconv.FormatFloat(opts.TTL.Seconds(), 'f', 0, 64))
// 	}

// 	return buf.String()
// }

// func (t t) Set(i interface{}) Op {
// 	m, ok := toMap(i)
// 	if !ok {
// 		panic("SetWithOptions: Incompatible type")
// 	}
// 	ks := append(t.info.keys.PartitionKeys, t.info.keys.ClusteringColumns...)
// 	updFields := removeFields(m, ks)
// 	if len(updFields) == 0 {
// 		return newWriteOp(t.keySpace.qe, filter{
// 			t: t,
// 		}, insertOpType, m)
// 	}
// 	transformFields(updFields)
// 	rels := relations(t.info.keys, m)
// 	return newWriteOp(t.keySpace.qe, filter{
// 		t:  t,
// 		rs: rels,
// 	}, updateOpType, updFields)
// }
