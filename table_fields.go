package gocassa

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/dancannon/gocassa/encoding"
	"github.com/gocql/gocql"
)

type tableField struct {
	name      string
	fieldType reflect.Type
	cqlType   gocql.Type
	typeName  string
}

// byName sorts tableField by name
type byName []tableField

func (x byName) Len() int { return len(x) }

func (x byName) Swap(i, j int) { x[i], x[j] = x[j], x[i] }

func (x byName) Less(i, j int) bool {
	return x[i].name < x[j].name
}

func documentFields(v interface{}) []tableField {
	var m map[string]interface{}

	switch v := v.(type) {
	case map[string]interface{}:
		m = v
	default:
		m = encoding.StructToMap(v)
	}

	tableFields := make([]tableField, 0, len(m))
	for k, v := range m {
		fieldType := reflect.TypeOf(v)
		tableFields = append(tableFields, tableField{
			name:      strings.ToLower(k),
			fieldType: fieldType,
			cqlType:   cqlType(v),
			typeName:  typeName(v, fieldType),
		})
	}

	// Ensure resulting fields slice is sorted
	sort.Sort(byName(tableFields))

	return tableFields
}

func typeName(v interface{}, t reflect.Type) string {
	isByteSlice := t.Kind() == reflect.Slice && t.Elem().Kind() == reflect.Uint8
	if !isByteSlice {
		// Check if we found a higher kinded type
		switch t.Kind() {
		case reflect.Slice:
			elemVal := reflect.Indirect(reflect.New(t.Elem())).Interface()

			return fmt.Sprintf("list<%v>", cqlType(elemVal))
		case reflect.Map:
			keyVal := reflect.Indirect(reflect.New(t.Key())).Interface()
			elemVal := reflect.Indirect(reflect.New(t.Elem())).Interface()

			return fmt.Sprintf("map<%s, %s>", cqlType(keyVal), cqlType(elemVal))
		}
	}

	return cqlType(v).String()
}

func cqlType(v interface{}) gocql.Type {
	switch v.(type) {
	case int, int32:
		return gocql.TypeInt
	case int64:
		return gocql.TypeBigInt
	case int8, int16, uint, uint8, uint16, uint32, uint64:
		return gocql.TypeVarint
	case string:
		return gocql.TypeVarchar
	case float32:
		return gocql.TypeFloat
	case float64:
		return gocql.TypeDouble
	case bool:
		return gocql.TypeBoolean
	case time.Time:
		return gocql.TypeTimestamp
	case gocql.UUID:
		return gocql.TypeUUID
	case []byte:
		return gocql.TypeBlob
	case Counter:
		return gocql.TypeCounter
	}

	// Fallback to using reflection if type not recognised
	typ := reflect.TypeOf(v)
	switch typ.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32:
		return gocql.TypeInt
	case reflect.Int64:
		return gocql.TypeBigInt
	case reflect.String:
		return gocql.TypeVarchar
	case reflect.Float32:
		return gocql.TypeFloat
	case reflect.Float64:
		return gocql.TypeDouble
	case reflect.Bool:
		return gocql.TypeBoolean
	}

	return gocql.TypeCustom
}
