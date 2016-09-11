package encoding

import "reflect"

// StructToMap converts a struct to map. The object's default key string
// is the struct field name but can be specified in the struct field's
// tag value. The "cql" key in the struct field's tag value is the key
// name. Examples:
//
//   // Field appears in the resulting map as key "myName".
//   Field int `cql:"myName"`
//
//   // Field appears in the resulting as key "Field"
//   Field int
//
//   // Field appears in the resulting map as key "myName"
//   Field int "myName"
func StructToMap(val interface{}) map[string]interface{} {
	// indirect so function works with both structs and pointers to them
	structVal := reflect.Indirect(reflect.ValueOf(val))
	kind := structVal.Kind()
	if kind != reflect.Struct {
		return nil
	}
	structFields := cachedTypeFields(structVal.Type())
	mapVal := make(map[string]interface{}, len(structFields))
	for _, info := range structFields {
		field := fieldByIndex(structVal, info.index)
		mapVal[info.name] = field.Interface()
	}
	return mapVal
}

// MapToStruct converts a map to a struct. It is the inverse of the StructToMap
// function. For details see StructToMap.
func MapToStruct(m map[string]interface{}, struc interface{}) error {
	val := reflect.Indirect(reflect.ValueOf(struc))
	structFields := cachedTypeFields(val.Type())

	// Create fields map for faster lookup
	fieldsMap := make(map[string]field)
	for _, field := range structFields {
		fieldsMap[field.name] = field
	}

	for k, v := range m {
		if info, ok := fieldsMap[k]; ok {
			structField := fieldByIndex(val, info.index)
			if structField.Type().Name() == reflect.TypeOf(v).Name() {
				structField.Set(reflect.ValueOf(v))
			}
		}
	}
	return nil
}

// FieldsAndValues returns a list field names and a corresponing list of values
// for the given struct. For details on how the field names are determined please
// see StructToMap.
func FieldsAndValues(val interface{}) ([]string, []interface{}, bool) {
	// indirect so function works with both structs and pointers to them
	structVal := reflect.Indirect(reflect.ValueOf(val))
	kind := structVal.Kind()
	if kind != reflect.Struct {
		return nil, nil, false
	}
	structFields := cachedTypeFields(structVal.Type())
	fields := make([]string, len(structFields))
	values := make([]interface{}, len(structFields))
	for i, info := range structFields {
		field := fieldByIndex(structVal, info.index)
		fields[i] = info.name
		values[i] = field.Interface()
	}
	return fields, values, true
}

func fieldByIndex(v reflect.Value, index []int) reflect.Value {
	for _, i := range index {
		if v.Kind() == reflect.Ptr {
			if v.IsNil() {
				if v.CanSet() {
					v.Set(reflect.New(v.Type().Elem()))
				} else {
					return reflect.Value{}
				}
			}
			v = v.Elem()
		}
		v = v.Field(i)
	}

	return v
}
