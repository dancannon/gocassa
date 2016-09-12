package gocassa

import (
	"math/big"
	"reflect"

	"github.com/dancannon/gocassa/encoding"
	"github.com/mitchellh/mapstructure"
)

type RunnableQuery struct {
	Executor QueryExecutor
	Query    QueryGenerator
}

func (q RunnableQuery) WithOptions(options QueryOptions) RunnableQuery {
	q.Query = q.Query.WithOptions(options)
	return q
}

// MapScan executes the query, copies the columns of the first selected
// row into the map pointed at by dest and discards the rest.
func (q RunnableQuery) ScanOne(dest interface{}) error {
	v, err := q.Executor.QueryOne(q.Query)
	if err != nil {
		return err
	}

	return decodeResult(v, dest)
}

// ScanCAS executes a lightweight transaction (i.e. an UPDATE or INSERT
// statement containing an IF clause). If the transaction fails because
// the existing values did not match, the previous values will be stored
// in dest.
func (q RunnableQuery) ScanCAS(dest interface{}) (applied bool, err error) {
	v, applied, err := q.Executor.QueryCAS(q.Query)
	if err != nil {
		return applied, err
	}

	return applied, decodeResult(v, dest)
}

// SliceMap is a helper function to make the API easier to use returns the data
//  from the query in the form of []map[string]interface{}
//
// MapScan executes the query, copies the columns of the each row into the slice
// of maps pointed at by m and discards the rest.
func (q RunnableQuery) Scan(dest interface{}) error {
	v, err := q.Executor.Query(q.Query)
	if err != nil {
		return err
	}

	return decodeResult(v, dest)
}

// NumRows returns the number of rows in this pagination, it will update when new
// pages are fetched, it is not the value of the total number of rows this iter
// will return unless there is only a single page returned.
func (q RunnableQuery) Iter() Iter {
	return q.Executor.Iter(q.Query)
}

func (q RunnableQuery) Execute() error {
	return q.Executor.Execute(q.Query)
}

type Iter interface {
	// Scan consumes the next row of the iterator and copies the columns of the
	// current row into the values pointed at by dest. Use nil as a dest value
	// to skip the corresponding column. Scan might send additional queries
	// to the database to retrieve the next set of rows if paging was enabled.
	//
	// Scan returns true if the row was successfully unmarshaled or false if the
	// end of the result set was reached or if an error occurred. Close should
	// be called afterwards to retrieve any potential errors.
	Scan(dest interface{}) bool

	// NumRows returns the number of rows in this pagination, it will update when new
	// pages are fetched, it is not the value of the total number of rows this iter
	// will return unless there is only a single page returned.
	NumRows() int

	// WillSwitchPage detects if iterator reached end of current page
	// and the next page is available.
	WillSwitchPage() bool

	// GetCustomPayload returns any parsed custom payload results if given in the
	// response from Cassandra. Note that the result is not a copy.
	GetCustomPayload() map[string][]byte
	// Close closes the iterator and returns any errors that happened during the
	// query or the iteration.
	Close() error
}

func decodeResult(v, dest interface{}) error {
	dec, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		ZeroFields:       true,
		WeaklyTypedInput: true,
		Result:           dest,
		TagName:          encoding.TagName,
		DecodeHook: mapstructure.ComposeDecodeHookFunc(
			decodeBigIntHook,
		),
	})
	if err != nil {
		return err
	}

	return dec.Decode(v)
}

func decodeBigIntHook(f reflect.Kind, t reflect.Kind, data interface{}) (interface{}, error) {
	if f != reflect.Ptr {
		return data, nil
	}

	if i, ok := data.(*big.Int); ok {
		switch t {
		case reflect.Uint64:
			return i.Uint64(), nil
		case reflect.Uint32:
			return uint32(i.Uint64()), nil
		case reflect.Uint16:
			return uint16(i.Uint64()), nil
		case reflect.Uint8:
			return uint8(i.Uint64()), nil
		case reflect.Uint:
			return uint(i.Uint64()), nil
		case reflect.Int16:
			return int16(i.Int64()), nil
		case reflect.Int8:
			return int8(i.Int64()), nil
		}
	}

	return data, nil
}
