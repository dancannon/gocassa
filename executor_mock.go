package gocassa

import (
	"sync"

	"github.com/stretchr/testify/mock"
)

// NewMockExecutor creates a mock query executor using the given mocker, when
// queries are executed the generated CQL statement and values are passed to
// the mocker which allows tests to verify that the correct statement and
// values are being executed and return the correct response.
//
// When setting up the expectations pass the expected CQL and bound parameters
// to mock.On(...) and the results as either map[string]interface{} or []map[string]interface{}
// depending on the function being mocked.
//
// 	mock.On("Query", "SELECT * FROM ... WHERE id = ?", []interface{}{"1"}).Return(
// 		[]map[string]interface{}{
// 			map[string]interface{}{"id": "1", "name": "John Smith"},
// 		},
// 	)
//
// When mocking the Iter function pass the results you wish to iterate over as
// a slice of maps ([]map[string]interface{}) and the executor will create the
// iterator for you.
func NewMockExecutor(m mock.Mock) QueryExecutor {
	return mockExecutor{
		mock: m,
	}
}

type mockExecutor struct {
	mock mock.Mock
}

func (qe mockExecutor) QueryOne(query QueryGenerator, options QueryOptions) (map[string]interface{}, error) {
	ret := qe.mock.Called(query.GenerateStatement(options))

	return ret.Get(0).(map[string]interface{}), ret.Error(1)
}

func (qe mockExecutor) QueryCAS(query QueryGenerator, options QueryOptions) (result map[string]interface{}, applied bool, err error) {
	ret := qe.mock.Called(query.GenerateStatement(options))

	return ret.Get(0).(map[string]interface{}), ret.Bool(1), ret.Error(2)
}

func (qe mockExecutor) Query(query QueryGenerator, options QueryOptions) ([]map[string]interface{}, error) {
	ret := qe.mock.Called(query.GenerateStatement(options))

	return ret.Get(0).([]map[string]interface{}), ret.Error(1)
}

func (qe mockExecutor) Iter(query QueryGenerator, options QueryOptions) Iter {
	ret := qe.mock.Called(query.GenerateStatement(options))

	return mockIter{
		rows: ret.Get(0).([]map[string]interface{}),
		err:  ret.Error(1),
	}
}

func (qe mockExecutor) Execute(query QueryGenerator, options QueryOptions) error {
	ret := qe.mock.Called(query.GenerateStatement(options))

	return ret.Error(0)
}

func (qe mockExecutor) ExecuteBatch(queries []QueryGenerator, options QueryOptions) error {
	stmts := []string{}
	values := [][]interface{}{}
	for _, query := range queries {
		stmt, vals := query.GenerateStatement(options)
		stmts = append(stmts, stmt)
		values = append(values, vals)
	}

	ret := qe.mock.Called(stmts, values)

	return ret.Error(0)
}

func (qe mockExecutor) ExecuteBatchCAS(queries []QueryGenerator, options QueryOptions) (result map[string]interface{}, iter Iter, applied bool, err error) {
	stmts := []string{}
	values := [][]interface{}{}
	for _, query := range queries {
		stmt, vals := query.GenerateStatement(options)
		stmts = append(stmts, stmt)
		values = append(values, vals)
	}

	ret := qe.mock.Called(stmts, values)
	rows := ret.Get(0).([]map[string]interface{})

	return rows[0], mockIter{
		rows: rows[1:],
	}, ret.Bool(1), ret.Error(2)
}

// Close closes the underlying session
func (qe mockExecutor) Close() {
	qe.mock.Called()
}

type mockIter struct {
	mtx  sync.Mutex
	rows []map[string]interface{}
	err  error
}

func (iter mockIter) Scan(dest interface{}) bool {
	iter.mtx.Lock()
	defer iter.mtx.Unlock()

	if iter.err != nil {
		return false
	}

	if err := decodeResult(iter.rows, dest); err != nil {
		iter.err = err

		return false
	}

	return true
}

func (iter mockIter) NumRows() int {
	iter.mtx.Lock()
	defer iter.mtx.Unlock()

	return len(iter.rows)
}

func (iter mockIter) WillSwitchPage() bool {
	return false
}

func (iter mockIter) GetCustomPayload() map[string][]byte {
	return nil
}

func (iter mockIter) Close() error {
	iter.mtx.Lock()
	defer iter.mtx.Unlock()

	return iter.err
}
