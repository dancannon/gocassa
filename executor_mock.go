package gocassa

import "github.com/stretchr/testify/mock"

// NewMockExecutor creates a mock query executor using the given mocker, when
// queries are executed the generated CQL statement and values are passed to
// the mocker which allows tests to verify that the correct statement and
// values are being executed and return the correct response.
func NewMockExecutor(m mock.Mock) QueryExecutor {
	return mockExecutor{
		mock: m,
	}
}

type mockExecutor struct {
	mock mock.Mock
}

// Query executes a query and returns the results.
func (qe mockExecutor) Query(query QueryGenerator, opts Options) ([]map[string]interface{}, error) {
	stmt, vals, err := query.GenerateStatement()
	if err != nil {
		return nil, err
	}

	ret := qe.mock.Called(stmt, vals)

	return ret.Get(0).([]map[string]interface{}), ret.Error(1)
}

// Query executes a query and returns the results.
func (qe mockExecutor) Execute(query QueryGenerator, opts Options) error {
	stmt, vals, err := query.GenerateStatement()
	if err != nil {
		return err
	}

	ret := qe.mock.Called(stmt, vals)

	return ret.Error(0)
}

// Close closes the underlying session
func (qe mockExecutor) Close() {
	qe.mock.Called()
}
