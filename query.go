package gocassa

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
}

func (q Query) GenerateStatement() (stmt string, values []interface{}, err error) {
	return "", nil, nil
}
