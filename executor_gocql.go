package gocassa

import "github.com/gocql/gocql"

// Connect uses the given gocql cluster configuration to connect to a Cassandra
// cluster using the built-in GoCQL query executor. If you wish to use your
// own query executor then use NewConnection.
func Connect(config *gocql.ClusterConfig) (QueryExecutor, error) {
	session, err := config.CreateSession()
	if err != nil {
		return nil, err
	}

	return NewGoCQLExecutor(session), nil
}

// NewGoCQLExecutor creates a GoCQL query executor using the given GoCQL session
func NewGoCQLExecutor(session *gocql.Session) QueryExecutor {
	return gocqlExecutor{
		session: session,
	}
}

type gocqlExecutor struct {
	session *gocql.Session
}

// Query executes a query and returns the results.
func (qe gocqlExecutor) Query(query QueryGenerator, opts *Options) ([]map[string]interface{}, error) {
	cqlQuery, err := qe.createCQLQuery(query, opts)
	if err != nil {
		return nil, err
	}

	iter := cqlQuery.Iter()
	ret := []map[string]interface{}{}

	m := map[string]interface{}{}
	for iter.MapScan(m) {
		ret = append(ret, m)

		m = map[string]interface{}{}
	}

	return ret, iter.Close()
}

// Query executes a query and returns the results.
func (qe gocqlExecutor) Execute(query QueryGenerator, opts *Options) error {
	cqlQuery, err := qe.createCQLQuery(query, opts)
	if err != nil {
		return err
	}

	return cqlQuery.Exec()
}

func (qe *gocqlExecutor) createCQLQuery(query QueryGenerator, opts *Options) (*gocql.Query, error) {
	stmt, vals, err := query.GenerateStatement()
	if err != nil {
		return nil, err
	}

	cqlQuery := qe.session.Query(stmt, vals...)
	if opts.Consistency != nil {
		cqlQuery = cqlQuery.Consistency(*opts.Consistency)
	}

	return cqlQuery, nil
}

// Close closes the underlying session
func (qe gocqlExecutor) Close() {
	qe.session.Close()
}
