package gocassa

// A QueryExecutor implements the functions required to execute queries against
// a Cassandra cluster
type QueryExecutor interface {
	// Query executes a query and returns the results.
	Query(qg QueryGenerator, opts Options) ([]map[string]interface{}, error)

	// Execute executes a query and returns without returning any results.
	Execute(qg QueryGenerator, opts Options) error

	// Close closes the underlying session
	Close()
}
