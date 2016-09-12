package gocassa

// A QueryExecutor implements the functions required to execute queries against
// a Cassandra cluster
type QueryExecutor interface {
	// QueryOne executes the query and returns the first selected row as a map
	// and discards the rest
	QueryOne(query QueryGenerator) (map[string]interface{}, error)

	// QueryCAS executes a lightweight transaction (i.e. an UPDATE or INSERT
	// statement containing an IF clause). If the transaction fails because
	// the existing values did not match, the previous values will be returned
	QueryCAS(query QueryGenerator) (result map[string]interface{}, applied bool, err error)

	// Query executes the query, returns a slice of maps containing each row.
	Query(query QueryGenerator) ([]map[string]interface{}, error)

	// Iter executes the query and returns an iterator capable of iterating over
	// all results.
	Iter(query QueryGenerator) Iter

	// Execute executes a query and discards any results
	Execute(query QueryGenerator) error

	// ExecuteBatch executes a batch operation and returns nil if successful
	// otherwise an error is returned describing the failure.
	ExecuteBatch(queries []QueryGenerator, options QueryOptions) error

	// ExecuteBatchCAS  executes a batch operation and returns true if successful,
	// the initial result as a map and an iterator (to scan aditional rows if
	// more than one conditional statement) was sent.
	ExecuteBatchCAS(queries []QueryGenerator, options QueryOptions) (result map[string]interface{}, iter Iter, applied bool, err error)

	// Close closes the underlying session
	Close()
}
