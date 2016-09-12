package gocassa

import (
	"sync"

	"github.com/Sirupsen/logrus"

	"github.com/gocql/gocql"
)

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

func (qe gocqlExecutor) QueryOne(query QueryGenerator) (map[string]interface{}, error) {
	cqlQuery := qe.createCQLQuery(query)

	m := map[string]interface{}{}
	if err := cqlQuery.MapScan(m); err != nil {
		return nil, err
	}

	return m, nil
}

func (qe gocqlExecutor) QueryCAS(query QueryGenerator) (result map[string]interface{}, applied bool, err error) {
	cqlQuery := qe.createCQLQuery(query)

	m := map[string]interface{}{}
	applied, err = cqlQuery.MapScanCAS(m)
	if err != nil {
		return nil, false, err
	}

	return m, applied, nil
}

func (qe gocqlExecutor) Query(query QueryGenerator) ([]map[string]interface{}, error) {
	cqlQuery := qe.createCQLQuery(query)

	iter := cqlQuery.Iter()
	ret := []map[string]interface{}{}

	m := map[string]interface{}{}
	for iter.MapScan(m) {
		ret = append(ret, m)

		m = map[string]interface{}{}
	}

	return ret, iter.Close()
}

func (qe gocqlExecutor) Iter(query QueryGenerator) Iter {
	cqlQuery := qe.createCQLQuery(query)

	return gocqlIter{
		iter: cqlQuery.Iter(),
	}
}

// Query executes a query and returns the results.
func (qe gocqlExecutor) Execute(query QueryGenerator) error {
	cqlQuery := qe.createCQLQuery(query)

	return cqlQuery.Exec()
}

func (qe gocqlExecutor) ExecuteBatch(queries []QueryGenerator, options QueryOptions) error {
	batch := qe.createCQLBatch(queries, options)

	return qe.session.ExecuteBatch(batch)
}

func (qe gocqlExecutor) ExecuteBatchCAS(
	queries []QueryGenerator, options QueryOptions,
) (
	result map[string]interface{}, iter Iter, applied bool, err error,
) {
	batch := qe.createCQLBatch(queries, options)

	applied, cqlIter, err := qe.session.MapExecuteBatchCAS(batch, result)

	return result, gocqlIter{
		iter: cqlIter,
		err:  err,
	}, applied, err
}

func (qe gocqlExecutor) Close() {
	qe.session.Close()
}

func (qe *gocqlExecutor) createCQLQuery(query QueryGenerator) *gocql.Query {
	stmt, vals := query.GenerateStatement()

	logrus.WithFields(logrus.Fields{
		"values": vals,
	}).Infof("Executing query: %s", stmt)

	cqlQuery := qe.session.Query(stmt, vals...)
	if query.Options().Consistency != nil {
		cqlQuery = cqlQuery.Consistency(*query.Options().Consistency)
	}

	return cqlQuery
}

func (qe *gocqlExecutor) createCQLBatch(queries []QueryGenerator, options QueryOptions) *gocql.Batch {
	batch := gocql.NewBatch(options.BatchType)
	if options.Consistency != nil {
		batch.Cons = *options.Consistency
	}
	if options.SerialConsistency != nil {
		batch = batch.SerialConsistency(*options.SerialConsistency)
	}

	for _, query := range queries {
		stmt, vals := query.GenerateStatement()

		logrus.WithFields(logrus.Fields{
			"values": vals,
		}).Infof("Adding query to batch: %s", stmt)

		batch.Query(stmt, vals...)
	}

	return batch
}

type gocqlIter struct {
	mtx  sync.Mutex
	iter *gocql.Iter
	err  error
}

func (iter gocqlIter) Scan(dest interface{}) bool {
	var m map[string]interface{}

	if ok := iter.iter.MapScan(m); !ok {
		return false
	}

	if err := decodeResult(m, dest); err != nil {
		iter.mtx.Lock()
		iter.err = err
		iter.mtx.Unlock()

		return false
	}

	return true
}

func (iter gocqlIter) NumRows() int {
	return iter.iter.NumRows()
}

func (iter gocqlIter) WillSwitchPage() bool {
	return iter.iter.WillSwitchPage()
}

func (iter gocqlIter) GetCustomPayload() map[string][]byte {
	return iter.iter.GetCustomPayload()
}

func (iter gocqlIter) Close() error {
	if err := iter.iter.Close(); err != nil {
		return err
	}

	iter.mtx.Lock()
	defer iter.mtx.Unlock()

	return iter.err
}
