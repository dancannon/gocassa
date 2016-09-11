package gocassa

import (
	"os"
	"strings"
	"testing"

	"github.com/gocql/gocql"
)

const (
	KEYSPACE_NAME = "gocassa_test"
)

var (
	keyspace *Keyspace
)

func TestMain(m *testing.M) {
	cluster := gocql.NewCluster(cassandraHosts()...)
	cluster.ProtoVersion = 3
	cluster.Consistency = gocql.One
	// cluster.Timeout = 10 * time.Second // Travis' C* is sloooow
	cluster.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: 3}

	qe, err := Connect(cluster)
	if err != nil {
		panic(qe)
	}
	defer qe.Close()

	keyspace = NewKeyspace(qe, KEYSPACE_NAME, nil)
	if err := keyspace.Drop(); err != nil {
		panic(err)
	}
	if err := keyspace.Create(); err != nil {
		panic(err)
	}

	os.Exit(m.Run())
}

func cassandraHosts() []string {
	if hosts := os.Getenv("CASSANDRA_HOSTS"); hosts != "" {
		return strings.Split(hosts, ",")
	} else {
		return []string{"127.0.0.1"}
	}
}
