package gocassa

import (
	"fmt"
	"sort"
	"strings"
)

type Keyspace struct {
	qe      QueryExecutor
	name    string
	options KeyspaceOptions
}

func NewKeyspace(qe QueryExecutor, name string, options *KeyspaceOptions) *Keyspace {
	if options == nil {
		options = &KeyspaceOptions{}
	}

	return &Keyspace{
		qe:      qe,
		name:    strings.ToLower(name),
		options: *options,
	}
}

// Name returns the name of the current keyspace
func (k *Keyspace) Name() string {
	return k.name
}

// CreateStatement returns a CQL which will create the current keyspace if it
// does not already exist.
func (k *Keyspace) CreateStatement() string {
	replicationMap := ""
	if k.options.ReplicationClass == "" {
		k.options.ReplicationClass = "SimpleStrategy"
		k.options.ReplicationFactor = 1
	}
	if k.options.ReplicationClass == "SimpleStrategy" {
		replicationMap = fmt.Sprintf("'class':'SimpleStrategy','replication_factor':%d", k.options.ReplicationFactor)
	} else if k.options.ReplicationClass == "NetworkTopologyStrategy" {
		dataCenters := make([]string, 0, len(k.options.DataCenters))
		for dc, rf := range k.options.DataCenters {
			dataCenters = append(dataCenters, fmt.Sprintf("'%s':%d", dc, rf))
		}
		// Sort to ensure generated CQL is always the same due to the fact that
		// Go's maps are unordered
		sort.Strings(dataCenters)

		replicationMap = "'class':'NetworkTopologyStrategy'," + strings.Join(dataCenters, ",")
	}

	return fmt.Sprintf(
		"CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = {%s} AND DURABLE_WRITES = %t;",
		k.Name(), replicationMap, k.options.DurableWrites,
	)
}

// Create attempts to create the current keyspace if it does not already exist.
func (k *Keyspace) Create() error {
	return k.qe.Execute(NewRawQuery(k.CreateStatement(), nil))
}

// DropStatement returns a CQL which will delete the current keyspace if it
// exists
func (k *Keyspace) DropStatement() string {
	return fmt.Sprintf("DROP KEYSPACE IF EXISTS %s", k.Name())
}

// Drop attempts to delete the current keyspace if it exists
func (k *Keyspace) Drop() error {
	return k.qe.Execute(NewRawQuery(k.DropStatement(), nil))
}

// Returns table names in a keyspace
func (k *Keyspace) Tables() ([]string, error) {
	stmt := fmt.Sprintf(
		"SELECT columnfamily_name FROM system.schema_columnfamilies WHERE keyspace_name = %s",
		k.Name(),
	)

	maps, err := k.qe.Query(NewRawQuery(stmt, nil))
	if err != nil {
		return nil, err
	}

	ret := []string{}
	for _, m := range maps {
		ret = append(ret, m["columnfamily_name"].(string))
	}

	return ret, nil
}

func (k *Keyspace) TableExists(name string) (bool, error) {
	ts, err := k.Tables()
	if err != nil {
		return false, err
	}

	for _, v := range ts {
		if strings.ToLower(v) == strings.ToLower(name) {
			return true, nil
		}
	}

	return false, nil
}

func (k *Keyspace) QueryExecutor() QueryExecutor {
	return k.qe
}
