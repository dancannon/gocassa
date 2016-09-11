package gocassa

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"testing"
)

func TestKeyspaceCreate_noOptions(t *testing.T) {
	m := mock.Mock{}
	m.On(
		"Execute",
		"CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class':'SimpleStrategy','replication_factor':1} AND DURABLE_WRITES = false;",
		[]interface{}(nil),
	).Return(nil)

	qe := NewMockExecutor(m)

	k := NewKeyspace(qe, "test", nil)
	assert.Nil(t, k.Create())
	m.AssertExpectations(t)
}

func TestKeyspaceCreate_durableWrites(t *testing.T) {
	m := mock.Mock{}
	m.On(
		"Execute",
		"CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class':'SimpleStrategy','replication_factor':1} AND DURABLE_WRITES = true;",
		[]interface{}(nil),
	).Return(nil)

	qe := NewMockExecutor(m)

	k := NewKeyspace(qe, "test", &KeyspaceOptions{
		DurableWrites: true,
	})
	assert.Nil(t, k.Create())
	m.AssertExpectations(t)
}

func TestKeyspaceCreate_simple(t *testing.T) {
	m := mock.Mock{}
	m.On(
		"Execute",
		"CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class':'SimpleStrategy','replication_factor':3} AND DURABLE_WRITES = false;",
		[]interface{}(nil),
	).Return(nil)

	qe := NewMockExecutor(m)

	k := NewKeyspace(qe, "test", &KeyspaceOptions{
		ReplicationClass:  "SimpleStrategy",
		ReplicationFactor: 3,
	})
	assert.Nil(t, k.Create())
	m.AssertExpectations(t)
}

func TestKeyspaceCreate_networkSingleDC(t *testing.T) {
	m := mock.Mock{}
	m.On(
		"Execute",
		"CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class':'NetworkTopologyStrategy','dc1':3} AND DURABLE_WRITES = false;",
		[]interface{}(nil),
	).Return(nil)

	qe := NewMockExecutor(m)

	k := NewKeyspace(qe, "test", &KeyspaceOptions{
		ReplicationClass: "NetworkTopologyStrategy",
		DataCenters: map[string]int{
			"dc1": 3,
		},
	})
	assert.Nil(t, k.Create())
	m.AssertExpectations(t)
}

func TestKeyspaceCreate_networkMultipleDC(t *testing.T) {
	m := mock.Mock{}
	m.On(
		"Execute",
		"CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class':'NetworkTopologyStrategy','dc1':3,'dc2':3} AND DURABLE_WRITES = false;",
		[]interface{}(nil),
	).Return(nil)

	qe := NewMockExecutor(m)

	k := NewKeyspace(qe, "test", &KeyspaceOptions{
		ReplicationClass: "NetworkTopologyStrategy",
		DataCenters: map[string]int{
			"dc1": 3,
			"dc2": 3,
		},
	})
	assert.Nil(t, k.Create())
	m.AssertExpectations(t)
}

func TestKeyspaceDrop(t *testing.T) {
	m := mock.Mock{}
	m.On(
		"Execute",
		"DROP KEYSPACE IF EXISTS test",
		[]interface{}(nil),
	).Return(nil)

	qe := NewMockExecutor(m)

	k := NewKeyspace(qe, "test", nil)
	assert.Nil(t, k.Drop())
	m.AssertExpectations(t)
}
