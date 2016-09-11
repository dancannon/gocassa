package gocassa

import (
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type TypeDocument struct {
	FieldA int        // int
	FieldB int64      // bigint
	FieldC uint       // varint
	FieldD string     // varchar
	FieldE float32    // float
	FieldF float64    // double
	FieldG bool       // boolean
	FieldH time.Time  // timestamp
	FieldI gocql.UUID // uuid
	FieldJ []byte     // blob
	FieldK Counter    // counter
}

type Document struct {
	FieldA string
	FieldB string
	FieldC string
	FieldD string
}

func TestTableCreate(t *testing.T) {
	m := mock.Mock{}
	m.On(
		"Execute",
		`CREATE TABLE IF NOT EXISTS test.test (fielda int,fieldb bigint,fieldc varint,fieldd varchar,fielde float,fieldf double,fieldg boolean,fieldh timestamp,fieldi uuid,fieldj blob,fieldk counter,PRIMARY KEY (fielda))`,
		[]interface{}(nil),
	).Return(nil)

	qe := NewMockExecutor(m)

	k := NewKeyspace(qe, "test", nil)
	tbl := NewTable(k, "test", TypeDocument{}, []string{"fielda"}, nil, nil)
	assert.Nil(t, tbl.Create())
	m.AssertExpectations(t)
}

func TestTableCreate_partitionKey(t *testing.T) {
	m := mock.Mock{}
	m.On(
		"Execute",
		`CREATE TABLE IF NOT EXISTS test.test (fielda varchar,fieldb varchar,fieldc varchar,fieldd varchar,PRIMARY KEY (fielda))`,
		[]interface{}(nil),
	).Return(nil)

	qe := NewMockExecutor(m)

	k := NewKeyspace(qe, "test", nil)
	tbl := NewTable(k, "test", Document{}, []string{"fielda"}, nil, nil)
	assert.Nil(t, tbl.Create())
	m.AssertExpectations(t)
}

func TestTableCreate_PartitionClustering(t *testing.T) {
	m := mock.Mock{}
	m.On(
		"Execute",
		`CREATE TABLE IF NOT EXISTS test.test (fielda varchar,fieldb varchar,fieldc varchar,fieldd varchar,PRIMARY KEY (fielda,fieldb))`,
		[]interface{}(nil),
	).Return(nil)

	qe := NewMockExecutor(m)

	k := NewKeyspace(qe, "test", nil)
	tbl := NewTable(k, "test", Document{}, []string{"fielda"}, []string{"fieldb"}, nil)
	assert.Nil(t, tbl.Create())
	m.AssertExpectations(t)
}

func TestTableCreate_multiplePartition(t *testing.T) {
	m := mock.Mock{}
	m.On(
		"Execute",
		`CREATE TABLE IF NOT EXISTS test.test (fielda varchar,fieldb varchar,fieldc varchar,fieldd varchar,PRIMARY KEY ((fielda,fieldb)))`,
		[]interface{}(nil),
	).Return(nil)

	qe := NewMockExecutor(m)

	k := NewKeyspace(qe, "test", nil)
	tbl := NewTable(k, "test", Document{}, []string{"fielda", "fieldb"}, nil, nil)
	assert.Nil(t, tbl.Create())
	m.AssertExpectations(t)
}

func TestTableCreate_multipleClustering(t *testing.T) {
	m := mock.Mock{}
	m.On(
		"Execute",
		`CREATE TABLE IF NOT EXISTS test.test (fielda varchar,fieldb varchar,fieldc varchar,fieldd varchar,PRIMARY KEY (fielda,fieldb,fieldc))`,
		[]interface{}(nil),
	).Return(nil)

	qe := NewMockExecutor(m)

	k := NewKeyspace(qe, "test", nil)
	tbl := NewTable(k, "test", Document{}, []string{"fielda"}, []string{"fieldb", "fieldc"}, nil)
	assert.Nil(t, tbl.Create())
	m.AssertExpectations(t)
}

func TestTableCreate_multipleKeys(t *testing.T) {
	m := mock.Mock{}
	m.On(
		"Execute",
		`CREATE TABLE IF NOT EXISTS test.test (fielda varchar,fieldb varchar,fieldc varchar,fieldd varchar,PRIMARY KEY ((fielda,fieldb),fieldc,fieldd))`,
		[]interface{}(nil),
	).Return(nil)

	qe := NewMockExecutor(m)

	k := NewKeyspace(qe, "test", nil)
	tbl := NewTable(k, "test", Document{}, []string{"fielda", "fieldb"}, []string{"fieldc", "fieldd"}, nil)
	assert.Nil(t, tbl.Create())
	m.AssertExpectations(t)
}

func TestTableCreate_options(t *testing.T) {
	m := mock.Mock{}
	m.On(
		"Execute",
		`CREATE TABLE IF NOT EXISTS test.test (fielda varchar,fieldb varchar,fieldc varchar,fieldd varchar,PRIMARY KEY (fielda)) WITH COMPACT STORAGE AND CLUSTERING ORDER (fieldb DESC,fieldc ASC)`,
		[]interface{}(nil),
	).Return(nil)

	qe := NewMockExecutor(m)

	k := NewKeyspace(qe, "test", nil)
	tbl := NewTable(k, "test", Document{}, []string{"fielda"}, nil, &TableOptions{
		ClusteringOrders: []ClusteringOrder{ClusteringOrder{"fieldb", DESC}, ClusteringOrder{"fieldc", ASC}},
		CompactStorage:   true,
	})
	assert.Nil(t, tbl.Create())
	m.AssertExpectations(t)
}
