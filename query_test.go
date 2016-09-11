package gocassa

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestQuerySelect_order(t *testing.T) {
	qe := NewMockExecutor(mock.Mock{})

	k := NewKeyspace(qe, "test", nil)
	tbl := NewTable(k, "test", Document{}, []string{"fielda"}, nil, nil)
	q := NewQuery(tbl, SelectQueryType).Select().OrderBy(
		Ordering{"fielda", DESC},
	)

	stmt, values := q.GenerateStatement(QueryOptions{})

	assert.Equal(t, `SELECT * FROM test.test ORDER BY fielda DESC`, stmt)
	assert.Equal(t, []interface{}{}, values)
}

func TestQuerySelect_limit(t *testing.T) {
	qe := NewMockExecutor(mock.Mock{})

	k := NewKeyspace(qe, "test", nil)
	tbl := NewTable(k, "test", Document{}, []string{"fielda"}, nil, nil)
	q := NewQuery(tbl, SelectQueryType).Select().Limit(10)

	stmt, values := q.GenerateStatement(QueryOptions{})

	assert.Equal(t, `SELECT * FROM test.test LIMIT ?`, stmt)
	assert.Equal(t, []interface{}{10}, values)
}

func TestQuerySelect_allowFiltering(t *testing.T) {
	qe := NewMockExecutor(mock.Mock{})

	k := NewKeyspace(qe, "test", nil)
	tbl := NewTable(k, "test", Document{}, []string{"fielda"}, nil, nil)
	q := NewQuery(tbl, SelectQueryType).Select()

	stmt, values := q.GenerateStatement(QueryOptions{
		AllowFiltering: true,
	})

	assert.Equal(t, `SELECT * FROM test.test ALLOW FILTERING`, stmt)
	assert.Equal(t, []interface{}{}, values)
}

func TestQueryUpdate_timestamp(t *testing.T) {
	qe := NewMockExecutor(mock.Mock{})

	k := NewKeyspace(qe, "test", nil)
	tbl := NewTable(k, "test", Document{}, []string{"fielda"}, nil, nil)
	q := NewQuery(tbl, UpdateQueryType).Values(map[string]interface{}{"a": "a"})

	stmt, values := q.GenerateStatement(QueryOptions{
		Timestamp: time.Date(2016, 1, 1, 0, 0, 0, 0, time.UTC),
	})

	assert.Equal(t, `UPDATE test.test SET a = ? USING TIMESTAMP 1451606400000`, stmt)
	assert.Equal(t, []interface{}{"a"}, values)
}

func TestQueryUpdate_ttl(t *testing.T) {
	qe := NewMockExecutor(mock.Mock{})

	k := NewKeyspace(qe, "test", nil)
	tbl := NewTable(k, "test", Document{}, []string{"fielda"}, nil, nil)
	q := NewQuery(tbl, UpdateQueryType).Values(map[string]interface{}{"a": "a"})

	stmt, values := q.GenerateStatement(QueryOptions{
		TTL: time.Hour,
	})

	assert.Equal(t, `UPDATE test.test SET a = ? USING TTL 3600`, stmt)
	assert.Equal(t, []interface{}{"a"}, values)
}

func TestQueryUpdate_timestampAndTTL(t *testing.T) {
	qe := NewMockExecutor(mock.Mock{})

	k := NewKeyspace(qe, "test", nil)
	tbl := NewTable(k, "test", Document{}, []string{"fielda"}, nil, nil)
	q := NewQuery(tbl, UpdateQueryType).Values(map[string]interface{}{"a": "a"})

	stmt, values := q.GenerateStatement(QueryOptions{
		Timestamp: time.Date(2016, 1, 1, 0, 0, 0, 0, time.UTC),
		TTL:       time.Hour,
	})

	assert.Equal(t, `UPDATE test.test SET a = ? USING TIMESTAMP 1451606400000 AND TTL 3600`, stmt)
	assert.Equal(t, []interface{}{"a"}, values)
}
