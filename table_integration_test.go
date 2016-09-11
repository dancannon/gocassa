// +build integration

package gocassa

import (
	"github.com/stretchr/testify/assert"

	"testing"
)

func TestIntegrationTableSetGet(t *testing.T) {
	tbl := NewTable(keyspace, "test", Document{}, []string{"fielda"}, nil, nil)

	assert.Nil(t, tbl.Drop())
	assert.Nil(t, tbl.Create())

	assert.Nil(t, tbl.Set(Document{
		FieldA: "a",
		FieldB: "b",
		FieldC: "c",
		FieldD: "d",
	}))
}

func TestIntegrationTableDelete(t *testing.T) {

}
