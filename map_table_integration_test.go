// +build integration

package gocassa

import (
	"github.com/stretchr/testify/assert"

	"testing"
)

func TestIntegrationMapTable(t *testing.T) {
	type Document struct {
		FieldA string
		FieldB string
		FieldC string
		FieldD string
	}

	tbl := NewMapTable(keyspace, "map_table", Document{}, "fielda")

	assert.Nil(t, tbl.Drop())
	assert.Nil(t, tbl.Create())

	t.Run("Set", func(t *testing.T) {
		assert.Nil(t, tbl.Set(Document{
			FieldA: "a",
			FieldB: "b",
			FieldC: "c",
			FieldD: "d",
		}).Execute())
	})

	t.Run("Insert", func(t *testing.T) {
		assert.Nil(t, tbl.Insert(map[string]interface{}{
			"fielda": "e",
			"fieldb": "f",
			"fieldc": "g",
		}).Execute())
	})

	t.Run("Update", func(t *testing.T) {
		assert.Nil(t, tbl.Update("e", map[string]interface{}{
			"fieldd": "h",
		}).Execute())
	})

	t.Run("Read", func(t *testing.T) {
		doc := &Document{}
		err := tbl.Read("a").ScanOne(doc)
		assert.Nil(t, err)

		assert.Equal(t, "a", doc.FieldA)
		assert.Equal(t, "b", doc.FieldB)
		assert.Equal(t, "c", doc.FieldC)
		assert.Equal(t, "d", doc.FieldD)
	})

	t.Run("List", func(t *testing.T) {
		docs := []Document{}
		err := tbl.List().Scan(&docs)

		assert.Nil(t, err)
		if assert.Len(t, docs, 2) {
			assert.Equal(t, "a", docs[0].FieldA)
			assert.Equal(t, "e", docs[1].FieldA)
		}
	})

	t.Run("Delete", func(t *testing.T) {
		err := tbl.Delete("a").Execute()
		assert.Nil(t, err)

		doc := &Document{}
		err = tbl.Read("a").ScanOne(doc)
		assert.NotNil(t, err)
	})
}
