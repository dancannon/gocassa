// +build integration

package gocassa

import (
	"github.com/stretchr/testify/assert"

	"testing"
)

func TestIntegrationTableSinglePartitionKey(t *testing.T) {
	type Document struct {
		FieldA string
		FieldB string
		FieldC string
		FieldD string
	}

	tbl := NewTable(keyspace, "table_single_partition", Document{}, []string{"fielda"}, nil, nil)

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
		assert.Nil(t, tbl.Where(Eq("fielda", "e")).Update(map[string]interface{}{
			"fieldd": "h",
		}).Execute())
	})

	t.Run("Read", func(t *testing.T) {
		doc := &Document{}
		err := tbl.Where(Eq("fielda", "a")).Read().ScanOne(doc)
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
		err := tbl.Where(Eq("fielda", "a")).Delete().Execute()
		assert.Nil(t, err)

		doc := &Document{}
		err = tbl.Where(Eq("fielda", "a")).Read().ScanOne(doc)
		assert.NotNil(t, err)
	})

	t.Run("SelectDeleted", func(t *testing.T) {
	})
}

func TestIntegrationModifiers(t *testing.T) {
	type Document struct {
		ID   string
		List []string
		Map  map[string]int
	}

	tbl := NewTable(keyspace, "table_modifiers", Document{}, []string{"ID"}, nil, nil)

	assert.Nil(t, tbl.Drop())
	assert.Nil(t, tbl.Create())

	t.Run("Set", func(t *testing.T) {
		assert.Nil(t, tbl.Set(Document{
			ID:   "1",
			List: []string{"a", "b"},
			Map:  map[string]int{"a": 1, "b": 2},
		}).Execute())
	})

	t.Run("DeleteMapKey", func(t *testing.T) {
		err := tbl.Where(Eq("ID", "1")).Delete(
			MapKey("map", "b"),
		).Execute()
		assert.Nil(t, err)

		doc := &Document{}
		err = tbl.Where(Eq("id", "1")).Read().ScanOne(doc)
		assert.Nil(t, err)
		assert.NotContains(t, doc.Map, "b")
	})

	t.Run("DeleteListIndex", func(t *testing.T) {
		err := tbl.Where(Eq("ID", "1")).Delete(
			ListIndex("list", 1),
		).Execute()
		assert.Nil(t, err)

		doc := &Document{}
		err = tbl.Where(Eq("id", "1")).Read().ScanOne(doc)
		assert.Nil(t, err)
		assert.NotContains(t, doc.Map, "b")
	})

	t.Run("UpdateMapSetField", func(t *testing.T) {
		err := tbl.Where(Eq("ID", "1")).Update(map[string]interface{}{
			"map": MapSetField("b", 2),
		}).Execute()
		assert.Nil(t, err)

		doc := &Document{}
		err = tbl.Where(Eq("id", "1")).Read().ScanOne(doc)
		assert.Nil(t, err)
		assert.Contains(t, doc.Map, "b")
	})

	t.Run("UpdateListAppend", func(t *testing.T) {
		err := tbl.Where(Eq("ID", "1")).Update(map[string]interface{}{
			"list": ListAppend("b"),
		}).Execute()
		assert.Nil(t, err)

		doc := &Document{}
		err = tbl.Where(Eq("id", "1")).Read().ScanOne(doc)
		assert.Nil(t, err)
		assert.Contains(t, doc.Map, "b")
	})
}

func TestIntegrationMultiQuery(t *testing.T) {
	type Document struct {
		ID string
	}

	tbl := NewTable(keyspace, "table_multi", Document{}, []string{"ID"}, nil, nil)

	assert.Nil(t, tbl.Drop())
	assert.Nil(t, tbl.Create())

	t.Run("Execute", func(t *testing.T) {
		q := MultiQuery()
		q = q.Add(tbl.Set(Document{
			ID: "1",
		}))
		q = q.Add(tbl.Set(Document{
			ID: "2",
		}))
		err := q.Execute()
		assert.Nil(t, err)

		iter := tbl.List().Iter()
		assert.Nil(t, iter.Close())
		assert.Equal(t, 2, iter.NumRows())
	})
	t.Run("Batch", func(t *testing.T) {
		q := MultiQuery()
		q = q.Add(tbl.Set(Document{
			ID: "3",
		}))
		q = q.Add(tbl.Set(Document{
			ID: "4",
		}))
		err := q.ExecuteBatch()
		assert.Nil(t, err)

		iter := tbl.List().Iter()
		assert.Nil(t, iter.Close())
		assert.Equal(t, 4, iter.NumRows())
	})
}
