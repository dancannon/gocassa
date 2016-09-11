package gocassa

type MapTable struct {
	*Table

	partitionKey string
}

func NewMapTable(keyspace *Keyspace, name string, documentValue interface{}, partitionKey string) *MapTable {
	return &MapTable{
		Table:        NewTable(keyspace, name, documentValue, []string{partitionKey}, nil, &TableOptions{}),
		partitionKey: partitionKey,
	}
}

func (t *MapTable) Update(id interface{}, m map[string]interface{}) RunnableQuery {
	return t.Where(Eq(t.partitionKey, id)).Update(m)
}

func (t *MapTable) Delete(id interface{}) RunnableQuery {
	return t.Where(Eq(t.partitionKey, id)).Delete()
}

func (t *MapTable) Read(id interface{}) RunnableQuery {
	return t.Where(Eq(t.partitionKey, id)).Read()
}

func (t *MapTable) MultiRead(ids []interface{}) RunnableQuery {
	return t.Where(In(t.partitionKey, ids...)).Read()
}

func (t *MapTable) WithOptions(options TableOptions) *MapTable {
	t.Table = t.Table.WithOptions(options)
	return t
}
