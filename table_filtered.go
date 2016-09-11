package gocassa

// The FilteredTable type represents a Table that has been filtered by some
// relations (a relation is a 'WHERE' condition such as '=' or 'IN'). To create
// a FilteredTable the Where function in the Table type can be used.
type FilteredTable struct {
	*Table
	relations []Relation
}

func (t *FilteredTable) Set(v interface{}) RunnableQuery {
	fields := transformFields(toMap(v))
	updateFields := removeFields(fields, append(t.partitionKeys, t.clusteringColumns...))

	var q Query
	if len(updateFields) == 0 {
		q = NewQuery(t.Table, InsertQueryType).Values(fields)
	} else {
		q = NewQuery(t.Table, UpdateQueryType).Values(updateFields)
		for _, relation := range t.relations {
			q = q.Where(relation)
		}
	}

	return RunnableQuery{
		Executor: t.keyspace.QueryExecutor(),
		Query:    q,
	}
}

func (t *FilteredTable) Update(m map[string]interface{}) RunnableQuery {
	fields := transformFields(m)

	q := NewQuery(t.Table, UpdateQueryType).Values(fields)
	for _, relation := range t.relations {
		q = q.Where(relation)
	}

	return RunnableQuery{
		Executor: t.keyspace.QueryExecutor(),
		Query:    q,
	}
}

func (t *FilteredTable) Select(fields ...Selection) RunnableQuery {
	q := NewQuery(t.Table, SelectQueryType).Select(fields...)
	for _, relation := range t.relations {
		q = q.Where(relation)
	}

	return RunnableQuery{
		Executor: t.keyspace.QueryExecutor(),
		Query:    q,
	}
}

func (t *FilteredTable) Delete(fields ...Selection) RunnableQuery {
	q := NewQuery(t.Table, DeleteQueryType).Select(fields...)
	for _, relation := range t.relations {
		q = q.Where(relation)
	}

	return RunnableQuery{
		Executor: t.keyspace.QueryExecutor(),
		Query:    q,
	}
}

func (t *FilteredTable) Where(relations ...Relation) *FilteredTable {
	t.relations = append(t.relations, relations...)

	return t
}
