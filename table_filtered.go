package gocassa

// The FilteredTable type represents a Table that has been filtered by some
// relations (a relation is a 'WHERE' condition such as '=' or 'IN'). To create
// a FilteredTable the Where function in the Table type can be used.
type FilteredTable struct {
	*Table
	relations  []Relation
	selections []Selection
	orderings  []Ordering
	limit      int
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

	q := NewQuery(t.Table, UpdateQueryType).
		Values(fields).
		Where(t.relations...)

	return RunnableQuery{
		Executor: t.keyspace.QueryExecutor(),
		Query:    q,
	}
}

func (t *FilteredTable) Read() RunnableQuery {
	q := NewQuery(t.Table, SelectQueryType).
		Fields(t.selections...).
		Where(t.relations...).
		OrderBy(t.orderings...).
		Limit(t.limit)

	return RunnableQuery{
		Executor: t.keyspace.QueryExecutor(),
		Query:    q,
	}
}

func (t *FilteredTable) List() RunnableQuery {
	q := NewQuery(t.Table, SelectQueryType).
		Fields(t.selections...).
		Where(t.relations...).
		OrderBy(t.orderings...).
		Limit(t.limit)

	return RunnableQuery{
		Executor: t.keyspace.QueryExecutor(),
		Query:    q,
	}
}

func (t *FilteredTable) Delete() RunnableQuery {
	q := NewQuery(t.Table, DeleteQueryType).
		Fields(t.selections...).
		Where(t.relations...)

	return RunnableQuery{
		Executor: t.keyspace.QueryExecutor(),
		Query:    q,
	}
}

func (t *FilteredTable) Fields(selections ...Selection) *FilteredTable {
	t.selections = append(t.selections, selections...)

	return t
}

func (t *FilteredTable) Where(relations ...Relation) *FilteredTable {
	t.relations = append(t.relations, relations...)

	return t
}

func (t *FilteredTable) OrderBy(orderings ...Ordering) *FilteredTable {
	t.orderings = append(t.orderings, orderings...)

	return t
}

func (t *FilteredTable) Limit(limit int) *FilteredTable {
	t.limit = limit

	return t
}
