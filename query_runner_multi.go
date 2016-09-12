package gocassa

import "fmt"

func MultiQuery(queries ...RunnableQuery) RunnableQueries {
	return RunnableQueries{
		Queries: queries,
	}
}

type RunnableQueries struct {
	Executor QueryExecutor
	Queries  []RunnableQuery
	Options  QueryOptions
}

func (qs RunnableQueries) WithOptions(options QueryOptions) RunnableQueries {
	qs.Options = options
	return qs
}

func (qs RunnableQueries) Add(queries_ ...RunnableQuery) RunnableQueries {
	for _, query := range queries_ {
		qs.Queries = append(qs.Queries, query)
	}
	return qs
}

func (qs RunnableQueries) Execute() error {
	for _, q := range qs.Queries {
		executor := qs.Executor
		if executor == nil {
			executor = q.Executor
		}

		if err := q.Execute(); err != nil {
			return err
		}
	}

	return nil
}

func (qs RunnableQueries) ExecuteBatch() error {
	if len(qs.Queries) == 0 {
		return nil
	}

	queries := make([]QueryGenerator, len(qs.Queries))
	for i, q := range qs.Queries {
		queries[i] = q.Query
	}
	return qs.queryExecutor().ExecuteBatch(queries, qs.Options)
}

func (qs RunnableQueries) ExecuteBatchCAS() (result map[string]interface{}, iter Iter, applied bool, err error) {
	if len(qs.Queries) == 0 {
		// We have to error here or we would return an unusable Iter
		return nil, nil, false, fmt.Errorf("No queries in batch")
	}

	queries := make([]QueryGenerator, len(qs.Queries))
	for i, q := range qs.Queries {
		queries[i] = q.Query
	}
	return qs.queryExecutor().ExecuteBatchCAS(queries, qs.Options)
}

func (qs *RunnableQueries) queryExecutor() QueryExecutor {
	if qs.Executor != nil {
		return qs.Executor
	}

	if len(qs.Queries) > 0 {
		return qs.Queries[0].Executor
	}

	return nil
}
