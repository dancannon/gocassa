package gocassa

//
// Raw CQL
//

// Filter is a subset of a Table, filtered by Relations.
// You can do writes or reads on a filter.
type Filter interface {
	// Updates does a partial update. Use this if you don't want to overwrite your whole row, but you want to modify fields atomically.
	Update(m map[string]interface{}) Op // Probably this is danger zone (can't be implemented efficiently) on a selectuinb with more than 1 document
	// Delete all rows matching the filter.
	Delete() Op
	// Read the results. Make sure you pass in a pointer to a slice.
	Read(pointerToASlice interface{}) Op
	// Read one result. Make sure you pass in a pointer.
	ReadOne(pointer interface{}) Op
}

// Keys is used with the raw CQL Table type. It is implicit when using recipe tables.
type Keys struct {
	PartitionKeys     []string
	ClusteringColumns []string
	Compound          bool //indicates if the partitions keys are gereated as compound key when no clustering columns are set
}

// Op is returned by both read and write methods, you have to run them explicitly to take effect.
// It represents one or more operations.
type Op interface {
	// Run the operation.
	Run() error
	// You do not need this in 95% of the use cases, use Run!
	// Using atomic batched writes (logged batches in Cassandra terminology) comes at a high performance cost!
	RunAtomically() error
	// Add an other Op to this one.
	Add(...Op) Op
	// WithOptions lets you specify `Op` level `Options`.
	// The `Op` level Options and the `Table` level `Options` will be merged in a way that Op level takes precedence.
	// All queries in an `Op` will have the specified `Options`.
	// When using Add(), the existing options are preserved.
	// For example:
	//
	//    op1.WithOptions(Options{Limit:3}).Add(op2.WithOptions(Options{Limit:2})) // op1 has a limit of 3, op2 has a limit of 2
	//    op1.WithOptions(Options{Limit:3}).Add(op2).WithOptions(Options{Limit:2}) // op1 and op2 both have a limit of 2
	//
	WithOptions(Options) Op
	// Preflight performs any pre-execution validation that confirms the op considers itself "valid".
	// NOTE: Run() and RunAtomically() should call this method before execution, and abort if any errors are returned.
	Preflight() error
	// GenerateStatement generates the statment and params to perform the operation
	GenerateStatement() (string, []interface{})
	// QueryExecutor returns the QueryExecutor
	QueryExecutor() QueryExecutor
}

type Counter int
