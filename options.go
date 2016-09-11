package gocassa

import (
	"time"

	"github.com/gocql/gocql"
)

type ColumnDirection bool

const (
	ASC  ColumnDirection = false
	DESC                 = true
)

func (d ColumnDirection) String() string {
	switch d {
	case ASC:
		return "ASC"
	case DESC:
		return "DESC"
	default:
		return ""
	}
}

// Ordering specifies a clustering column and whether its
// clustering order is ASC or DESC.
type Ordering struct {
	Column    string
	Direction ColumnDirection
}

// // Options can contain table or statement specific options.
// // The reason for this is because statement specific (TTL, Limit) options make sense as table level options
// // (eg. have default TTL for every Update without specifying it all the time)
type QueryOptions struct {
	// Timestamp specifies the time at which the column was written to the database,
	// if not specified then the time that the write occurred to the column is used
	Timestamp time.Time
	// TTL specifies a duration over which data is valid. It will be truncated to second precision upon statement
	// execution.
	TTL time.Duration
	// Limit query result set
	Limit int
	// ClusteringOrder specifies the clustering order during table creation. If empty, it is omitted and the defaults are used.
	Orderings []Ordering
	// Indicates if allow filtering should be appended at the end of the query
	AllowFiltering bool
	// Consistency specifies the consistency level. If nil, it is considered not set
	Consistency *gocql.Consistency
	// SerialConsistency sets the consistency level for the
	// serial phase of conditional updates. That consistency can only be
	// either SERIAL or LOCAL_SERIAL and if not present, it defaults to
	// SERIAL. This option will be ignored for anything else that a
	// conditional update/insert.
	SerialConsistency *gocql.SerialConsistency
	// BatchType is used when executing a batch query
	BatchType gocql.BatchType
}

type KeyspaceOptions struct {
	ReplicationClass  string
	ReplicationFactor int
	DataCenters       map[string]int
	DurableWrites     bool
}

type TableOptions struct {
	CompactStorage bool
	Orderings      []Ordering
	Comment        string
}
