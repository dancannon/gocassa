package gocassa

import (
	"bytes"

	"strings"
)

type relationType uint8

const (
	relationTypeEQ relationType = iota
	relationTypeIN
	relationTypeGT
	relationTypeGE
	relationTypeLT
	relationTypeLE
)

type Relation struct {
	relationType relationType
	key          string
	terms        []interface{}
}

func (r Relation) generateCQL() (string, []interface{}) {
	buf := &bytes.Buffer{}

	buf.WriteString(strings.ToLower(r.key))

	switch r.relationType {
	case relationTypeEQ:
		buf.WriteString(" = ?")
	case relationTypeIN:
		buf.WriteString(" IN ?")
	case relationTypeGT:
		buf.WriteString(" > ?")
	case relationTypeGE:
		buf.WriteString(" >= ?")
	case relationTypeLT:
		buf.WriteString(" < ?")
	case relationTypeLE:
		buf.WriteString(" <= ?")
	}

	return buf.String(), r.terms
}

func Eq(key string, term interface{}) Relation {
	return Relation{
		relationType: relationTypeEQ,
		key:          key,
		terms:        []interface{}{term},
	}
}

func In(key string, terms ...interface{}) Relation {
	return Relation{
		relationType: relationTypeIN,
		key:          key,
		terms:        terms,
	}
}

func GT(key string, term interface{}) Relation {
	return Relation{
		relationType: relationTypeGT,
		key:          key,
		terms:        []interface{}{term},
	}
}

func GTE(key string, term interface{}) Relation {
	return Relation{
		relationType: relationTypeGE,
		key:          key,
		terms:        []interface{}{term},
	}
}

func LT(key string, term interface{}) Relation {
	return Relation{
		relationType: relationTypeLT,
		key:          key,
		terms:        []interface{}{term},
	}
}

func LTE(key string, term interface{}) Relation {
	return Relation{
		relationType: relationTypeLE,
		key:          key,
		terms:        []interface{}{term},
	}
}
