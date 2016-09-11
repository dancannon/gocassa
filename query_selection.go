package gocassa

import "fmt"

// A Selection is used with SELECT and DELETE statements and is used to build
// selection , for example:
//
//     tbl.AddValue(MapTerm("field", "element")).List()
//
// will create the following CQL query:
//
//     SELECT field[element] FROM keyspace.table
type Selection struct {
	identifier string
	term       interface{}
}

// Identifier creates a regular selection
func Identifier(identifier string) Selection {
	return Selection{
		identifier: identifier,
	}
}

// MapKey creates a selection for a map key
func MapKey(identifier string, key interface{}) Selection {
	return Selection{
		identifier: identifier,
		term:       key,
	}
}

// ListIndex creates a selection for a list index
func ListIndex(identifier string, index int) Selection {
	return Selection{
		identifier: identifier,
		term:       index,
	}
}

func (s Selection) generateCQL() string {
	if s.term == nil {
		return s.identifier
	}

	return fmt.Sprintf("%s[%s]", s.identifier, printElem(s.term))
}
