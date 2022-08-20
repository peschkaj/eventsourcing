package eventsourcing

import (
	"github.com/gofrs/uuid"
)

// idFunc is a global function that generates aggregate IDs.
// It could be changed from the outside via the SetIDFunc function.
var idFunc = NewUuid

// SetIDFunc is used to change how aggregate IDs are generated
// default is a random string
func SetIDFunc(f func() uuid.UUID) {
	idFunc = f
}

func NewUuid() uuid.UUID {
	id, err := uuid.NewV7(uuid.MillisecondPrecision)

	if err != nil {
		return emptyAggregateID
	}

	return id
}
