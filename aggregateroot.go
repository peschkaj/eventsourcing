package eventsourcing

import (
	"errors"
	"reflect"
	"time"

	"github.com/gofrs/uuid"
)

// Version is the event version used in event.Version, event.GlobalVersion and aggregateRoot
type Version uint64

// AggregateRoot to be included into aggregates
type AggregateRoot struct {
	aggregateID            uuid.UUID
	aggregateVersion       Version
	aggregateGlobalVersion Version
	aggregateEvents        []Event
}

var emptyAggregateID uuid.UUID = uuid.Nil

// ErrAggregateAlreadyExists returned if the aggregateID is set more than one time
var ErrAggregateAlreadyExists = errors.New("its not possible to set ID on already existing aggregate")

// TrackChange is used internally by behaviour methods to apply a state change to
// the current instance and also track it in order that it can be persisted later.
func (ar *AggregateRoot) TrackChange(a Aggregate, data interface{}) {
	ar.TrackChangeWithMetadata(a, data, nil)
}

// TrackChangeWithMetadata is used internally by behaviour methods to apply a state change to
// the current instance and also track it in order that it can be persisted later.
// meta data is handled by this func to store none related application state
func (ar *AggregateRoot) TrackChangeWithMetadata(a Aggregate, data interface{}, metadata map[string]interface{}) {
	// This can be overwritten in the constructor of the aggregate
	if ar.aggregateID == emptyAggregateID {
		ar.aggregateID = idFunc()
	}

	name := reflect.TypeOf(a).Elem().Name()
	event := Event{
		AggregateID:   ar.aggregateID,
		Version:       ar.nextVersion(),
		AggregateType: name,
		Timestamp:     time.Now().UTC(),
		Data:          data,
		Metadata:      metadata,
	}
	ar.aggregateEvents = append(ar.aggregateEvents, event)
	a.Transition(event)
}

// BuildFromHistory builds the aggregate state from events
func (ar *AggregateRoot) BuildFromHistory(a Aggregate, events []Event) {
	for _, event := range events {
		a.Transition(event)
		//Set the aggregate ID
		ar.aggregateID = event.AggregateID
		// Make sure the aggregate is in the correct version (the last event)
		ar.aggregateVersion = event.Version
		ar.aggregateGlobalVersion = event.GlobalVersion
	}
}

func (ar *AggregateRoot) setInternals(id uuid.UUID, version, globalVersion Version) {
	ar.aggregateID = id
	ar.aggregateVersion = version
	ar.aggregateGlobalVersion = globalVersion
	ar.aggregateEvents = []Event{}
}

func (ar *AggregateRoot) nextVersion() Version {
	return ar.Version() + 1
}

// update sets the AggregateVersion and AggregateGlobalVersion to the values in the last event
// This function is called after the aggregate is saved in the repository
func (ar *AggregateRoot) update() {
	if len(ar.aggregateEvents) > 0 {
		lastEvent := ar.aggregateEvents[len(ar.aggregateEvents)-1]
		ar.aggregateVersion = lastEvent.Version
		ar.aggregateGlobalVersion = lastEvent.GlobalVersion
		ar.aggregateEvents = []Event{}
	}
}

// path return the full name of the aggregate making it unique to other aggregates with
// the same name but placed in other packages.
func (ar *AggregateRoot) path() string {
	return reflect.TypeOf(ar).Elem().PkgPath()
}

// SetID opens up the possibility to set manual aggregate ID from the outside
func (ar *AggregateRoot) SetID(id uuid.UUID) error {
	if ar.aggregateID != emptyAggregateID {
		return ErrAggregateAlreadyExists
	}
	ar.aggregateID = id
	return nil
}

// ID returns the aggregate ID as a string
func (ar *AggregateRoot) ID() uuid.UUID {
	return ar.aggregateID
}

// Root returns the included Aggregate Root state, and is used from the interface Aggregate.
func (ar *AggregateRoot) Root() *AggregateRoot {
	return ar
}

// Version return the version based on events that are not stored
func (ar *AggregateRoot) Version() Version {
	if len(ar.aggregateEvents) > 0 {
		return ar.aggregateEvents[len(ar.aggregateEvents)-1].Version
	}
	return ar.aggregateVersion
}

// GlobalVersion returns the global version based on the last stored event
func (ar *AggregateRoot) GlobalVersion() Version {
	return ar.aggregateGlobalVersion
}

// Events return the aggregate events from the aggregate
// make a copy of the slice preventing outsiders modifying events.
func (ar *AggregateRoot) Events() []Event {
	e := make([]Event, len(ar.aggregateEvents))
	copy(e, ar.aggregateEvents)
	return e
}

// UnsavedEvents return true if there's unsaved events on the aggregate
func (ar *AggregateRoot) UnsavedEvents() bool {
	return len(ar.aggregateEvents) > 0
}
