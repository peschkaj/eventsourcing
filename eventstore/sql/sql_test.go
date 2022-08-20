package sql_test

import (
	sqldriver "database/sql"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/hallgren/eventsourcing"
	"github.com/hallgren/eventsourcing/eventstore/sql"
	"github.com/hallgren/eventsourcing/eventstore/suite"
	_ "github.com/proullon/ramsql/driver"
)

var seededRand = rand.New(rand.NewSource(time.Now().UnixNano()))

func TestSuite(t *testing.T) {
	f := func(ser eventsourcing.Serializer) (eventsourcing.EventStore, func(), error) {
		// use random int to get a new db on each test run
		r := seededRand.Intn(999999999999)
		db, err := sqldriver.Open("ramsql", fmt.Sprintf("%d", r))
		if err != nil {
			return nil, nil, fmt.Errorf("could not open ramsql database %v", err)
		}
		err = db.Ping()
		if err != nil {
			return nil, nil, fmt.Errorf("could not ping database %v", err)
		}

		es := sql.Open(db, ser)
		err = es.MigrateTest()
		if err != nil {
			return nil, nil, fmt.Errorf("could not migrate database %v", err)
		}
		return es, func() {
			es.Close()
		}, nil
	}
	suite.Test(t, f)
}
