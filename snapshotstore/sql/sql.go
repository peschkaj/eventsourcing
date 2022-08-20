package sql

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/gofrs/uuid"
	"github.com/hallgren/eventsourcing"
)

// SQL is the struct holding the underlying database and serializer
type SQL struct {
	db *sql.DB
}

// New returns a SQL struct
func New(db *sql.DB) *SQL {
	return &SQL{
		db: db,
	}
}

// Close the connection
func (s *SQL) Close() {
	s.db.Close()
}

// Get retrieves the persisted snapshot
func (s *SQL) Get(ctx context.Context, id uuid.UUID, typ string) (eventsourcing.Snapshot, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return eventsourcing.Snapshot{}, err
	} else if ctx.Err() != nil {
		return eventsourcing.Snapshot{}, ctx.Err()
	}
	defer tx.Rollback()

	statement := `SELECT state, version, global_version FROM snapshots WHERE id=$1 AND type=$2 LIMIT 1`
	var state []byte
	var version uint64
	var globalVersion uint64
	err = tx.QueryRowContext(ctx, statement, id, typ).Scan(&state, &version, &globalVersion)
	if err != nil && err != sql.ErrNoRows {
		return eventsourcing.Snapshot{}, err
	} else if err == sql.ErrNoRows {
		return eventsourcing.Snapshot{}, eventsourcing.ErrSnapshotNotFound
	} else if ctx.Err() != nil {
		return eventsourcing.Snapshot{}, ctx.Err()
	}
	snap := eventsourcing.Snapshot{
		ID:            id,
		Type:          typ,
		State:         state,
		Version:       eventsourcing.Version(version),
		GlobalVersion: eventsourcing.Version(globalVersion),
	}
	return snap, nil
}

// Save persists the snapshot
func (s *SQL) Save(snap eventsourcing.Snapshot) error {
	tx, err := s.db.BeginTx(context.Background(), nil)
	if err != nil {
		return fmt.Errorf("could not start a write transaction, %v", err)
	}
	defer tx.Rollback()

	statement := `SELECT id FROM snapshots WHERE id=$1 AND type=$2 LIMIT 1`
	var id string
	err = tx.QueryRow(statement, snap.ID, snap.Type).Scan(&id)
	if err != nil && err != sql.ErrNoRows {
		return err
	}
	if err == sql.ErrNoRows {
		// insert
		statement = `INSERT INTO snapshots (state, id, type, version, global_version) VALUES ($1, $2, $3, $4, $5)`
		_, err = tx.Exec(statement, string(snap.State), snap.ID, snap.Type, snap.Version, snap.GlobalVersion)
		if err != nil {
			return err
		}
	} else {
		// update
		statement = `UPDATE snapshots SET state=$1, version=$2, global_version=$3 WHERE id=$4 AND type=$5`
		_, err = tx.Exec(statement, string(snap.State), snap.Version, snap.GlobalVersion, snap.ID, snap.Type)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}
