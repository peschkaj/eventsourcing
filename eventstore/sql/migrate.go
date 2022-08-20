package sql

import "context"

const createTable = `CREATE TABLE events (event_id UUID PRIMARY KEY, aggregate_id UUID NOT NULL, version INTEGER, reason VARCHAR, type VARCHAR, timestamp VARCHAR, data BLOB, metadata BLOB);`

// Migrate the database
func (s *SQL) Migrate() error {
	sqlStmt := []string{
		createTable,
		`CREATE UNIQUE INDEX aggregate_id_type_version ON events(aggregate_id, type, version);`,
		`CREATE INDEX aggregate_id_type ON events (aggregate_id, type);`,
	}
	return s.migrate(sqlStmt)
}

// MigrateTest remove the index that the test sql driver does not support
func (s *SQL) MigrateTest() error {
	return s.migrate([]string{createTable})
}

func (s *SQL) migrate(stm []string) error {
	tx, err := s.db.BeginTx(context.Background(), nil)
	if err != nil {
		return nil
	}
	defer tx.Rollback()
	for _, b := range stm {
		_, err := tx.Exec(b)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}
