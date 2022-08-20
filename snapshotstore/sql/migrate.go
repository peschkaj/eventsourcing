package sql

import "context"

const createTable = `CREATE TABLE snapshots (id UUID NOT NULL, type VARCHAR, version INTEGER, global_version INTEGER, state BLOB);`

// Migrate the database
func (s *SQL) Migrate() error {
	sqlStmt := []string{
		createTable,
		`CREATE UNIQUE INDEX id_type ON snapshots (id, type);`,
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
