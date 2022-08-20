package sql

import "context"

const createTable = `CREATE TABLE events (seq INTEGER PRIMARY KEY AUTOINCREMENT, id UUID NOT NULL, version INTEGER, reason VARCHAR, type VARCHAR, timestamp VARCHAR, data BLOB, metadata BLOB);`

// Migrate the database
func (s *SQL) Migrate() error {
	sqlStmt := []string{
		createTable,
		`CREATE UNIQUE INDEX id_type_version ON events(id, type, version);`,
		`CREATE INDEX id_type ON events (id, type);`,
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
