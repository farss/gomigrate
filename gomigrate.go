// A simple database migrator for PostgreSQL.

package gomigrate

import (
	"database/sql"
	"errors"
	"io/ioutil"
	"sort"
)

type migrationType string

const (
	migrationTableName = "gomigrate"
	upMigration        = migrationType("up")
	downMigration      = migrationType("down")
)

var (
	InvalidMigrationFile  = errors.New("Invalid migration file")
	InvalidMigrationPair  = errors.New("Invalid pair of migration files")
	InvalidMigrationsPath = errors.New("Invalid migrations path")
	InvalidMigrationType  = errors.New("Invalid migration type")
	NoActiveMigrations    = errors.New("No active migrations to rollback")
)

type Migrator struct {
	DB         *sql.DB
	dbAdapter  Migratable
	migrations map[uint64]*Migration
	logger     Logger
	Source     MigrationSource
}

type Logger interface {
	Print(v ...interface{})
	Printf(format string, v ...interface{})
	Println(v ...interface{})
	Fatalf(format string, v ...interface{})
}

// Returns true if the migration table already exists.
func (m *Migrator) MigrationTableExists() (bool, error) {
	row := m.DB.QueryRow(m.dbAdapter.SelectMigrationTableSql(), migrationTableName)
	var tableName string
	err := row.Scan(&tableName)
	if err == sql.ErrNoRows {
		m.logger.Print("Migrations table not found")
		return false, nil
	}
	if err != nil {
		m.logger.Printf("Error checking for migration table: %v", err)
		return false, err
	}
	m.logger.Print("Migrations table found")
	return true, nil
}

// Creates the migrations table if it doesn't exist.
func (m *Migrator) CreateMigrationsTable() error {
	_, err := m.DB.Exec(m.dbAdapter.CreateMigrationTableSql())
	if err != nil {
		m.logger.Fatalf("Error creating migrations table: %v", err)
	}

	m.logger.Printf("Created migrations table: %s", migrationTableName)

	return nil
}

// Returns a new migrator with the specified logger.
func NewMigratorWithLogger(db *sql.DB, adapter Migratable, ms MigrationSource, logger Logger) (*Migrator, error) {

	migrator := Migrator{
		db,
		adapter,
		make(map[uint64]*Migration),
		logger,
		ms,
	}

	// Create the migrations table if it doesn't exist.
	tableExists, err := migrator.MigrationTableExists()
	if err != nil {
		return nil, err
	}
	if !tableExists {
		if err := migrator.CreateMigrationsTable(); err != nil {
			return nil, err
		}
	}

	// Get all metadata from the database.
	migrator.migrations, err = migrator.Source.FindMigrations(logger)
	if err != nil {
		return nil, err
	}
	if err := migrator.getMigrationStatuses(); err != nil {
		return nil, err
	}

	return &migrator, nil
}

// Queries the migration table to determine the status of each
// migration.
func (m *Migrator) getMigrationStatuses() error {
	for _, migration := range m.migrations {
		row := m.DB.QueryRow(m.dbAdapter.GetMigrationSql(), migration.Id)
		var mid uint64
		err := row.Scan(&mid)
		if err == sql.ErrNoRows {
			continue
		}
		if err != nil {
			m.logger.Printf(
				"Error getting migration status for %s: %v",
				migration.Name,
				err,
			)
			return err
		}
		migration.Status = Active
	}
	return nil
}

// Returns a sorted list of migration ids for a given status. -1 returns
// all migrations.
func (m *Migrator) Migrations(status int) []*Migration {
	// Sort all migration ids.
	ids := make([]uint64, 0)
	for id, _ := range m.migrations {
		ids = append(ids, id)
	}
	sort.Sort(uint64slice(ids))

	// Find ids for the given status.
	migrations := make([]*Migration, 0)
	for _, id := range ids {
		migration := m.migrations[id]
		if status == -1 || migration.Status == status {
			migrations = append(migrations, migration)
		}
	}
	return migrations
}

// Applies a single migration.
func (m *Migrator) ApplyMigration(migration *Migration, mType migrationType) error {
	var path string
	if mType == upMigration {
		path = migration.UpPath
	} else if mType == downMigration {
		path = migration.DownPath
	} else {
		return InvalidMigrationType
	}

	m.logger.Printf("Applying migration: %s", path)

	var sql []byte
	var err error

	switch m.Source.(type) {
	case *FileMigrationSource:
		sql, err = ioutil.ReadFile(path)
	case *AssetMigrationSource:
		sql, err = m.Source.(*AssetMigrationSource).Asset(path)
	default:
		m.logger.Println("Unsupport MigrationSource type")
		return errors.New("Unsupport MigrationSource type")
	}
	if err != nil {
		m.logger.Printf("Error reading migration: %s", path)
		return err
	}
	transaction, err := m.DB.Begin()
	if err != nil {
		m.logger.Printf("Error opening transaction: %v", err)
		return err
	}

	// Certain adapters can not handle multiple sql commands in one file so we need the adapter to split up the command
	commands := m.dbAdapter.GetMigrationCommands(string(sql))

	// Perform the migration.
	for _, cmd := range commands {
		result, err := transaction.Exec(cmd)
		if err != nil {
			m.logger.Printf("Error executing migration: %v", err)
			if rollbackErr := transaction.Rollback(); rollbackErr != nil {
				m.logger.Printf("Error rolling back transaction: %v", rollbackErr)
				return rollbackErr
			}
			return err
		}
		if result != nil {
			if rowsAffected, err := result.RowsAffected(); err != nil {
				m.logger.Printf("Error getting rows affected: %v", err)
				if rollbackErr := transaction.Rollback(); rollbackErr != nil {
					m.logger.Printf("Error rolling back transaction: %v", rollbackErr)
					return rollbackErr
				}
				return err
			} else {
				m.logger.Printf("Rows affected: %v", rowsAffected)
			}
		}
	}

	// Log the event.
	if mType == upMigration {
		_, err = transaction.Exec(
			m.dbAdapter.MigrationLogInsertSql(),
			migration.Id,
		)
	} else {
		_, err = transaction.Exec(
			m.dbAdapter.MigrationLogDeleteSql(),
			migration.Id,
		)
	}
	if err != nil {
		m.logger.Printf("Error logging migration: %v", err)
		if rollbackErr := transaction.Rollback(); rollbackErr != nil {
			m.logger.Printf("Error rolling back transaction: %v", rollbackErr)
			return rollbackErr
		}
		return err
	}

	// Commit and update the struct status.
	if err := transaction.Commit(); err != nil {
		m.logger.Printf("Error commiting transaction: %v", err)
		return err
	}
	if mType == upMigration {
		migration.Status = Active
	} else {
		migration.Status = Inactive
	}

	return nil
}

// Applies all inactive migrations.
func (m *Migrator) Migrate() error {
	for _, migration := range m.Migrations(Inactive) {
		if err := m.ApplyMigration(migration, upMigration); err != nil {
			return err
		}
	}
	return nil
}

// Rolls back the last migration.
func (m *Migrator) Rollback() error {
	return m.RollbackN(1)
}

// Rolls back N migrations.
func (m *Migrator) RollbackN(n int) error {
	migrations := m.Migrations(Active)
	if len(migrations) == 0 {
		return nil
	}

	last_migration := len(migrations) - 1 - n

	for i := len(migrations) - 1; i != last_migration; i-- {
		if err := m.ApplyMigration(migrations[i], downMigration); err != nil {
			return err
		}
	}

	return nil
}

// Rolls back all migrations.
func (m *Migrator) RollbackAll() error {
	migrations := m.Migrations(Active)
	return m.RollbackN(len(migrations))
}
