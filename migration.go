// Holds metadata about a migration.

package gomigrate

import (
	"path/filepath"
)

// Migration statuses.
const (
	Inactive = iota
	Active
)

// Holds configuration information for a given migration.
type Migration struct {
	DownPath string
	Id       uint64
	Name     string
	Status   int
	UpPath   string
}

// Performs a basic validation of a migration.
func (m *Migration) valid() bool {
	if m.Id != 0 && m.Name != "" && m.UpPath != "" && m.DownPath != "" {
		return true
	}
	return false
}

type MigrationSource interface {
	// Finds the migrations.
	//
	// The resulting slice of migrations should be sorted by Id.
	FindMigrations(logger Logger) (map[uint64]*Migration, error)
}

type FileMigrationSource struct {
	Dir string
}

func (f FileMigrationSource) FindMigrations(logger Logger) (map[uint64]*Migration, error) {
	// Normalize the migrations path.
	path := []byte(f.Dir)
	pathLength := len(path)
	if path[pathLength-1] != '/' {
		path = append(path, '/')
	}

	logger.Printf("Migrations path: %s", path)
	pathGlob := append([]byte(f.Dir), []byte("*")...)

	matches, err := filepath.Glob(string(pathGlob))
	if err != nil {
		logger.Fatalf("Error while globbing migrations: %v", err)
	}
	ms := make(map[uint64]*Migration)
	for _, match := range matches {
		num, migrationType, name, err := parseMigrationPath(filepath.Base(match))
		if err != nil {
			logger.Printf("Invalid migration file found: %s", match)
			continue
		}

		logger.Printf("Migration file found: %s", match)

		migration, ok := ms[num]
		if !ok {
			migration = &Migration{Id: num, Name: name, Status: Inactive}
			ms[num] = migration
		}
		if migrationType == upMigration {
			migration.UpPath = match
		} else {
			migration.DownPath = match
		}
	}

	// Validate each migration.
	for _, migration := range ms {
		if !migration.valid() {
			path := migration.UpPath
			if path == "" {
				path = migration.DownPath
			}
			logger.Printf("Invalid migration pair for path: %s", path)
			return ms, InvalidMigrationPair
		}
	}

	logger.Printf("Migrations file pairs found: %v", len(ms))

	return ms, nil
}

type AssetMigrationSource struct {
	// Asset should return content of file in path if exists
	Asset func(path string) ([]byte, error)

	// AssetDir should return list of files in the path
	AssetDir func(path string) ([]string, error)

	// Path in the bindata to use.
	Dir string
}

func (a AssetMigrationSource) FindMigrations(logger Logger) (map[uint64]*Migration, error) {
	files, err := a.AssetDir(a.Dir)
	if err != nil {
		return nil, err
	}

	ms := make(map[uint64]*Migration)
	for _, match := range files {
		num, migrationType, name, err := parseMigrationPath(match)
		if err != nil {
			logger.Printf("Invalid migration file found: %s", match)
			continue
		}

		logger.Printf("Migration file found: %s", match)

		migration, ok := ms[num]
		if !ok {
			migration = &Migration{Id: num, Name: name, Status: Inactive}
			ms[num] = migration
		}
		if migrationType == upMigration {
			migration.UpPath = match
		} else {
			migration.DownPath = match
		}
	}

	// Validate each migration.
	for _, migration := range ms {
		if !migration.valid() {
			path := migration.UpPath
			if path == "" {
				path = migration.DownPath
			}
			logger.Printf("Invalid migration pair for path: %s", path)
			return ms, InvalidMigrationPair
		}
	}

	logger.Printf("Migrations file pairs found: %v", len(ms))

	return ms, nil
}
