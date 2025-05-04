package database

import (
	"database/sql"

	_ "github.com/marcboeker/go-duckdb/v2"
)

func Init() (*sql.DB, error) {
	db, err := sql.Open("duckdb", "codescene.db")
	if err != nil {
		return nil, err
	}

	// TODO: Do I need a primary key for the filestates?
	createTablesStmt := `
				CREATE TABLE IF NOT EXISTS commits (
					hash TEXT PRIMARY KEY UNIQUE,
					contributor TEXT NOT NULL,
					author_date INTEGER NOT NULL,
					project TEXT NOT NULL
				);
				CREATE TABLE IF NOT EXISTS filestates (
					commit_hash TEXT NOT NULL REFERENCES commits(hash),
					path TEXT NOT NULL,
					language TEXT NOT NULL,
					sloc INTEGER NOT NULL,
					cloc INTEGER NOT NULL,
					blank INTEGER NOT NULL,
					lines_added INTEGER NOT NULL,
					lines_removed INTEGER NOT NULL,
					complexity INTEGER NOT NULL,
					blame_authors TEXT[] NOT NULL,
					blame_dates INTEGER[] NOT NULL
				);`

	if _, err = db.Exec(createTablesStmt); err != nil {
		return nil, err
	}

	return db, nil
}

func Clean(db *sql.DB, repo string) error {
	deleteFileStatesStmt := `
    DELETE FROM filestates
    WHERE commit_hash IN (
        SELECT hash
        FROM commits
        WHERE project = ?
    );`
	if _, err := db.Exec(deleteFileStatesStmt, repo); err != nil {
		return err
	}

	deleteCommitsStmt := `
    DELETE FROM commits
    WHERE project = ?;`
	if _, err := db.Exec(deleteCommitsStmt, repo); err != nil {
		return err
	}

	return nil
}
