package database

import (
	"cmp"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/marcboeker/go-duckdb/v2"
)

var ErrProjectNotFound = errors.New("project not found")

type DB struct {
	*sql.DB
	CommitsAppender    *duckdb.Appender
	FilestatesAppender *duckdb.Appender
	driver.Conn
}

func (db *DB) Close() error {
	if err := db.CommitsAppender.Close(); err != nil {
		return err
	}

	if err := db.FilestatesAppender.Close(); err != nil {
		return err
	}

	if err := db.Conn.Close(); err != nil {
		return err
	}

	if err := db.DB.Close(); err != nil {
		return err
	}

	return nil
}

func Init() (*DB, error) {
	c, err := duckdb.NewConnector("codescene.db", nil)
	if err != nil {
		return nil, err
	}

	con, err := c.Connect(context.Background())
	if err != nil {
		return nil, err
	}

	db := sql.OpenDB(c)

	// TODO: Do I need a primary key for the filestates?
	// TODO: Do I need the foreign key from filestates to commits?
	createTablesStmt := `
				CREATE TABLE IF NOT EXISTS commits (
					hash TEXT PRIMARY KEY UNIQUE,
					contributor TEXT NOT NULL,
					author_date TIMESTAMP_S NOT NULL,
					project TEXT NOT NULL,
					message TEXT NOT NULL,
				);
				CREATE TABLE IF NOT EXISTS filestates (
					commit_hash TEXT NOT NULL,
					path TEXT NOT NULL,
					language TEXT NOT NULL,
					sloc INTEGER NOT NULL,
					cloc INTEGER NOT NULL,
					blank INTEGER NOT NULL,
					complexity INTEGER NOT NULL,
					lines_added INTEGER NOT NULL,
					lines_deleted INTEGER NOT NULL,
				);`

	if _, err = db.Exec(createTablesStmt); err != nil {
		return nil, err
	}

	commitsAppender, err := duckdb.NewAppenderFromConn(con, "", "commits")
	if err != nil {
		return nil, err
	}

	filestatesAppender, err := duckdb.NewAppenderFromConn(con, "", "filestates")
	if err != nil {
		return nil, err
	}

	return &DB{db, commitsAppender, filestatesAppender, con}, nil
}

func (db *DB) Clean(repo string) error {
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

func (db *DB) GetProjects() ([]string, error) {
	rows, err := db.Query("SELECT DISTINCT project FROM commits")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var projects []string
	for rows.Next() {
		var project string
		if err := rows.Scan(&project); err != nil {
			return nil, err
		}
		projects = append(projects, project)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return projects, nil
}

type CommitData struct {
	CommitDate string `json:"commitDate"`
	Sloc       int    `json:"sloc"`
	Complexity int    `json:"complexity"`
}

type ContributorData struct {
	Contributor string `json:"contributor"`
	Commits     int    `json:"commits"`
}

type CommitFrequency struct {
	Day     string `json:"day"`
	Commits int    `json:"commits"`
}

type ProjectMetadata struct {
	CommitData      []CommitData      `json:"commitData"`
	ContributorData []ContributorData `json:"contributorData"`
	CommitFrequency []CommitFrequency `json:"commitFrequency"`
}

func (db *DB) GetProjectMetadata(project string) (ProjectMetadata, error) {
	rows, err := db.Query(`
	SELECT 
		c.author_date,
		(SELECT SUM(f.complexity) 
		 FROM filestates f 
		 WHERE f.commit_hash = c.hash) AS total_complexity,
		(SELECT SUM(f.sloc) 
		 FROM filestates f 
		 WHERE f.commit_hash = c.hash) AS total_sloc
	FROM 
		commits c
	WHERE 
		c.project = ?`, project)
	if err != nil {
		return ProjectMetadata{}, err
	}
	defer rows.Close()

	var commitData []CommitData
	for rows.Next() {
		var cd CommitData
		if err := rows.Scan(&cd.CommitDate, &cd.Complexity, &cd.Sloc); err != nil {
			return ProjectMetadata{}, err
		}
		commitData = append(commitData, cd)
	}

	if err := rows.Err(); err != nil {
		return ProjectMetadata{}, err
	}

	if len(commitData) == 0 {
		return ProjectMetadata{}, ErrProjectNotFound
	}

	slices.SortFunc(commitData,
		func(a, b CommitData) int {
			return cmp.Compare(a.CommitDate, b.CommitDate)
		})

	rows, err = db.Query("SELECT contributor, COUNT(*) as num_commits FROM commits WHERE project = ? GROUP BY contributor ORDER BY num_commits DESC", project)
	if err != nil {
		return ProjectMetadata{}, err
	}

	var contributorData []ContributorData
	for rows.Next() {
		var cd ContributorData
		if err := rows.Scan(&cd.Contributor, &cd.Commits); err != nil {
			return ProjectMetadata{}, err
		}
		contributorData = append(contributorData, cd)
	}

	if err := rows.Err(); err != nil {
		return ProjectMetadata{}, err
	}

	var commitFrequency []CommitFrequency
	firstCommit, err := time.Parse(time.RFC3339, commitData[0].CommitDate)
	if err != nil {
		return ProjectMetadata{}, nil
	}
	firstCommit = firstCommit.Truncate(24 * time.Hour)
	today := time.Now().Truncate(24 * time.Hour)
	for d := firstCommit; !d.After(today); d = d.AddDate(0, 0, 1) {
		commits, err := countTimestampsOnDay(commitData, d)
		if err != nil {
			return ProjectMetadata{}, err
		}
		cf := CommitFrequency{
			Day:     d.Format(time.RFC3339),
			Commits: commits,
		}
		commitFrequency = append(commitFrequency, cf)
	}

	return ProjectMetadata{
		CommitData:      commitData,
		ContributorData: contributorData,
		CommitFrequency: commitFrequency,
	}, nil
}

func (db *DB) ImportCSV(filename string) error {
	query := fmt.Sprintf("COPY filestates FROM %s", filename)
	if _, err := db.Exec(query); err != nil {
		return err
	}
	return nil
}

func countTimestampsOnDay(commitData []CommitData, day time.Time) (int, error) {
	day = day.Truncate(24 * time.Hour)
	count := 0

	for _, cd := range commitData {
		t, err := time.Parse(time.RFC3339, cd.CommitDate)
		if err != nil {
			return -1, err
		}
		if t.Truncate(24 * time.Hour).Equal(day) {
			count++
		}
	}
	return count, nil
}
