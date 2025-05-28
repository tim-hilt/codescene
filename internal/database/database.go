package database

import (
	"cmp"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"slices"
	"time"

	"github.com/boyter/scc/v3/processor"
	"github.com/marcboeker/go-duckdb/v2"
)

var ErrProjectNotFound = errors.New("project not found")

type FileState struct {
	CommitHash   string
	RenameFrom   string
	LinesAdded   int64
	LinesDeleted int64
	*processor.FileJob
}

type Commit struct {
	Hash    string
	Author  string
	Message string
	Date    string
	Project string
}

type DB struct {
	*sql.DB
	filestatesAppender *duckdb.Appender
	commitsAppender    *duckdb.Appender
	driver.Conn
}

func (db *DB) Close() error {
	if err := db.commitsAppender.Close(); err != nil {
		return err
	}

	if err := db.filestatesAppender.Close(); err != nil {
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
	createTablesStmt := `
	            CREATE SEQUENCE id_sequence START 1;
				CREATE TABLE IF NOT EXISTS commits (
					id INTEGER PRIMARY KEY DEFAULT nextval('id_sequence'),
					hash TEXT UNIQUE,
					contributor TEXT NOT NULL,
					author_date TIMESTAMP_S NOT NULL,
					project TEXT NOT NULL,
					message TEXT NOT NULL,
				);
				CREATE TABLE IF NOT EXISTS filestates (
					commit_hash TEXT NOT NULL REFERENCES commits(hash),
					path TEXT NOT NULL,
					rename_from TEXT,
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

	filestatesAppender, err := duckdb.NewAppenderFromConn(con, "", "filestates")
	if err != nil {
		return nil, err
	}

	commitsAppender, err := duckdb.NewAppenderFromConn(con, "", "commits")
	if err != nil {
		return nil, err
	}

	return &DB{db, filestatesAppender, commitsAppender, con}, nil
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

func (db DB) GetProjects() ([]string, error) {
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

func (db DB) GetNewestCommitDate(repo string) (time.Time, error) {
	var authorDate time.Time
	err := db.QueryRow("SELECT author_date FROM commits WHERE project = ? ORDER BY author_date DESC LIMIT 1", repo).Scan(&authorDate)

	if err != nil && err != sql.ErrNoRows {
		// If no rows are in the result-set, then the project hasn't been analyzed yet
		return authorDate, err
	}

	return authorDate, nil
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

func (db DB) GetProjectMetadata(project string) (ProjectMetadata, error) {
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

func (db *DB) PersistCommits(commits chan Commit, errs chan error) {
	for commit := range commits {
		date, err := time.Parse(time.RFC3339, commit.Date)
		if err != nil {
			errs <- err
			return
		}
		if err = db.commitsAppender.AppendRow(commit.Hash, commit.Author, date, commit.Project, commit.Message); err != nil {
			errs <- err
			return
		}
	}

	if err := db.commitsAppender.Flush(); err != nil {
		errs <- err
		return
	}
}

func (db *DB) PersistFileStates(filestates chan FileState, numFilestates int, filestateProcessedCallback func(curr, total int), errs chan error) {
	var (
		err error
		i   int
	)

	for filestate := range filestates {
		err = db.filestatesAppender.AppendRow(
			filestate.CommitHash,
			filestate.Filename,
			filestate.RenameFrom,
			filestate.Language,
			int32(filestate.LinesAdded),
			int32(filestate.LinesDeleted),
			int32(filestate.Code),
			int32(filestate.Comment),
			int32(filestate.Blank),
			int32(filestate.Complexity),
		)
		if err != nil {
			errs <- err
			return
		}
		i++
		filestateProcessedCallback(i, numFilestates)
	}
}

func (db *DB) Flush() error {
	if err := db.filestatesAppender.Flush(); err != nil {
		return err
	}

	return nil
}
func (db *DB) FillFilestates(repo string, removedFiles map[string][]string) error {
	hashes, err := db.getCommitHashes(repo)
	if err != nil {
		return err
	}

	var filestatesLastCommit []FileState
	for _, hash := range hashes {
		filestatesCurrentCommit, err := db.getFilestatesWithHash(hash)
		if err != nil {
			return err
		}

		var relevantFilestatesLastCommit []FileState

	ProcessFilestatesLastCommit:
		for _, filestateLastCommit := range filestatesLastCommit {
			// If f is removed in this commit, continue
			if rfs, exist := removedFiles[hash]; exist {
				for _, rf := range rfs {
					if rf == filestateLastCommit.Filename {
						continue ProcessFilestatesLastCommit
					}
				}
			}
			// If f is (renamed) in filestates, continue
			for _, filestateCurrentCommit := range filestatesCurrentCommit {
				if filestateCurrentCommit.Filename == filestateLastCommit.Filename || filestateCurrentCommit.RenameFrom == filestateLastCommit.Filename {
					continue ProcessFilestatesLastCommit
				}
			}

			if err := db.filestatesAppender.AppendRow(
				hash,
				filestateLastCommit.Filename,
				filestateLastCommit.RenameFrom,
				filestateLastCommit.Language,
				int32(filestateLastCommit.LinesAdded),
				int32(filestateLastCommit.LinesDeleted),
				int32(filestateLastCommit.Code),
				int32(filestateLastCommit.Comment),
				int32(filestateLastCommit.Blank),
				int32(filestateLastCommit.Complexity),
			); err != nil {
				return err
			}

			relevantFilestatesLastCommit = append(relevantFilestatesLastCommit, filestateLastCommit)
		}

		filestatesLastCommit = append(filestatesCurrentCommit, relevantFilestatesLastCommit...)
	}

	return db.Flush()
}

func (db DB) getCommitHashes(repo string) ([]string, error) {
	query := "SELECT hash from commits WHERE project = ? ORDER BY id"
	rows, err := db.Query(query, repo)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var hashes []string
	for rows.Next() {
		var hash string
		if err := rows.Scan(&hash); err != nil {
			return nil, err
		}
		hashes = append(hashes, hash)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return hashes, nil
}

func (db DB) getFilestatesWithHash(hash string) ([]FileState, error) {
	query := "SELECT * from filestates WHERE commit_hash = ?"
	rows, err := db.Query(query, hash)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var filestates []FileState
	for rows.Next() {
		f := FileState{
			FileJob: &processor.FileJob{},
		}
		if err := rows.Scan(
			&f.CommitHash,
			&f.Filename,
			&f.RenameFrom,
			&f.Language,
			&f.Code,
			&f.Comment,
			&f.Blank,
			&f.Complexity,
			&f.LinesAdded,
			&f.LinesDeleted,
		); err != nil {
			return nil, err
		}
		filestates = append(filestates, f)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return filestates, nil
}
