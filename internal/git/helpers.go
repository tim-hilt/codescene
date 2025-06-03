package git

import (
	"regexp"
	"strconv"
	"strings"

	"github.com/boyter/scc/v3/processor"
	"github.com/tim-hilt/codescene/internal/database"
)

func parseCommit(commitString string) (database.Commit, error) {
	commitData := strings.SplitN(commitString, ";", 4)

	return database.Commit{
		Hash:    commitData[0],
		Date:    commitData[1],
		Author:  commitData[2],
		Message: commitData[3],
	}, nil
}

func parseFilestates(filechanges []string) ([]database.FileState, error) {
	var filestates []database.FileState
	for _, filechange := range filechanges {
		filechangeParts := strings.Split(filechange, "\t")
		if filechangeParts[0] == "-" && filechangeParts[1] == "-" {
			// Don't record binary files, skip empty lines
			continue
		}
		linesAdded, err := strconv.ParseInt(filechangeParts[0], 10, 32)
		if err != nil {
			return nil, err
		}
		linesDeleted, err := strconv.ParseInt(filechangeParts[1], 10, 32)
		if err != nil {
			return nil, err
		}
		renameFrom, path := getRenamedPaths(filechangeParts[2])
		filestates = append(filestates, database.FileState{
			LinesAdded:   linesAdded,
			LinesDeleted: linesDeleted,
			RenameFrom:   renameFrom,
			FileJob: &processor.FileJob{
				Filename: path,
			},
		})
	}
	return filestates, nil
}

// TODO: Could I spare calculations for renamed files? Or would there be a data race?
func getRenamedPaths(path string) (string, string) {
	// Case 1: {... => ...} within a path segment
	reBrace := regexp.MustCompile(`\{([^{}]*) => ([^{}]*)\}`)
	if reBrace.MatchString(path) {
		match := reBrace.FindStringSubmatch(path)
		prefix := path[:strings.Index(path, "{")]
		suffix := path[strings.LastIndex(path, "}")+1:]
		oldPath := prefix + match[1] + suffix
		newPath := prefix + match[2] + suffix
		return oldPath, newPath
	}

	// Case 2: Full-path rename with => separator
	reArrow := regexp.MustCompile(`^(.*) => (.*)$`)
	if reArrow.MatchString(path) {
		match := reArrow.FindStringSubmatch(path)
		return strings.TrimSpace(match[1]), strings.TrimSpace(match[2])
	}

	return "", path
}
