package git

import (
	"regexp"
	"strconv"
	"strings"

	"github.com/boyter/scc/v3/processor"
	"github.com/tim-hilt/codescene/internal/database"
)

func parseCommit(commitString string) (database.Commit, []database.FileState, error) {
	commitString = strings.TrimSpace(commitString)
	lines := strings.Split(commitString, "\n")

	commitData := strings.Split(strings.Trim(lines[0], "'"), ";")

	commit := database.Commit{
		Hash:    commitData[0],
		Author:  commitData[2],
		Message: commitData[3],
		Date:    commitData[1],
	}

	var filestates []database.FileState

	for _, line := range lines[1:] {
		fileChange := strings.Split(line, "\t")
		if fileChange[0] == "-" && fileChange[1] == "-" || line == "" {
			// Don't record binary files, skip empty lines
			continue
		}
		linesAdded, err := strconv.ParseInt(fileChange[0], 10, 32)
		if err != nil {
			return database.Commit{}, nil, err
		}
		linesDeleted, err := strconv.ParseInt(fileChange[1], 10, 32)
		if err != nil {
			return database.Commit{}, nil, err
		}
		renameFrom, path := getRenamedPaths(fileChange[2])
		filestates = append(filestates, database.FileState{
			CommitHash:   commitData[0],
			RenameFrom:   renameFrom,
			LinesAdded:   linesAdded,
			LinesDeleted: linesDeleted,
			FileJob: &processor.FileJob{
				Filename: path,
			},
		})
	}

	return commit, filestates, nil
}

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
