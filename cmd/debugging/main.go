package main

import (
	"encoding/json"
	"fmt"
	"os"
	"slices"

	"github.com/boyter/scc/v3/processor"
	"github.com/tim-hilt/codescene/internal/database"
)

type Language struct {
	Files []processor.FileJob
}

func main() {
	db, err := database.Init()
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// repo := "github.com/go-git/go-git"
	// hashes, err := db.GetCommitHashes(repo)
	// if err != nil {
	// 	panic(err)
	// }

	// repository, err := git.Clone(repo, time.Time{})
	// if err != nil {
	// 	panic(err)
	// }
	// defer repository.Close()

	// totalFilestates, totalFiles := 0, 0

	// for i, hash := range hashes {
	filestates, err := db.GetFilestatesWithHash("bff56c6f3fa89752bfac153d104b197189075adb")
	if err != nil {
		panic(err)
	}
	var filestateNames []string
	for _, filestate := range filestates {
		filestateNames = append(filestateNames, filestate.Filename)
	}
	slices.Sort(filestateNames)

	bytes, err := os.ReadFile("scc.json")
	if err != nil {
		panic(err)
	}

	var languageSummaries []Language
	err = json.Unmarshal(bytes, &languageSummaries)
	if err != nil {
		panic(err)
	}

	filejobNames := extractNames(languageSummaries)
	slices.Sort(filejobNames)

	matches := 0
	for _, filejobName := range filejobNames {
		for _, filestateName := range filestateNames {
			if filejobName == filestateName {
				matches++
			}
		}
	}

	aDiffB, bDiffA := diff(filejobNames, filestateNames)

	for _, language := range languageSummaries {
		for _, filejob := range language.Files {
			for i, filestate := range filestates {
				if filejob.Location == filestate.Filename && filejob.Complexity != filestate.Complexity {
					fmt.Printf("%d %s %d %d\n", i, filejob.Location, filejob.Complexity, filestate.Complexity)
					return
				}
			}
		}
	}

	fmt.Printf("%d, %d, %d\n", len(filejobNames), len(filestates), matches)
	fmt.Println(aDiffB)
	fmt.Println(bDiffA)

	// 	files, err := repository.Files(hash)
	// 	if err != nil {
	// 		panic(err)
	// 	}

	// 	var discrepancy []string
	// Outer:
	// 	for _, filestate := range filestates {
	// 		for _, file := range files {
	// 			if filestate.Filename == file {
	// 				continue Outer
	// 			}
	// 		}
	// 		discrepancy = append(discrepancy, filestate.Filename)
	// 	}

	// 	if len(discrepancy) > 0 {
	// 		fmt.Printf("%d\t%s\t%d\t%d\t%s\n", i, hash, len(filestates), len(files), discrepancy)
	// 	}

	// 	totalFilestates += len(filestates)
	// 	totalFiles += len(files)
	// }

	// fmt.Println("total filestates", totalFilestates)
	// fmt.Println("total files", totalFiles)
}

func extractNames(languageSummaries []Language) []string {
	var filejobNames []string

	for _, languageSummary := range languageSummaries {
		for _, filejob := range languageSummary.Files {
			filejobNames = append(filejobNames, filejob.Location)
		}
	}

	return filejobNames
}

func diff(a, b []string) (aDiffB, bDiffA []string) {
	setA := make(map[string]struct{})
	setB := make(map[string]struct{})

	for _, item := range a {
		setA[item] = struct{}{}
	}
	for _, item := range b {
		setB[item] = struct{}{}
	}

	for _, item := range a {
		if _, found := setB[item]; !found {
			aDiffB = append(aDiffB, item)
		}
	}

	for _, item := range b {
		if _, found := setA[item]; !found {
			bDiffA = append(bDiffA, item)
		}
	}

	return
}
