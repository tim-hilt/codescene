package database

import "time"

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
