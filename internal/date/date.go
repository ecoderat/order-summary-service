package date

import "time"

// ToStartOfDay rounds down a date with time to the start of the day in UTC.
func ToStartOfDay(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}
