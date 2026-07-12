package partnerlist

import "time"

var businessLocation = time.FixedZone("Asia/Shanghai", 8*60*60)

func recommendationDayKey(now time.Time) string {
	return now.In(businessLocation).Add(-4 * time.Hour).Format("2006-01-02")
}

func previousDayKey(dayKey string) string {
	day, err := time.ParseInLocation("2006-01-02", dayKey, businessLocation)
	if err != nil {
		return dayKey
	}
	return day.AddDate(0, 0, -1).Format("2006-01-02")
}

func nextRecommendationBoundary(now time.Time) time.Time {
	local := now.In(businessLocation)
	boundary := time.Date(local.Year(), local.Month(), local.Day(), 4, 0, 0, 0, businessLocation)
	if !local.Before(boundary) {
		boundary = boundary.AddDate(0, 0, 1)
	}
	return boundary
}

func rotationDeadline(firstServedAt int64) int64 {
	if firstServedAt <= 0 {
		return 0
	}
	rotateAt := time.UnixMilli(firstServedAt).Add(rotateAfter)
	nextBoundary := nextRecommendationBoundary(time.UnixMilli(firstServedAt))
	if rotateAt.After(nextBoundary) {
		return nextBoundary.UnixMilli()
	}
	return rotateAt.UnixMilli()
}
