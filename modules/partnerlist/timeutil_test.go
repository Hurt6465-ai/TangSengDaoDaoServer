package partnerlist

import (
	"testing"
	"time"
)

func TestRecommendationDayKeyUsesFourAMBoundary(t *testing.T) {
	before := time.Date(2026, 7, 12, 3, 59, 0, 0, businessLocation)
	after := time.Date(2026, 7, 12, 4, 0, 0, 0, businessLocation)
	if got := recommendationDayKey(before); got != "2026-07-11" {
		t.Fatalf("before boundary: got %s", got)
	}
	if got := recommendationDayKey(after); got != "2026-07-12" {
		t.Fatalf("after boundary: got %s", got)
	}
}

func TestRotationDeadlineDoesNotCrossRecommendationDay(t *testing.T) {
	first := time.Date(2026, 7, 12, 2, 0, 0, 0, businessLocation)
	deadline := time.UnixMilli(rotationDeadline(first.UnixMilli())).In(businessLocation)
	want := time.Date(2026, 7, 12, 4, 0, 0, 0, businessLocation)
	if !deadline.Equal(want) {
		t.Fatalf("deadline=%v want=%v", deadline, want)
	}
}
