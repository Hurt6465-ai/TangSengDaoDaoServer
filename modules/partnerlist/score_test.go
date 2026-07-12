package partnerlist

import "testing"

func TestLanguageScoreTakesHighestRuleOnly(t *testing.T) {
	viewer := &viewerProfile{UID: "a", NativeLanguages: []string{"zh"}, LearningLanguages: []string{"my", "en"}, PrimaryLearning: "my"}
	candidate := &ListUser{UID: "b", NativeLanguages: []string{"my"}, LearningLanguages: []string{"zh"}}
	if got := languageMatchScore(viewer, candidate); got != 40 {
		t.Fatalf("mutual score=%v", got)
	}
}

func TestStableRandomIsDeterministic(t *testing.T) {
	a := normalizedHash("viewer:candidate:1", 8)
	b := normalizedHash("viewer:candidate:1", 8)
	if a != b || a < 0 || a > 8 {
		t.Fatalf("a=%v b=%v", a, b)
	}
}
