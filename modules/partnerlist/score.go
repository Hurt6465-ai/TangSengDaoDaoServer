package partnerlist

import (
	"hash/fnv"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
)

type scoreContext struct {
	viewer             *viewerProfile
	dayKey             string
	nowMS              int64
	online             map[string]struct{}
	repeatDays         map[string]int
	todayAssign        map[string]int
	yesterdayAssign    map[string]int
	assignmentBaseline float64
	fairnessFactor     float64
	inboxLoads         map[string]int
}

func scoreAndSort(candidates []*ListUser, ctx scoreContext) []*ListUser {
	out := make([]*ListUser, 0, len(candidates))
	for _, candidate := range candidates {
		if candidate == nil || candidate.UID == "" {
			continue
		}
		language := languageMatchScore(ctx.viewer, candidate)
		if language <= 0 {
			continue
		}
		score := language
		score += activeRecommendationScore(candidate, ctx.online, ctx.nowMS)
		score += optionalProfileScore(candidate)
		score += newcomerScore(candidate, ctx.nowMS)
		score += normalizedHash(ctx.viewer.UID+":"+candidate.UID+":"+strconv.Itoa(AlgorithmVersion), 8)
		score += normalizedHash(ctx.viewer.UID+":"+candidate.UID+":"+ctx.dayKey+":"+strconv.Itoa(AlgorithmVersion), 8)
		score -= repeatPenalty(ctx.repeatDays[candidate.UID])
		score -= assignmentPenalty(candidate.UID, ctx)
		score -= inboxPenalty(ctx.inboxLoads[candidate.UID])
		candidate.Score = math.Round(score*100) / 100
		if candidate.ProfileCompletedAtMS > 0 && ctx.nowMS-candidate.ProfileCompletedAtMS <= int64(newcomerWindow/time.Millisecond) {
			candidate.IsNew = 1
		}
		out = append(out, candidate)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Score == out[j].Score {
			return out[i].UID < out[j].UID
		}
		return out[i].Score > out[j].Score
	})
	return out
}

func languageMatchScore(viewer *viewerProfile, candidate *ListUser) float64 {
	if viewer == nil || candidate == nil {
		return 0
	}
	candidateNative := candidate.NativeLanguages
	candidateLearning := candidate.LearningLanguages
	mutual := false
	for _, learning := range viewer.LearningLanguages {
		if containsString(candidateNative, learning) {
			for _, native := range viewer.NativeLanguages {
				if containsString(candidateLearning, native) {
					mutual = true
					break
				}
			}
		}
		if mutual {
			break
		}
	}
	if mutual {
		return 40
	}
	if viewer.PrimaryLearning != "" && containsString(candidateNative, viewer.PrimaryLearning) {
		return 32
	}
	for _, learning := range viewer.LearningLanguages {
		if containsString(candidateNative, learning) {
			return 24
		}
	}
	for _, native := range viewer.NativeLanguages {
		if containsString(candidateLearning, native) {
			return 12
		}
	}
	return 0
}

func activeRecommendationScore(candidate *ListUser, online map[string]struct{}, nowMS int64) float64 {
	if candidate == nil {
		return 0
	}
	if candidate.Online == 1 {
		if _, ok := online[candidate.UID]; ok {
			return 20
		}
	}
	elapsed := time.Duration(nowMS-candidate.LastActiveAt) * time.Millisecond
	switch {
	case candidate.LastActiveAt <= 0:
		return 0
	case elapsed <= 5*time.Minute:
		return 17
	case elapsed <= 10*time.Minute:
		return 15
	case elapsed <= 15*time.Minute:
		return 13
	case elapsed <= 30*time.Minute:
		return 10
	case elapsed <= time.Hour:
		return 8
	case elapsed <= 3*time.Hour:
		return 6
	case elapsed <= 12*time.Hour:
		return 4
	case elapsed <= 24*time.Hour:
		return 3
	case elapsed <= hotWindow:
		return 1
	default:
		return 0
	}
}

func optionalProfileScore(candidate *ListUser) float64 {
	if candidate == nil {
		return 0
	}
	score := 0.0
	introLen := len([]rune(strings.TrimSpace(candidate.Intro)))
	if introLen >= 10 {
		score += 1
	}
	if introLen >= 40 {
		score += 1
	}
	if len(candidate.Tags) >= 2 {
		score += 1
	}
	if len(candidate.Tags) >= 5 {
		score += 1
	}
	if strings.TrimSpace(candidate.CountryCode) != "" {
		score += 1
	}
	if strings.TrimSpace(candidate.Birthday) != "" {
		score += 1
	}
	// Avatar/native/learning fields are eligibility gates, not quality bonuses.
	// Only optional extra photos are rewarded here.
	if len(candidate.ProfileImages) >= 2 {
		score += 1
	}
	if len(candidate.ProfileImages) >= 4 {
		score += 1
	}
	if score > 8 {
		return 8
	}
	return score
}

func newcomerScore(candidate *ListUser, nowMS int64) float64 {
	if candidate == nil || candidate.ProfileCompletedAtMS <= 0 {
		return 0
	}
	age := time.Duration(nowMS-candidate.ProfileCompletedAtMS) * time.Millisecond
	if age <= 24*time.Hour {
		return 8
	}
	if age <= 48*time.Hour {
		return 4
	}
	return 0
}

func repeatPenalty(days int) float64 {
	switch days {
	case 1:
		return 35
	case 2:
		return 22
	case 3:
		return 12
	case 4, 5, 6, 7:
		return 5
	default:
		return 0
	}
}

func assignmentPenalty(uid string, ctx scoreContext) float64 {
	load := float64(ctx.todayAssign[uid]) + float64(ctx.yesterdayAssign[uid])*0.5
	if load <= ctx.assignmentBaseline || ctx.assignmentBaseline <= 0 {
		return 0
	}
	raw := ((load - ctx.assignmentBaseline) / (ctx.assignmentBaseline + 1)) * 6
	raw *= ctx.fairnessFactor
	if raw < 0 {
		return 0
	}
	if raw > 16 {
		return 16
	}
	return raw
}

func inboxPenalty(load int) float64 {
	switch {
	case load <= 3:
		return 0
	case load <= 7:
		return 2
	case load <= 15:
		return 5
	case load <= 30:
		return 9
	default:
		return 14
	}
}

func normalizedHash(value string, max float64) float64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(value))
	return (float64(h.Sum64()%1000000) / 1000000.0) * max
}
