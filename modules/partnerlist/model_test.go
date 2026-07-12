package partnerlist

import "testing"

func TestLanguageAndRawListParsingAreSeparated(t *testing.T) {
	langs := parseStringList(`["zh-CN","English","缅甸语"]`, 5)
	if len(langs) != 3 || langs[0] != "zh" || langs[1] != "en" || langs[2] != "my" {
		t.Fatalf("unexpected languages: %#v", langs)
	}
	images := parseRawStringList(`["https://cdn.example/a.webp","https://cdn.example/b.webp"]`, 9)
	if len(images) != 2 || images[0] != "https://cdn.example/a.webp" {
		t.Fatalf("image URLs were changed: %#v", images)
	}
}

func TestInterleaveKeepsDailyListBound(t *testing.T) {
	kept := make([]string, 60)
	added := make([]string, 20)
	for i := range kept {
		kept[i] = "k" + string(rune('A'+i))
	}
	for i := range added {
		added[i] = "n" + string(rune('A'+i))
	}
	out := interleaveIDs(kept, added, InitialListLimit)
	if len(out) != 80 {
		t.Fatalf("got %d ids", len(out))
	}
}

func TestUnknownLanguageIsRejected(t *testing.T) {
	langs := parseStringList(`["zh","not-a-language","my"]`, 5)
	if len(langs) != 2 || langs[0] != "zh" || langs[1] != "my" {
		t.Fatalf("unexpected languages: %#v", langs)
	}
}
