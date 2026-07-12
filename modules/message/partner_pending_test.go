package message

import (
	"strings"
	"testing"
)

func TestNormalizePartnerClientMsgNoRejectsEmptyAndLong(t *testing.T) {
	if _, err := normalizePartnerClientMsgNo("", nil); err != errPartnerClientMsgNoRequired {
		t.Fatalf("empty error=%v", err)
	}
	if _, err := normalizePartnerClientMsgNo(strings.Repeat("x", 101), nil); err != errPartnerClientMsgNoTooLong {
		t.Fatalf("long error=%v", err)
	}
	got, err := normalizePartnerClientMsgNo(" client-1 ", nil)
	if err != nil || got != "client-1" {
		t.Fatalf("got=%q err=%v", got, err)
	}
}

func TestStablePartnerIMClientMsgNoIsDeterministicAndSenderScoped(t *testing.T) {
	a := stablePartnerIMClientMsgNo("u1", "m1")
	b := stablePartnerIMClientMsgNo("u1", "m1")
	c := stablePartnerIMClientMsgNo("u2", "m1")
	if a != b || a == c || !strings.HasPrefix(a, "partner:") || len(a) > 100 {
		t.Fatalf("a=%q b=%q c=%q", a, b, c)
	}
}

func TestCanonicalPayloadHashDetectsMessageChanges(t *testing.T) {
	a, err := canonicalPayloadHash(map[string]interface{}{"type": 1, "content": "hello"})
	if err != nil {
		t.Fatal(err)
	}
	b, err := canonicalPayloadHash(map[string]interface{}{"content": "hello", "type": 1})
	if err != nil {
		t.Fatal(err)
	}
	c, err := canonicalPayloadHash(map[string]interface{}{"type": 1, "content": "changed"})
	if err != nil {
		t.Fatal(err)
	}
	if a != b || a == c {
		t.Fatalf("a=%s b=%s c=%s", a, b, c)
	}
}
