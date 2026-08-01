package dating

import "testing"

func TestEffectiveGenderPreference(t *testing.T) {
	tests := []struct {
		name    string
		profile *DatingProfileResp
		want    int
	}{
		{name: "male defaults to women", profile: &DatingProfileResp{Sex: 1, GenderPreference: -1}, want: 0},
		{name: "female defaults to men", profile: &DatingProfileResp{Sex: 0, GenderPreference: -1}, want: 1},
		{name: "explicit men remains men", profile: &DatingProfileResp{Sex: 1, GenderPreference: 1}, want: 1},
		{name: "explicit women remains women", profile: &DatingProfileResp{Sex: 0, GenderPreference: 0}, want: 0},
		{name: "unknown sex remains unrestricted", profile: &DatingProfileResp{Sex: -1, GenderPreference: -1}, want: -1},
		{name: "nil profile", profile: nil, want: -1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := effectiveGenderPreference(tt.profile); got != tt.want {
				t.Fatalf("effectiveGenderPreference()=%d, want %d", got, tt.want)
			}
		})
	}
}

func TestFitsMutualFiltersDefaultOppositeSex(t *testing.T) {
	male := &DatingProfileResp{Sex: 1, GenderPreference: -1, Age: 28, MinAge: 18, MaxAge: 99, Photos: []string{"m"}}
	female := &DatingProfileResp{Sex: 0, GenderPreference: -1, Age: 26, MinAge: 18, MaxAge: 99, Photos: []string{"f"}}
	otherMale := &DatingProfileResp{Sex: 1, GenderPreference: -1, Age: 27, MinAge: 18, MaxAge: 99, Photos: []string{"m2"}}

	if !fitsMutualFilters(male, female) {
		t.Fatal("male and female with unset preferences should match")
	}
	if fitsMutualFilters(male, otherMale) {
		t.Fatal("two men with unset preferences must not be recommended to each other")
	}

	male.GenderPreference = 1
	otherMale.GenderPreference = 1
	if !fitsMutualFilters(male, otherMale) {
		t.Fatal("explicit mutual same-sex preference should remain supported")
	}
}
