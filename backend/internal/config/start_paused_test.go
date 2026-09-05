package config

import "testing"

func TestStartPausedIsExplicitAndStrict(t *testing.T) {
	for _, v := range []string{"true", "false", "1", "", "TRUE"} {
		t.Run(v, func(t *testing.T) {
			t.Setenv("AO_START_PAUSED", v)
			got, err := Load()
			if v == "true" || v == "false" {
				if err != nil || got.StartPaused != (v == "true") {
					t.Fatalf("config %+v,%v", got, err)
				}
			} else if err == nil {
				t.Fatal("accepted malformed maintenance flag")
			}
		})
	}
}
