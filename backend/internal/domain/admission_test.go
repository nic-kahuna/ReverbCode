package domain

import (
	"encoding/json"
	"testing"
)

func TestAdmissionConfigRejectsMalformedBoolean(t *testing.T) {
	for _, raw := range []string{`{"admissionPaused":null}`, `{"admissionPaused":"true"}`, `{"admissionPaused":1}`, `{"admissionPaused":{}}`, `null`} {
		var cfg ProjectConfig
		if err := json.Unmarshal([]byte(raw), &cfg); err == nil {
			t.Fatalf("accepted %s", raw)
		}
	}
	for _, raw := range []string{`{"admissionPaused":false}`, `{"admissionPaused":true}`} {
		var cfg ProjectConfig
		if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
			t.Fatal(err)
		}
		if !cfg.AdmissionPausedSet {
			t.Fatal("lost presence")
		}
	}
}

func TestDesktopProjectsAdmissionConfigPresence(t *testing.T) {
	for _, raw := range []string{`{"desktopProjectsAdmission":null}`, `{"desktopProjectsAdmission":"true"}`, `{"desktopProjectsAdmission":1}`, `{"desktopProjectsAdmission":{}}`} {
		var cfg ProjectConfig
		if err := json.Unmarshal([]byte(raw), &cfg); err == nil {
			t.Fatalf("accepted %s", raw)
		}
	}
	for _, raw := range []string{`{"desktopProjectsAdmission":false}`, `{"desktopProjectsAdmission":true}`} {
		var cfg ProjectConfig
		if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
			t.Fatal(err)
		}
		if !cfg.DesktopProjectsAdmissionSet {
			t.Fatal("lost presence")
		}
	}
}
