package cli

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

func TestProjectAdmissionReadAndWrite(t *testing.T) {
	for _, tc := range []struct {
		name, flag, method, body string
		paused                   bool
	}{
		{"read", "", http.MethodGet, "", false},
		{"pause", "--paused=true", http.MethodPut, `{"paused":true}`, true},
		{"allow", "--paused=false", http.MethodPut, `{"paused":false}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := setConfigEnv(t)
			response := `{"projectId":"demo","admissionPaused":false,"scope":"new_launches_only","existingSessionsMayBeRunning":true}`
			if tc.paused {
				response = strings.Replace(response, `"admissionPaused":false`, `"admissionPaused":true`, 1)
			}
			srv, capture := projectServer(t, http.StatusOK, response)
			writeRunFileFor(t, cfg, srv)
			args := []string{"project", "admission", "demo", "--json"}
			if tc.flag != "" {
				args = append(args, tc.flag)
			}
			out, stderr, err := executeCLI(t, Deps{ProcessAlive: func(int) bool { return true }}, args...)
			if err != nil {
				t.Fatalf("admission: %v; stderr=%s", err, stderr)
			}
			if capture.method != tc.method || capture.path != "/api/v1/projects/demo/admission" || strings.TrimSpace(string(capture.body)) != tc.body {
				t.Fatalf("request = %s %s %s", capture.method, capture.path, capture.body)
			}
			var got projectAdmissionState
			if err := json.Unmarshal([]byte(out), &got); err != nil || got.AdmissionPaused == nil || *got.AdmissionPaused != tc.paused || got.ProjectID != "demo" || got.Scope != "new_launches_only" || got.ExistingSessionsMayBeRunning == nil || !*got.ExistingSessionsMayBeRunning {
				t.Fatalf("unexpected admission output: %s; err=%v", out, err)
			}
		})
	}
}

func TestProjectAdmissionUsageErrors(t *testing.T) {
	for _, args := range [][]string{
		{"project", "admission"},
		{"project", "admission", " "},
		{"project", "admission", "demo", "extra"},
		{"project", "admission", "demo", "--paused=maybe"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			setConfigEnv(t)
			_, _, err := executeCLI(t, Deps{}, args...)
			if ExitCode(err) != 2 {
				t.Fatalf("error = %v, exit=%d; want usage error", err, ExitCode(err))
			}
		})
	}
}

func TestProjectAdmissionRejectsIncompleteEvidence(t *testing.T) {
	for _, response := range []string{
		`{}`,
		`{"projectId":"demo","scope":"new_launches_only","existingSessionsMayBeRunning":true}`,
		`{"projectId":"other","admissionPaused":true,"scope":"new_launches_only","existingSessionsMayBeRunning":true}`,
		`{"projectId":"demo","admissionPaused":true,"scope":"stopped","existingSessionsMayBeRunning":true}`,
		`{"projectId":"demo","admissionPaused":true,"scope":"new_launches_only","existingSessionsMayBeRunning":false}`,
		`{"projectId":"demo","admissionPaused":false,"scope":"new_launches_only","existingSessionsMayBeRunning":true}`,
	} {
		t.Run(response, func(t *testing.T) {
			cfg := setConfigEnv(t)
			srv, _ := projectServer(t, http.StatusOK, response)
			writeRunFileFor(t, cfg, srv)
			out, _, err := executeCLI(t, Deps{ProcessAlive: func(int) bool { return true }}, "project", "admission", "demo", "--paused=true", "--json")
			if ExitCode(err) != 1 || out != "" {
				t.Fatalf("out=%q error=%v exit=%d", out, err, ExitCode(err))
			}
		})
	}
}

func TestProjectAdmissionPreservesDaemonError(t *testing.T) {
	cfg := setConfigEnv(t)
	srv, _ := projectServer(t, http.StatusConflict, `{"error":"conflict","code":"PROJECT_ADMISSION_UNKNOWN","message":"Project config cannot be read","requestId":"req-admission"}`)
	writeRunFileFor(t, cfg, srv)
	_, stderr, err := executeCLI(t, Deps{ProcessAlive: func(int) bool { return true }}, "project", "admission", "demo", "--paused=true")
	if ExitCode(err) != 1 || !strings.Contains(err.Error()+stderr, "PROJECT_ADMISSION_UNKNOWN") || !strings.Contains(err.Error()+stderr, "req-admission") {
		t.Fatalf("error=%v stderr=%s exit=%d", err, stderr, ExitCode(err))
	}
}

func TestProjectConfigAdmissionBooleanPresence(t *testing.T) {
	for _, tc := range []struct{ raw, want string }{
		{`{}`, ""}, {`{"admissionPaused":true}`, "true"}, {`{"admissionPaused":false}`, "false"},
	} {
		cfg, err := buildProjectConfig(projectSetConfigOptions{configJSON: tc.raw})
		if err != nil {
			t.Fatal(err)
		}
		encoded, err := json.Marshal(cfg)
		if err != nil {
			t.Fatal(err)
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(encoded, &fields); err != nil {
			t.Fatal(err)
		}
		if string(fields["admissionPaused"]) != tc.want {
			t.Fatalf("config %s became %s", tc.raw, encoded)
		}
	}
	for _, raw := range []string{`null`, `[]`, `{"admissionPaused":null}`, `{"admissionPaused":"false"}`} {
		_, err := buildProjectConfig(projectSetConfigOptions{configJSON: raw})
		if ExitCode(err) != 2 {
			t.Fatalf("config %s: err=%v; want usage error", raw, err)
		}
	}
}
