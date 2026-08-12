package cli

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

type projectCapture struct {
	method string
	path   string
	body   []byte
}

func projectServer(t *testing.T, status int, respBody string) (*httptest.Server, *projectCapture) {
	t.Helper()
	capture := &projectCapture{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capture.method = r.Method
		capture.path = r.URL.Path
		capture.body, _ = io.ReadAll(r.Body)
		if !strings.HasPrefix(r.URL.Path, "/api/v1/projects") {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = io.WriteString(w, respBody)
	}))
	t.Cleanup(srv.Close)
	return srv, capture
}

func fullyPopulatedProjectConfig(policy string) projectConfig {
	return projectConfig{
		DefaultBranch:        "develop",
		SessionPrefix:        "demo",
		StartupRestorePolicy: policy,
		Env:                  map[string]string{"FOO": "bar"},
		Symlinks:             []string{".env"},
		PostCreate:           []string{"npm install"},
		AgentConfig: agentConfig{
			Model:           "base-model",
			Profile:         "base-profile",
			ReasoningEffort: "medium",
			Permissions:     "auto",
		},
		Worker: roleOverride{
			Agent: "claude-code",
			AgentConfig: agentConfig{
				Model:           "worker-model",
				Profile:         "worker-profile",
				ReasoningEffort: "high",
				Permissions:     "accept-edits",
			},
		},
		Orchestrator: roleOverride{
			Agent: "codex",
			AgentConfig: agentConfig{
				Model:           "gpt-5.6-luna",
				Profile:         "ao-minimal",
				ReasoningEffort: "low",
				Permissions:     "bypass-permissions",
			},
		},
		Reviewers: []reviewerConfig{{Harness: "codex"}, {Harness: "claude-code"}},
		TrackerIntake: trackerIntakeConfig{
			Enabled:  true,
			Provider: "github",
			Repo:     "acme/demo",
			Assignee: "alice",
		},
	}
}

func TestProjectSetConfig_TrackerIntakeFlags(t *testing.T) {
	cfg := setConfigEnv(t)
	srv, capture := projectServer(t, http.StatusOK, `{"project":{"id":"demo","path":"/repo/demo"}}`)
	writeRunFileFor(t, cfg, srv)

	_, errOut, err := executeCLI(t, Deps{
		ProcessAlive: func(int) bool { return true },
	}, "project", "set-config", "demo", "--tracker-intake", "--tracker-repo", "acme/demo", "--tracker-assignee", "alice")
	if err != nil {
		t.Fatalf("unexpected error: %v\nstderr=%s", err, errOut)
	}
	if capture.method != http.MethodPut || capture.path != "/api/v1/projects/demo/config" {
		t.Fatalf("request = %s %s, want PUT /api/v1/projects/demo/config", capture.method, capture.path)
	}
	var got setConfigRequest
	if err := json.Unmarshal(capture.body, &got); err != nil {
		t.Fatalf("decode request: %v\nbody=%s", err, capture.body)
	}
	if !got.Config.TrackerIntake.Enabled || got.Config.TrackerIntake.Provider != "github" || got.Config.TrackerIntake.Repo != "acme/demo" || got.Config.TrackerIntake.Assignee != "alice" {
		t.Fatalf("tracker intake request = %#v", got.Config.TrackerIntake)
	}
}

func TestProjectSetConfig_TrackerIntakeJSON(t *testing.T) {
	cfg := setConfigEnv(t)
	srv, capture := projectServer(t, http.StatusOK, `{"project":{"id":"demo","path":"/repo/demo"}}`)
	writeRunFileFor(t, cfg, srv)

	_, errOut, err := executeCLI(t, Deps{
		ProcessAlive: func(int) bool { return true },
	}, "project", "set-config", "demo", "--config-json", `{"trackerIntake":{"enabled":true,"provider":"github","assignee":"alice"}}`)
	if err != nil {
		t.Fatalf("unexpected error: %v\nstderr=%s", err, errOut)
	}
	var got setConfigRequest
	if err := json.Unmarshal(capture.body, &got); err != nil {
		t.Fatalf("decode request: %v\nbody=%s", err, capture.body)
	}
	if !got.Config.TrackerIntake.Enabled || got.Config.TrackerIntake.Provider != "github" || got.Config.TrackerIntake.Assignee != "alice" {
		t.Fatalf("tracker intake request = %#v", got.Config.TrackerIntake)
	}
}

func TestProjectConfigJSONRoundTripPreservesCompleteConfig(t *testing.T) {
	cfg := setConfigEnv(t)
	want := fullyPopulatedProjectConfig("preserve_only")
	configJSON, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}

	var captured setConfigRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/projects/demo":
			_, _ = io.WriteString(w, `{"status":"ok","project":{"id":"demo","path":"/repo/demo","config":`+string(configJSON)+`}}`)
		case r.Method == http.MethodPut && r.URL.Path == "/api/v1/projects/demo/config":
			if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
				t.Errorf("decode set-config request: %v", err)
			}
			_, _ = io.WriteString(w, `{"project":{"id":"demo","path":"/repo/demo"}}`)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	writeRunFileFor(t, cfg, srv)

	out, errOut, err := executeCLI(t, Deps{ProcessAlive: func(int) bool { return true }}, "project", "get", "demo", "--json")
	if err != nil {
		t.Fatalf("project get: %v\nstderr=%s", err, errOut)
	}
	var read projectGetResult
	if err := json.Unmarshal([]byte(out), &read); err != nil {
		t.Fatalf("decode project get output: %v\nout=%s", err, out)
	}
	if read.Project.Config == nil || !reflect.DeepEqual(*read.Project.Config, want) {
		t.Fatalf("read config = %#v, want %#v", read.Project.Config, want)
	}

	readJSON, err := json.Marshal(read.Project.Config)
	if err != nil {
		t.Fatal(err)
	}
	_, errOut, err = executeCLI(t, Deps{ProcessAlive: func(int) bool { return true }}, "project", "set-config", "demo", "--config-json", string(readJSON))
	if err != nil {
		t.Fatalf("project set-config: %v\nstderr=%s", err, errOut)
	}
	if !reflect.DeepEqual(captured.Config, want) {
		t.Fatalf("round-tripped config = %#v, want %#v", captured.Config, want)
	}
}

func TestBuildProjectConfigTrackerIntakeFlags(t *testing.T) {
	got, err := buildProjectConfig(projectSetConfigOptions{
		trackerIntake:   true,
		trackerRepo:     "acme/demo",
		trackerAssignee: "alice",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !got.TrackerIntake.Enabled || got.TrackerIntake.Provider != "github" || got.TrackerIntake.Repo != "acme/demo" || got.TrackerIntake.Assignee != "alice" {
		t.Fatalf("tracker intake config = %#v", got.TrackerIntake)
	}
}

func TestProjectSetConfig_StartupRestorePolicyFlag(t *testing.T) {
	cfg := setConfigEnv(t)
	current := fullyPopulatedProjectConfig("automatic")
	want := current
	want.StartupRestorePolicy = "preserve_only"
	currentJSON, err := json.Marshal(current)
	if err != nil {
		t.Fatal(err)
	}
	var currentFields map[string]json.RawMessage
	if err := json.Unmarshal(currentJSON, &currentFields); err != nil {
		t.Fatal(err)
	}
	currentFields["futureConfig"] = json.RawMessage(`{"nested":[1,true,"keep"]}`)
	currentJSON, err = json.Marshal(currentFields)
	if err != nil {
		t.Fatal(err)
	}

	var requests []string
	var captured setConfigRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/internal/telemetry/cli-invoked" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		requests = append(requests, r.Method+" "+r.URL.Path)
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/projects/demo":
			_, _ = io.WriteString(w, `{"status":"ok","project":{"id":"demo","path":"/repo/demo","config":`+string(currentJSON)+`}}`)
		case r.Method == http.MethodPut && r.URL.Path == "/api/v1/projects/demo/config":
			if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
				t.Errorf("decode set-config request: %v", err)
			}
			_, _ = io.WriteString(w, `{"project":{"id":"demo","path":"/repo/demo"}}`)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	writeRunFileFor(t, cfg, srv)

	_, errOut, err := executeCLI(t, Deps{
		ProcessAlive: func(int) bool { return true },
	}, "project", "set-config", "demo", "--startup-restore-policy", "preserve_only", "--json")
	if err != nil {
		t.Fatalf("unexpected error: %v\nstderr=%s", err, errOut)
	}
	if !reflect.DeepEqual(requests, []string{
		"GET /api/v1/projects/demo",
		"PUT /api/v1/projects/demo/config",
	}) {
		t.Fatalf("requests = %#v, want GET then PUT", requests)
	}
	if got := string(captured.Config.Extra["futureConfig"]); got != `{"nested":[1,true,"keep"]}` {
		t.Fatalf("unknown forward config = %s, want preserved verbatim", got)
	}
	captured.Config.Extra = nil
	if !reflect.DeepEqual(captured.Config, want) {
		t.Fatalf("policy patch config = %#v, want unrelated fields preserved %#v", captured.Config, want)
	}
}

func TestProjectSetConfig_StartupRestorePolicyRejectsOtherMutationFlags(t *testing.T) {
	setConfigEnv(t)
	cases := []struct {
		name string
		args []string
	}{
		{name: "default branch", args: []string{"--default-branch", "main"}},
		{name: "session prefix", args: []string{"--session-prefix", "demo"}},
		{name: "model", args: []string{"--model", "model"}},
		{name: "permission", args: []string{"--permission", "auto"}},
		{name: "worker agent", args: []string{"--worker-agent", "codex"}},
		{name: "orchestrator agent", args: []string{"--orchestrator-agent", "codex"}},
		{name: "env", args: []string{"--env", "KEY=value"}},
		{name: "symlink", args: []string{"--symlink", ".env"}},
		{name: "post create", args: []string{"--post-create", "make setup"}},
		{name: "tracker intake", args: []string{"--tracker-intake"}},
		{name: "tracker repo", args: []string{"--tracker-repo", "acme/demo"}},
		{name: "tracker assignee", args: []string{"--tracker-assignee", "alice"}},
		{name: "config json", args: []string{"--config-json", `{}`}},
		{name: "clear", args: []string{"--clear"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			args := []string{"project", "set-config", "demo", "--startup-restore-policy", "automatic"}
			args = append(args, tc.args...)
			_, errOut, err := executeCLI(t, Deps{}, args...)
			if err == nil || ExitCode(err) != 2 {
				t.Fatalf("error = %v exit=%d, want usage error\nstderr=%s", err, ExitCode(err), errOut)
			}
			if !strings.Contains(err.Error(), "cannot be combined") && !strings.Contains(errOut, "cannot be combined") {
				t.Fatalf("error missing combination guidance: %v\nstderr=%s", err, errOut)
			}
		})
	}
}

func TestProjectSetConfig_StartupRestorePolicyRejectsInvalidValueBeforeRead(t *testing.T) {
	setConfigEnv(t)
	_, errOut, err := executeCLI(t, Deps{}, "project", "set-config", "demo", "--startup-restore-policy", "managed")
	if err == nil || ExitCode(err) != 2 {
		t.Fatalf("error = %v exit=%d, want usage error\nstderr=%s", err, ExitCode(err), errOut)
	}
	if !strings.Contains(err.Error(), "want automatic or preserve_only") && !strings.Contains(errOut, "want automatic or preserve_only") {
		t.Fatalf("error missing accepted values: %v\nstderr=%s", err, errOut)
	}
}

func TestProjectSetConfig_StartupRestorePolicyRefusesDegradedConfig(t *testing.T) {
	cfg := setConfigEnv(t)
	var requests []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/internal/telemetry/cli-invoked" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		requests = append(requests, r.Method+" "+r.URL.Path)
		if r.Method != http.MethodGet || r.URL.Path != "/api/v1/projects/demo" {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			http.Error(w, "unexpected write", http.StatusInternalServerError)
			return
		}
		_, _ = io.WriteString(w, `{"status":"degraded","project":{"id":"demo","path":"/repo/demo","resolveError":"decode project config: invalid JSON"}}`)
	}))
	t.Cleanup(srv.Close)
	writeRunFileFor(t, cfg, srv)

	_, errOut, err := executeCLI(t, Deps{ProcessAlive: func(int) bool { return true }}, "project", "set-config", "demo", "--startup-restore-policy", "automatic")
	if err == nil || ExitCode(err) != 1 {
		t.Fatalf("error = %v exit=%d, want runtime refusal\nstderr=%s", err, ExitCode(err), errOut)
	}
	if !strings.Contains(err.Error(), "decode project config") && !strings.Contains(errOut, "decode project config") {
		t.Fatalf("error missing degraded reason: %v\nstderr=%s", err, errOut)
	}
	if !reflect.DeepEqual(requests, []string{"GET /api/v1/projects/demo"}) {
		t.Fatalf("requests = %#v, want read only", requests)
	}
}

func TestProjectSetConfigHelpIncludesStartupRestorePolicy(t *testing.T) {
	out, errOut, err := executeCLI(t, Deps{}, "project", "set-config", "--help")
	if err != nil {
		t.Fatalf("set-config help: %v\nstderr=%s", err, errOut)
	}
	if !strings.Contains(out, "--startup-restore-policy") ||
		!strings.Contains(out, "automatic or preserve_only") ||
		!strings.Contains(out, "narrow patch") ||
		!strings.Contains(out, "preserve every unrelated config field") {
		t.Fatalf("set-config help missing startup restore policy:\n%s", out)
	}
}

func TestProjectList_Success(t *testing.T) {
	cfg := setConfigEnv(t)
	srv, capture := projectServer(t, http.StatusOK, `{"projects":[{"id":"zeta","name":"Zeta","sessionPrefix":"zeta"},{"id":"alpha","name":"Alpha","sessionPrefix":"alpha","resolveError":"config missing"}]}`)
	writeRunFileFor(t, cfg, srv)

	out, errOut, err := executeCLI(t, Deps{
		ProcessAlive: func(int) bool { return true },
	}, "project", "ls")
	if err != nil {
		t.Fatalf("unexpected error: %v\nstderr=%s", err, errOut)
	}
	if capture.method != http.MethodGet || capture.path != "/api/v1/projects" {
		t.Fatalf("request = %s %s, want GET /api/v1/projects", capture.method, capture.path)
	}
	if !strings.Contains(out, "ID") || !strings.Contains(out, "SESSION PREFIX") {
		t.Fatalf("output missing table header:\n%s", out)
	}
	if strings.Index(out, "alpha") > strings.Index(out, "zeta") {
		t.Fatalf("projects should be sorted by id in output:\n%s", out)
	}
	if !strings.Contains(out, "degraded: config missing") {
		t.Fatalf("output missing degraded status:\n%s", out)
	}
}

func TestProjectList_JSON(t *testing.T) {
	cfg := setConfigEnv(t)
	srv, _ := projectServer(t, http.StatusOK, `{"projects":[{"id":"demo","name":"Demo","sessionPrefix":"demo"}]}`)
	writeRunFileFor(t, cfg, srv)

	out, errOut, err := executeCLI(t, Deps{
		ProcessAlive: func(int) bool { return true },
	}, "project", "ls", "--json")
	if err != nil {
		t.Fatalf("unexpected error: %v\nstderr=%s", err, errOut)
	}
	var got projectListResult
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode json output: %v\nout=%s", err, out)
	}
	if len(got.Projects) != 1 || got.Projects[0].ID != "demo" {
		t.Fatalf("projects = %#v, want demo", got.Projects)
	}
}

func TestProjectList_Empty(t *testing.T) {
	cfg := setConfigEnv(t)
	srv, _ := projectServer(t, http.StatusOK, `{"projects":[]}`)
	writeRunFileFor(t, cfg, srv)

	out, errOut, err := executeCLI(t, Deps{
		ProcessAlive: func(int) bool { return true },
	}, "project", "ls")
	if err != nil {
		t.Fatalf("unexpected error: %v\nstderr=%s", err, errOut)
	}
	if !strings.Contains(out, "No projects registered") || !strings.Contains(out, "ao project add --path") {
		t.Fatalf("empty output missing hint:\n%s", out)
	}
}

func TestProjectGet_Success(t *testing.T) {
	cfg := setConfigEnv(t)
	srv, capture := projectServer(t, http.StatusOK, `{"status":"ok","project":{"id":"demo","name":"Demo","path":"/repo/demo","repo":"git@example.com:demo.git","defaultBranch":"main","agent":"codex"}}`)
	writeRunFileFor(t, cfg, srv)

	out, errOut, err := executeCLI(t, Deps{
		ProcessAlive: func(int) bool { return true },
	}, "project", "get", "demo")
	if err != nil {
		t.Fatalf("unexpected error: %v\nstderr=%s", err, errOut)
	}
	if capture.method != http.MethodGet || capture.path != "/api/v1/projects/demo" {
		t.Fatalf("request = %s %s, want GET /api/v1/projects/demo", capture.method, capture.path)
	}
	for _, want := range []string{"Project demo (ok)", "name: Demo", "path: /repo/demo", "default branch: main", "agent: codex"} {
		if !strings.Contains(out, want) {
			t.Fatalf("output missing %q:\n%s", want, out)
		}
	}
}

func TestProjectGet_JSON(t *testing.T) {
	cfg := setConfigEnv(t)
	srv, capture := projectServer(t, http.StatusOK, `{"status":"degraded","project":{"id":"demo","name":"Demo","path":"/repo/demo","resolveError":"config missing"}}`)
	writeRunFileFor(t, cfg, srv)

	out, errOut, err := executeCLI(t, Deps{
		ProcessAlive: func(int) bool { return true },
	}, "project", "get", "demo", "--json")
	if err != nil {
		t.Fatalf("unexpected error: %v\nstderr=%s", err, errOut)
	}
	if capture.method != http.MethodGet || capture.path != "/api/v1/projects/demo" {
		t.Fatalf("request = %s %s, want GET /api/v1/projects/demo", capture.method, capture.path)
	}
	var got projectGetResult
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode json output: %v\nout=%s", err, out)
	}
	if got.Status != "degraded" || got.Project.ID != "demo" || got.Project.ResolveError != "config missing" {
		t.Fatalf("get json = %#v, want degraded demo with resolve error", got)
	}
}

func TestProjectGet_MissingArg(t *testing.T) {
	setConfigEnv(t)
	_, _, err := executeCLI(t, Deps{}, "project", "get")
	if err == nil {
		t.Fatal("expected missing arg error")
	}
	if got := ExitCode(err); got != 2 {
		t.Fatalf("exit code = %d, want 2", got)
	}
}

func TestProjectGet_NotFound(t *testing.T) {
	cfg := setConfigEnv(t)
	srv, _ := projectServer(t, http.StatusNotFound, `{"error":"not_found","code":"PROJECT_NOT_FOUND","message":"Unknown project"}`)
	writeRunFileFor(t, cfg, srv)

	_, errOut, err := executeCLI(t, Deps{
		ProcessAlive: func(int) bool { return true },
	}, "project", "get", "missing")
	if err == nil {
		t.Fatal("expected not found error")
	}
	if got := ExitCode(err); got != 1 {
		t.Fatalf("exit code = %d, want 1", got)
	}
	if !strings.Contains(err.Error(), "PROJECT_NOT_FOUND") && !strings.Contains(errOut, "PROJECT_NOT_FOUND") {
		t.Fatalf("error did not surface not found envelope: %v\nstderr=%s", err, errOut)
	}
}

func TestProjectRemove_RequiresID(t *testing.T) {
	setConfigEnv(t)
	_, _, err := executeCLI(t, Deps{}, "project", "rm")
	if err == nil {
		t.Fatal("expected missing id error")
	}
	if got := ExitCode(err); got != 2 {
		t.Fatalf("exit code = %d, want 2", got)
	}
}

func TestProjectRemove_NotFound(t *testing.T) {
	cfg := setConfigEnv(t)
	srv, _ := projectServer(t, http.StatusNotFound, `{"error":"not_found","code":"PROJECT_NOT_FOUND","message":"Unknown project"}`)
	writeRunFileFor(t, cfg, srv)

	_, errOut, err := executeCLI(t, Deps{
		ProcessAlive: func(int) bool { return true },
	}, "project", "rm", "missing", "--yes")
	if err == nil {
		t.Fatal("expected not found error")
	}
	if got := ExitCode(err); got != 1 {
		t.Fatalf("exit code = %d, want 1", got)
	}
	if !strings.Contains(err.Error(), "PROJECT_NOT_FOUND") && !strings.Contains(errOut, "PROJECT_NOT_FOUND") {
		t.Fatalf("error did not surface not found envelope: %v\nstderr=%s", err, errOut)
	}
}

func TestProjectRemove_AbortsWhenConfirmationDoesNotMatch(t *testing.T) {
	setConfigEnv(t)
	out, _, err := executeCLI(t, Deps{
		In: strings.NewReader("nope\n"),
	}, "project", "rm", "demo")
	if err != nil {
		t.Fatalf("unexpected abort error: %v", err)
	}
	if !strings.Contains(out, "Type the project id to confirm") || !strings.Contains(out, "aborted") {
		t.Fatalf("output missing prompt/abort:\n%s", out)
	}
}

func TestProjectRemove_DeletesAfterConfirmation(t *testing.T) {
	cfg := setConfigEnv(t)
	srv, capture := projectServer(t, http.StatusOK, `{"ok":true,"id":"demo"}`)
	writeRunFileFor(t, cfg, srv)

	out, errOut, err := executeCLI(t, Deps{
		In:           strings.NewReader("demo\n"),
		ProcessAlive: func(int) bool { return true },
	}, "project", "rm", "demo")
	if err != nil {
		t.Fatalf("unexpected error: %v\nstderr=%s", err, errOut)
	}
	if capture.method != http.MethodDelete || capture.path != "/api/v1/projects/demo" {
		t.Fatalf("request = %s %s, want DELETE /api/v1/projects/demo", capture.method, capture.path)
	}
	if !strings.Contains(out, "removed project demo") {
		t.Fatalf("output missing removal message:\n%s", out)
	}
}

func TestProjectRemove_JSONDocumentedEnvelope(t *testing.T) {
	cfg := setConfigEnv(t)
	srv, capture := projectServer(t, http.StatusOK, `{"ok":true,"id":"demo"}`)
	writeRunFileFor(t, cfg, srv)

	out, errOut, err := executeCLI(t, Deps{
		In:           strings.NewReader("wrong\n"),
		ProcessAlive: func(int) bool { return true },
	}, "project", "rm", "demo", "--yes", "--json")
	if err != nil {
		t.Fatalf("unexpected error: %v\nstderr=%s", err, errOut)
	}
	if capture.method != http.MethodDelete || capture.path != "/api/v1/projects/demo" {
		t.Fatalf("request = %s %s, want DELETE /api/v1/projects/demo", capture.method, capture.path)
	}
	var got projectRemoveResult
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode json output: %v\nout=%s", err, out)
	}
	if !got.OK || got.ID != "demo" || got.ProjectID != "" {
		t.Fatalf("remove json = %#v, want documented ok/id envelope", got)
	}
}

func TestProjectRemove_JSONBackendEnvelope(t *testing.T) {
	cfg := setConfigEnv(t)
	removedStorageDir := false
	srv, _ := projectServer(t, http.StatusOK, `{"projectId":"demo","removedStorageDir":false}`)
	writeRunFileFor(t, cfg, srv)

	out, errOut, err := executeCLI(t, Deps{
		ProcessAlive: func(int) bool { return true },
	}, "project", "rm", "demo", "--yes", "--json")
	if err != nil {
		t.Fatalf("unexpected error: %v\nstderr=%s", err, errOut)
	}
	var got projectRemoveResult
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode json output: %v\nout=%s", err, out)
	}
	if got.ProjectID != "demo" || got.RemovedStorageDir == nil || *got.RemovedStorageDir != removedStorageDir {
		t.Fatalf("remove json = %#v, want backend projectId/removedStorageDir envelope", got)
	}
}

func TestProjectRemove_EmptySuccessFallsBackToRequestedID(t *testing.T) {
	cfg := setConfigEnv(t)
	srv, _ := projectServer(t, http.StatusNoContent, ``)
	writeRunFileFor(t, cfg, srv)

	out, errOut, err := executeCLI(t, Deps{
		ProcessAlive: func(int) bool { return true },
	}, "project", "rm", "demo", "--yes")
	if err != nil {
		t.Fatalf("unexpected error for empty 2xx body: %v\nstderr=%s", err, errOut)
	}
	if !strings.Contains(out, "removed project demo") {
		t.Fatalf("output missing fallback removal id:\n%s", out)
	}
}

func TestProjectRemove_YesSkipsConfirmationAndSupportsBackendRemoveEnvelope(t *testing.T) {
	cfg := setConfigEnv(t)
	srv, capture := projectServer(t, http.StatusOK, `{"projectId":"demo","removedStorageDir":false}`)
	writeRunFileFor(t, cfg, srv)

	out, errOut, err := executeCLI(t, Deps{
		In:           strings.NewReader("wrong\n"),
		ProcessAlive: func(int) bool { return true },
	}, "project", "rm", "demo", "--yes")
	if err != nil {
		t.Fatalf("unexpected error: %v\nstderr=%s", err, errOut)
	}
	if capture.method != http.MethodDelete || capture.path != "/api/v1/projects/demo" {
		t.Fatalf("request = %s %s, want DELETE /api/v1/projects/demo", capture.method, capture.path)
	}
	if strings.Contains(out, "Type the project id") || !strings.Contains(out, "removed project demo") {
		t.Fatalf("--yes output should skip prompt and print removal:\n%s", out)
	}
}
