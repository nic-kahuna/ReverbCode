package cli

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestSessionRecoveryGetAndListPreserveMachineReadableEvidence(t *testing.T) {
	cfg := setConfigEnv(t)
	rawSession := `{
		"id":"demo-1",
		"projectId":"demo",
		"kind":"worker",
		"harness":"codex",
		"activity":{"state":"exited","lastActivityAt":"2026-08-12T00:00:00Z"},
		"isTerminated":true,
		"createdAt":"2026-08-11T00:00:00Z",
		"updatedAt":"2026-08-12T00:00:00Z",
		"status":"terminated",
		"branch":"ao/demo-1/root",
		"workspacePath":"/worktrees/demo-1",
		"requestedRoute":{"harness":"codex","model":"gpt-5.6-sol","reasoningEffort":"ultra"},
		"launchRoute":{"harness":"codex","model":"gpt-5.6-sol","reasoningEffort":"ultra"},
		"recovery":{
			"state":"awaiting_recovery",
			"policy":"preserve_only",
			"runtimeState":"absent",
			"providerSessionSaved":true,
			"worktrees":[
				{
					"repoName":"__root__",
					"branch":"ao/demo-1/root",
					"baseSha":"base-123",
					"worktreePath":"/worktrees/demo-1",
					"preservedRef":"refs/ao/preserved/demo-1",
					"state":"preserved"
				},
				{
					"repoName":"api",
					"branch":"ao/demo-1/root",
					"baseSha":"base-api",
					"worktreePath":"/worktrees/demo-1/api",
					"preservedRef":"",
					"state":"preserved_removed"
				}
			]
		}
	}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/v1/sessions/demo-1":
			_, _ = io.WriteString(w, `{"session":`+rawSession+`}`)
		case "/api/v1/sessions":
			_, _ = io.WriteString(w, `{"sessions":[`+rawSession+`]}`)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	writeRunFileFor(t, cfg, srv)
	deps := Deps{ProcessAlive: func(int) bool { return true }}

	textOut, errOut, err := executeCLI(t, deps, "session", "get", "demo-1")
	if err != nil {
		t.Fatalf("session get: %v\nstderr=%s", err, errOut)
	}
	for _, want := range []string{
		"status: terminated",
		"activity: exited",
		"recovery: awaiting_recovery (policy preserve_only)",
		"recovery runtime: absent",
		"provider session saved: true",
		`recovery worktree __root__: branch="ao/demo-1/root" baseSha="base-123" path="/worktrees/demo-1" preservedRef="refs/ao/preserved/demo-1" state="preserved"`,
	} {
		if !strings.Contains(textOut, want) {
			t.Fatalf("session get output missing %q:\n%s", want, textOut)
		}
	}

	jsonOut, errOut, err := executeCLI(t, deps, "session", "get", "demo-1", "--json")
	if err != nil {
		t.Fatalf("session get --json: %v\nstderr=%s", err, errOut)
	}
	var got sessionResponse
	if err := json.Unmarshal([]byte(jsonOut), &got); err != nil {
		t.Fatalf("decode get JSON: %v\nout=%s", err, jsonOut)
	}
	assertCLIRecovery(t, got.Session.Recovery)
	if got.Session.Status != "terminated" || got.Session.RequestedRoute == nil || got.Session.LaunchRoute == nil {
		t.Fatalf("session identity = %#v", got.Session)
	}

	listText, errOut, err := executeCLI(t, deps, "session", "ls", "--include-terminated")
	if err != nil {
		t.Fatalf("session ls: %v\nstderr=%s", err, errOut)
	}
	if !strings.Contains(listText, "[terminated]  [recovery:awaiting_recovery]") {
		t.Fatalf("session ls did not expose recovery substate:\n%s", listText)
	}

	listOut, errOut, err := executeCLI(t, deps, "session", "ls", "--include-terminated", "--json")
	if err != nil {
		t.Fatalf("session ls --json: %v\nstderr=%s", err, errOut)
	}
	var list sessionListOutput
	if err := json.Unmarshal([]byte(listOut), &list); err != nil {
		t.Fatalf("decode list JSON: %v\nout=%s", err, listOut)
	}
	if len(list.Data) != 1 || list.Data[0].Status != "terminated" || !list.Data[0].IsTerminated {
		t.Fatalf("list data = %#v", list.Data)
	}
	if list.Data[0].RequestedRoute == nil || list.Data[0].RequestedRoute.Model != "gpt-5.6-sol" ||
		list.Data[0].LaunchRoute == nil || list.Data[0].LaunchRoute.ReasoningEffort != "ultra" {
		t.Fatalf("list route identity = requested %#v launch %#v", list.Data[0].RequestedRoute, list.Data[0].LaunchRoute)
	}
	assertCLIRecovery(t, list.Data[0].Recovery)
}

func assertCLIRecovery(t *testing.T, recovery *sessionRecoveryDTO) {
	t.Helper()
	if recovery == nil {
		t.Fatal("recovery = nil")
	}
	if recovery.State != "awaiting_recovery" || recovery.Policy != "preserve_only" || recovery.RuntimeState != "absent" || !recovery.ProviderSessionSaved {
		t.Fatalf("recovery = %#v", recovery)
	}
	if len(recovery.Worktrees) != 2 {
		t.Fatalf("worktrees = %#v", recovery.Worktrees)
	}
	wt := recovery.Worktrees[0]
	if wt.RepoName != "__root__" || wt.Branch != "ao/demo-1/root" || wt.BaseSHA != "base-123" || wt.WorktreePath != "/worktrees/demo-1" || wt.PreservedRef != "refs/ao/preserved/demo-1" || wt.State != "preserved" {
		t.Fatalf("worktree = %#v", wt)
	}
	absent := recovery.Worktrees[1]
	if absent.RepoName != "api" || absent.Branch != "ao/demo-1/root" || absent.BaseSHA != "base-api" || absent.WorktreePath != "/worktrees/demo-1/api" || absent.PreservedRef != "" || absent.State != "preserved_removed" {
		t.Fatalf("absent worktree = %#v", absent)
	}
}
