package controllers_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/httpd/apierr"
)

func TestSessionsAPI_GetAndListExposeManagedRecoveryWithoutProviderID(t *testing.T) {
	svc := newFakeSessionService()
	sess := svc.sessions["ao-1"]
	sess.IsTerminated = true
	sess.Status = domain.StatusTerminated
	sess.Activity.State = domain.ActivityExited
	sess.TerminalHandleID = ""
	sess.Metadata.AgentSessionID = "provider-secret-id"
	sess.Metadata.RequestedRoute = &domain.AgentRoute{
		Harness:         domain.HarnessCodex,
		Model:           "gpt-5.6-sol",
		ReasoningEffort: domain.ReasoningEffortUltra,
	}
	sess.Metadata.LaunchRoute = &domain.AgentLaunchRoute{
		Harness:         domain.HarnessCodex,
		Model:           "gpt-5.6-sol",
		ReasoningEffort: domain.ReasoningEffortUltra,
	}
	sess.Recovery = &domain.SessionRecovery{
		State:                domain.SessionRecoveryAwaitingRecovery,
		Policy:               domain.StartupRestorePreserveOnly,
		RuntimeState:         domain.SessionRecoveryRuntimeAbsent,
		ProviderSessionSaved: true,
		Worktrees: []domain.RecoveryWorktree{
			{
				RepoName:     domain.RootWorkspaceRepoName,
				Branch:       "ao/ao-1/root",
				BaseSHA:      "base-123",
				WorktreePath: "/worktrees/ao-1",
				PreservedRef: "refs/ao/preserved/ao-1",
				State:        domain.SessionWorktreeStatePreserved,
			},
			{
				RepoName:     "api",
				Branch:       "ao/ao-1/root",
				BaseSHA:      "base-api",
				WorktreePath: "/worktrees/ao-1/api",
				PreservedRef: "",
				State:        domain.SessionWorktreeStatePreservedRemoved,
			},
		},
	}
	svc.sessions["ao-1"] = sess
	srv := newSessionTestServer(t, svc)

	for _, path := range []string{"/api/v1/sessions/ao-1", "/api/v1/sessions?project=ao"} {
		body, status, _ := doRequest(t, srv, http.MethodGet, path, "")
		if status != http.StatusOK {
			t.Fatalf("GET %s = %d, want 200; body=%s", path, status, body)
		}
		if strings.Contains(string(body), "provider-secret-id") || strings.Contains(string(body), "agentSessionId") {
			t.Fatalf("GET %s leaked provider session identity: %s", path, body)
		}

		var envelope map[string]any
		mustJSON(t, body, &envelope)
		var rawSession map[string]any
		if one, ok := envelope["session"].(map[string]any); ok {
			rawSession = one
		} else {
			rows, ok := envelope["sessions"].([]any)
			if !ok || len(rows) != 1 {
				t.Fatalf("GET %s sessions = %#v", path, envelope["sessions"])
			}
			rawSession, _ = rows[0].(map[string]any)
		}
		assertRecoverySessionWire(t, rawSession)
	}
}

func TestSessionsAPI_ManagedRestoreReturnsStructuredFailClosedEvidence(t *testing.T) {
	svc := newFakeSessionService()
	svc.restoreErr = apierr.Conflict("SESSION_RESTORE_FAILED", "Managed session restore did not complete; recovery evidence was preserved", map[string]any{
		"reason": "launch_route_missing", "sessionId": "ao-1", "projectId": "ao", "role": "worker",
		"providerSessionSaved": true,
		"worktrees":            []map[string]any{{"repoName": "__root__", "branch": "ao/ao-1", "worktreePath": "/worktrees/ao-1", "preservedRef": "", "state": "preserved"}},
	})
	srv := newSessionTestServer(t, svc)
	body, status, _ := doRequest(t, srv, http.MethodPost, "/api/v1/sessions/ao-1/restore", "")
	if status != http.StatusConflict {
		t.Fatalf("restore status = %d, want 409; body=%s", status, body)
	}
	var got struct {
		Code    string         `json:"code"`
		Details map[string]any `json:"details"`
	}
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got.Code != "SESSION_RESTORE_FAILED" || got.Details["reason"] != "launch_route_missing" || got.Details["providerSessionSaved"] != true {
		t.Fatalf("response = %#v", got)
	}
	if strings.Contains(string(body), "agentSessionId") || strings.Contains(string(body), "provider-secret") {
		t.Fatalf("restore error leaked provider identity: %s", body)
	}
}

func assertRecoverySessionWire(t *testing.T, session map[string]any) {
	t.Helper()
	if got := session["status"]; got != string(domain.StatusTerminated) {
		t.Fatalf("status = %#v, want terminated", got)
	}
	if session["isTerminated"] != true {
		t.Fatalf("isTerminated = %#v, want true", session["isTerminated"])
	}
	if handle, exists := session["terminalHandleId"]; exists && handle != nil && handle != "" {
		t.Fatalf("terminalHandleId = %#v, want omitted", handle)
	}
	activity, _ := session["activity"].(map[string]any)
	if activity["state"] != string(domain.ActivityExited) {
		t.Fatalf("activity = %#v, want exited recovery projection", activity)
	}
	recovery, ok := session["recovery"].(map[string]any)
	if !ok {
		t.Fatalf("recovery = %#v, want object", session["recovery"])
	}
	if recovery["state"] != string(domain.SessionRecoveryAwaitingRecovery) ||
		recovery["policy"] != string(domain.StartupRestorePreserveOnly) ||
		recovery["runtimeState"] != string(domain.SessionRecoveryRuntimeAbsent) ||
		recovery["providerSessionSaved"] != true {
		t.Fatalf("recovery header = %#v", recovery)
	}
	worktrees, ok := recovery["worktrees"].([]any)
	if !ok || len(worktrees) != 2 {
		t.Fatalf("recovery worktrees = %#v", recovery["worktrees"])
	}
	worktree, _ := worktrees[0].(map[string]any)
	want := map[string]any{
		"repoName":     domain.RootWorkspaceRepoName,
		"branch":       "ao/ao-1/root",
		"baseSha":      "base-123",
		"worktreePath": "/worktrees/ao-1",
		"preservedRef": "refs/ao/preserved/ao-1",
		"state":        domain.SessionWorktreeStatePreserved,
	}
	for key, value := range want {
		if worktree[key] != value {
			t.Fatalf("worktree[%q] = %#v, want %#v; worktree=%#v", key, worktree[key], value, worktree)
		}
	}
	absentWorktree, _ := worktrees[1].(map[string]any)
	if absentWorktree["repoName"] != "api" || absentWorktree["baseSha"] != "base-api" ||
		absentWorktree["worktreePath"] != "/worktrees/ao-1/api" ||
		absentWorktree["state"] != domain.SessionWorktreeStatePreservedRemoved {
		t.Fatalf("absent worktree = %#v", absentWorktree)
	}
	if preservedRef, present := absentWorktree["preservedRef"]; !present || preservedRef != "" {
		t.Fatalf("absent worktree preservedRef = %#v present=%t, want explicit empty string", preservedRef, present)
	}
	requested, _ := session["requestedRoute"].(map[string]any)
	launch, _ := session["launchRoute"].(map[string]any)
	if requested["harness"] != "codex" || requested["model"] != "gpt-5.6-sol" || requested["reasoningEffort"] != "ultra" {
		t.Fatalf("requestedRoute = %#v", requested)
	}
	if launch["harness"] != "codex" || launch["model"] != "gpt-5.6-sol" || launch["reasoningEffort"] != "ultra" {
		t.Fatalf("launchRoute = %#v", launch)
	}
}
