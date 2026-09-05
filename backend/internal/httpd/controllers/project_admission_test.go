package controllers_test

import (
	"net/http"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/admission"
	projectsvc "github.com/aoagents/agent-orchestrator/backend/internal/service/project"
)

func TestProjectAdmissionAPIReadPauseAndAllow(t *testing.T) {
	srv := newTestServer(t)
	repo := gitRepo(t, "admission")
	body, status, _ := doRequest(t, srv, "POST", "/api/v1/projects", `{"path":`+quote(repo)+`,"projectId":"demo"}`)
	if status != http.StatusCreated {
		t.Fatalf("create = %d: %s", status, body)
	}
	for _, tc := range []struct {
		method, body string
		paused       bool
	}{
		{http.MethodGet, "", false},
		{http.MethodPut, `{"paused":true}`, true},
		{http.MethodGet, "", true},
		{http.MethodPut, `{"paused":false}`, false},
		{http.MethodGet, "", false},
	} {
		body, status, headers := doRequest(t, srv, tc.method, "/api/v1/projects/demo/admission", tc.body)
		assertJSON(t, headers)
		if status != http.StatusOK {
			t.Fatalf("%s admission = %d: %s", tc.method, status, body)
		}
		var state projectsvc.AdmissionState
		mustJSON(t, body, &state)
		if state.ProjectID != "demo" || state.AdmissionPaused != tc.paused || state.Scope != "new_launches_only" || !state.ExistingSessionsMayBeRunning {
			t.Fatalf("admission state = %#v", state)
		}
	}
}

func TestProjectAdmissionAPIRejectsMissingNullAndMalformedBody(t *testing.T) {
	srv := newTestServer(t)
	repo := gitRepo(t, "admission-validation")
	body, status, _ := doRequest(t, srv, "POST", "/api/v1/projects", `{"path":`+quote(repo)+`,"projectId":"demo"}`)
	if status != http.StatusCreated {
		t.Fatalf("create = %d: %s", status, body)
	}
	_, status, _ = doRequest(t, srv, http.MethodPut, "/api/v1/projects/demo/admission", `{"paused":true}`)
	if status != http.StatusOK {
		t.Fatalf("pause = %d", status)
	}
	for _, raw := range []string{"", `{}`, `null`, `{"paused":null}`, `{"paused":"false"}`, `{"paused":false,"extra":1}`, `{"paused":false}{"paused":true}`} {
		t.Run(raw, func(t *testing.T) {
			body, status, _ := doRequest(t, srv, http.MethodPut, "/api/v1/projects/demo/admission", raw)
			assertErrorCode(t, body, status, http.StatusBadRequest, "INVALID_JSON")
			body, status, _ = doRequest(t, srv, http.MethodGet, "/api/v1/projects/demo/admission", "")
			var state projectsvc.AdmissionState
			mustJSON(t, body, &state)
			if status != http.StatusOK || !state.AdmissionPaused {
				t.Fatalf("invalid request changed admission: %d %s", status, body)
			}
		})
	}
}

func TestProjectAdmissionAPINotFound(t *testing.T) {
	srv := newTestServer(t)
	for _, method := range []string{http.MethodGet, http.MethodPut} {
		body, status, _ := doRequest(t, srv, method, "/api/v1/projects/missing/admission", `{"paused":true}`)
		assertErrorCode(t, body, status, http.StatusNotFound, "PROJECT_NOT_FOUND")
	}
}

func TestReviewTriggerAdmissionErrors(t *testing.T) {
	for _, tc := range []struct {
		err  error
		code string
	}{{admission.ErrPaused, "PROJECT_ADMISSION_PAUSED"}, {admission.ErrUncertain, "PROJECT_ADMISSION_UNKNOWN"}} {
		t.Run(tc.code, func(t *testing.T) {
			srv := newReviewTestServer(t, &fakeReviewService{triggerErr: tc.err})
			body, status, _ := doRequest(t, srv, http.MethodPost, "/api/v1/sessions/demo-1/reviews/trigger", "")
			assertErrorCode(t, body, status, http.StatusConflict, tc.code)
		})
	}
}
