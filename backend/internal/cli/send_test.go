package cli

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/runfile"
)

// sendCapture records the request body and path the CLI hit.
type sendCapture struct {
	body string
	path string
}

// writeRunFileFor points the CLI's run-file at srv so postJSON dials the test
// server. It mirrors the run-file convention the other CLI tests use.
func writeRunFileFor(t *testing.T, cfg testConfig, srv *httptest.Server) {
	t.Helper()
	if err := runfile.Write(cfg.runFile, runfile.Info{
		PID: os.Getpid(), Port: serverPort(t, srv.URL), StartedAt: time.Unix(100, 0).UTC(),
	}); err != nil {
		t.Fatalf("write run-file: %v", err)
	}
}

func sendServer(t *testing.T, status int, respBody string) (*httptest.Server, *sendCapture) {
	t.Helper()
	capture := &sendCapture{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.NotFound(w, r)
			return
		}
		if !strings.HasPrefix(r.URL.Path, "/api/v1/sessions/") || (!strings.HasSuffix(r.URL.Path, "/send") && !strings.HasSuffix(r.URL.Path, "/send-admitted")) {
			http.NotFound(w, r)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		capture.body = string(body)
		capture.path = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = io.WriteString(w, respBody)
	}))
	t.Cleanup(srv.Close)
	return srv, capture
}

func TestSend_RequireAdmissionUsesDistinctRoute(t *testing.T) {
	for _, required := range []bool{false, true} {
		t.Run(map[bool]string{false: "ordinary", true: "admitted"}[required], func(t *testing.T) {
			t.Setenv("AO_SESSION_ID", "")
			cfg := setConfigEnv(t)
			srv, capture := sendServer(t, http.StatusOK, `{"ok":true,"sessionId":"demo-1","message":"continue"}`)
			writeRunFileFor(t, cfg, srv)
			args := []string{"send", "--session", "demo-1", "--message", "continue"}
			if required {
				args = append(args, "--require-admission")
			}
			_, stderr, err := executeCLI(t, Deps{ProcessAlive: func(int) bool { return true }}, args...)
			if err != nil {
				t.Fatalf("send: %v; stderr=%s", err, stderr)
			}
			var got map[string]json.RawMessage
			if err := json.Unmarshal([]byte(capture.body), &got); err != nil {
				t.Fatal(err)
			}
			wantPath := "/api/v1/sessions/demo-1/send"
			if required {
				wantPath += "-admitted"
			}
			if capture.path != wantPath {
				t.Fatalf("path=%q want=%q", capture.path, wantPath)
			}
			if len(got) != 1 || string(got["message"]) != `"continue"` {
				t.Fatalf("request=%s; want only message", capture.body)
			}
		})
	}
}

// The pre-admission daemon only registers /send and decodes a message-only DTO.
// json.Decoder ignores unknown fields, so an optional JSON flag cannot safely
// introduce admission enforcement to that endpoint.
func TestSend_RequireAdmissionRejectsOlderDaemonBeforeDelivery(t *testing.T) {
	t.Setenv("AO_SESSION_ID", "")
	cfg := setConfigEnv(t)
	var deliveries, requests atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/sessions/demo-1/send", func(w http.ResponseWriter, r *http.Request) {
		var in struct {
			Message string `json:"message"`
		}
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}
		deliveries.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"ok":true}`)
	})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/v1/sessions/") {
			requests.Add(1)
		}
		mux.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	writeRunFileFor(t, cfg, srv)

	// Establish the compatibility hazard: the old endpoint accepts and
	// delivers a message even when an unknown guard field is present.
	resp, err := http.Post(srv.URL+"/api/v1/sessions/demo-1/send", "application/json", strings.NewReader(`{"message":"old guard","requireAdmission":true}`))
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK || deliveries.Load() != 1 {
		t.Fatalf("legacy baseline status=%d deliveries=%d", resp.StatusCode, deliveries.Load())
	}
	deliveries.Store(0)
	requests.Store(0)

	_, stderr, err := executeCLI(t, Deps{ProcessAlive: func(int) bool { return true }}, "send", "--session", "demo-1", "--message", "background turn", "--require-admission")
	if ExitCode(err) != 1 || !strings.Contains(err.Error()+stderr, "404") {
		t.Fatalf("error=%v stderr=%s; want unsupported endpoint failure", err, stderr)
	}
	if deliveries.Load() != 0 || requests.Load() != 1 {
		t.Fatalf("guarded request delivered or retried: deliveries=%d requests=%d", deliveries.Load(), requests.Load())
	}

	// Compatibility for the ordinary command remains intact.
	_, stderr, err = executeCLI(t, Deps{ProcessAlive: func(int) bool { return true }}, "send", "--session", "demo-1", "--message", "foreground message")
	if err != nil || deliveries.Load() != 1 {
		t.Fatalf("ordinary send error=%v stderr=%s deliveries=%d", err, stderr, deliveries.Load())
	}
}

func TestSend_RequireAdmissionErrorAndUsage(t *testing.T) {
	cfg := setConfigEnv(t)
	srv, _ := sendServer(t, http.StatusConflict, `{"error":"conflict","code":"PROJECT_ADMISSION_PAUSED","message":"Admission paused","requestId":"req-send"}`)
	writeRunFileFor(t, cfg, srv)
	_, stderr, err := executeCLI(t, Deps{ProcessAlive: func(int) bool { return true }}, "send", "--session", "demo-1", "--message", "continue", "--require-admission")
	if ExitCode(err) != 1 || !strings.Contains(err.Error()+stderr, "PROJECT_ADMISSION_PAUSED") || !strings.Contains(err.Error()+stderr, "req-send") {
		t.Fatalf("error=%v stderr=%s", err, stderr)
	}
	_, _, err = executeCLI(t, Deps{}, "send", "--session", "demo-1", "--message", "continue", "--require-admission=maybe")
	if ExitCode(err) != 2 {
		t.Fatalf("invalid bool: %v exit=%d", err, ExitCode(err))
	}
}

func TestSend_Success(t *testing.T) {
	t.Setenv("AO_SESSION_ID", "")
	cfg := setConfigEnv(t)
	srv, capture := sendServer(t, http.StatusOK,
		`{"ok":true,"sessionId":"demo-1","message":"hello agent"}`)
	writeRunFileFor(t, cfg, srv)

	_, errOut, err := executeCLI(t, Deps{
		ProcessAlive: func(int) bool { return true },
	}, "send", "--session", "demo-1", "--message", "hello agent")
	if err != nil {
		t.Fatalf("unexpected error: %v\nstderr=%s", err, errOut)
	}
	if capture.path != "/api/v1/sessions/demo-1/send" {
		t.Errorf("path = %q, want /api/v1/sessions/demo-1/send", capture.path)
	}
	var req struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal([]byte(capture.body), &req); err != nil {
		t.Fatalf("decode body: %v\nbody=%s", err, capture.body)
	}
	if req.Message != "hello agent" {
		t.Errorf("captured message = %q, want %q", req.Message, "hello agent")
	}
}

func TestSend_PrefixesMessageWithSenderSessionID(t *testing.T) {
	t.Setenv("AO_SESSION_ID", "aa-47")
	cfg := setConfigEnv(t)
	srv, capture := sendServer(t, http.StatusOK,
		`{"ok":true,"sessionId":"demo-1","message":"hi"}`)
	writeRunFileFor(t, cfg, srv)

	_, errOut, err := executeCLI(t, Deps{
		ProcessAlive: func(int) bool { return true },
	}, "send", "--session", "demo-1", "--message", "  hi  ")
	if err != nil {
		t.Fatalf("unexpected error: %v\nstderr=%s", err, errOut)
	}
	var req struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal([]byte(capture.body), &req); err != nil {
		t.Fatalf("decode body: %v\nbody=%s", err, capture.body)
	}
	want := "[from aa-47]   hi  "
	if req.Message != want {
		t.Errorf("captured message = %q, want %q", req.Message, want)
	}
}

func TestSend_BlankSenderSessionIDDoesNotPrefixMessage(t *testing.T) {
	t.Setenv("AO_SESSION_ID", " \t ")
	cfg := setConfigEnv(t)
	srv, capture := sendServer(t, http.StatusOK,
		`{"ok":true,"sessionId":"demo-1","message":"hello agent"}`)
	writeRunFileFor(t, cfg, srv)

	_, errOut, err := executeCLI(t, Deps{
		ProcessAlive: func(int) bool { return true },
	}, "send", "--session", "demo-1", "--message", "hello agent")
	if err != nil {
		t.Fatalf("unexpected error: %v\nstderr=%s", err, errOut)
	}
	var req struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal([]byte(capture.body), &req); err != nil {
		t.Fatalf("decode body: %v\nbody=%s", err, capture.body)
	}
	if req.Message != "hello agent" {
		t.Errorf("captured message = %q, want %q", req.Message, "hello agent")
	}
}

func TestSend_PreservesMessageWhitespace(t *testing.T) {
	t.Setenv("AO_SESSION_ID", "")
	cfg := setConfigEnv(t)
	srv, capture := sendServer(t, http.StatusOK, `{"ok":true,"sessionId":"demo-1","message":"hi"}`)
	writeRunFileFor(t, cfg, srv)

	_, _, err := executeCLI(t, Deps{
		ProcessAlive: func(int) bool { return true },
	}, "send", "--session", "demo-1", "--message", "  hi  ")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var req struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal([]byte(capture.body), &req); err != nil {
		t.Fatalf("decode body: %v\nbody=%s", err, capture.body)
	}
	if req.Message != "  hi  " {
		t.Errorf("server received %q, want preserved whitespace", req.Message)
	}
}

func TestSend_EmptyMessageIsUsageError(t *testing.T) {
	setConfigEnv(t)
	_, _, err := executeCLI(t, Deps{}, "send", "--session", "demo-1", "--message", "   ")
	if err == nil {
		t.Fatal("expected usage error for empty message")
	}
	if got := ExitCode(err); got != 2 {
		t.Fatalf("exit code = %d, want 2", got)
	}
	if !strings.Contains(err.Error(), "--message is required") {
		t.Fatalf("error missing usage message: %v", err)
	}
}

func TestSend_MissingSessionIsUsageError(t *testing.T) {
	setConfigEnv(t)
	_, _, err := executeCLI(t, Deps{}, "send", "--message", "hi")
	if err == nil {
		t.Fatal("expected usage error for missing --session")
	}
	if got := ExitCode(err); got != 2 {
		t.Fatalf("exit code = %d, want 2", got)
	}
}

func TestSend_ServerBadRequestExits1(t *testing.T) {
	cfg := setConfigEnv(t)
	srv, _ := sendServer(t, http.StatusBadRequest,
		`{"error":"bad_request","code":"MESSAGE_REQUIRED","message":"Message is required"}`)
	writeRunFileFor(t, cfg, srv)

	_, errOut, err := executeCLI(t, Deps{
		ProcessAlive: func(int) bool { return true },
	}, "send", "--session", "demo-1", "--message", "hi")
	if err == nil {
		t.Fatal("expected runtime error from 400")
	}
	if got := ExitCode(err); got != 1 {
		t.Fatalf("exit code = %d, want 1", got)
	}
	if !strings.Contains(err.Error(), "MESSAGE_REQUIRED") && !strings.Contains(errOut, "MESSAGE_REQUIRED") {
		t.Fatalf("error did not surface the server error envelope: %v\nstderr=%s", err, errOut)
	}
}

func TestSend_ServerNotFoundExits1(t *testing.T) {
	cfg := setConfigEnv(t)
	srv, _ := sendServer(t, http.StatusNotFound,
		`{"error":"not_found","code":"SESSION_NOT_FOUND","message":"Unknown session"}`)
	writeRunFileFor(t, cfg, srv)

	_, _, err := executeCLI(t, Deps{
		ProcessAlive: func(int) bool { return true },
	}, "send", "--session", "missing", "--message", "hi")
	if err == nil {
		t.Fatal("expected runtime error from 404")
	}
	if got := ExitCode(err); got != 1 {
		t.Fatalf("exit code = %d, want 1", got)
	}
}

func TestSend_ServerInternalErrorExits1(t *testing.T) {
	cfg := setConfigEnv(t)
	srv, _ := sendServer(t, http.StatusInternalServerError,
		`{"error":"internal","code":"SESSION_OPERATION_FAILED","message":"Session operation failed"}`)
	writeRunFileFor(t, cfg, srv)

	_, errOut, err := executeCLI(t, Deps{
		ProcessAlive: func(int) bool { return true },
	}, "send", "--session", "demo-1", "--message", "hi")
	if err == nil {
		t.Fatal("expected runtime error from 500")
	}
	if got := ExitCode(err); got != 1 {
		t.Fatalf("exit code = %d, want 1", got)
	}
	// Regression guard: a future change that swallows the API envelope and
	// prints only "daemon returned HTTP 500" would silently hide what the
	// daemon was trying to tell the operator.
	if !strings.Contains(err.Error(), "SESSION_OPERATION_FAILED") && !strings.Contains(errOut, "SESSION_OPERATION_FAILED") {
		t.Fatalf("error did not surface the server error envelope: %v\nstderr=%s", err, errOut)
	}
}

func TestSend_DaemonNotRunningExits1(t *testing.T) {
	setConfigEnv(t)
	_, _, err := executeCLI(t, Deps{}, "send", "--session", "demo-1", "--message", "hi")
	if err == nil {
		t.Fatal("expected error when daemon is not running")
	}
	if got := ExitCode(err); got != 1 {
		t.Fatalf("exit code = %d, want 1", got)
	}
}

func TestSend_NetworkErrorExits1(t *testing.T) {
	cfg := setConfigEnv(t)
	// Start and immediately close a server so the run-file points at a closed port.
	srv, _ := sendServer(t, http.StatusOK, "{}")
	writeRunFileFor(t, cfg, srv)
	srv.Close()

	_, _, err := executeCLI(t, Deps{
		ProcessAlive: func(int) bool { return true },
	}, "send", "--session", "demo-1", "--message", "hi")
	if err == nil {
		t.Fatal("expected runtime error from network failure")
	}
	if got := ExitCode(err); got != 1 {
		t.Fatalf("exit code = %d, want 1", got)
	}
}
