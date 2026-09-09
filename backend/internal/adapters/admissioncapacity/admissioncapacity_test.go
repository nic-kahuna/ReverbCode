package admissioncapacity

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

func admissionRequest() ports.WorkerAdmissionRequest {
	return ports.WorkerAdmissionRequest{
		ProjectID:  "sample",
		Repository: "https://github.com/owner/sample.git",
		SessionID:  "sample-worker",
		IssueID:    "github:owner/sample#10",
		Operation:  ports.WorkerAdmissionSpawn,
		Route: domain.AgentRoute{
			Harness:         domain.HarnessCodex,
			Model:           "gpt-5.6-sol",
			ReasoningEffort: domain.ReasoningEffortMedium,
		},
	}
}

func admittedResponse(t *testing.T, in ports.WorkerAdmissionRequest) successResponse {
	t.Helper()
	route := wireRoute{Harness: "codex", Model: "gpt-5.6-sol", ReasoningEffort: "medium", Fallback: "none"}
	return successResponse{
		OK: true, Command: commandName, Admitted: true,
		Project: string(in.ProjectID), Repository: "owner/sample", SessionID: string(in.SessionID),
		Ticket: string(in.IssueID), Operation: string(in.Operation), Route: route,
		ClaimID:   "native:" + string(in.SessionID),
		IssueBody: "authoritative issue body", AlreadyHeld: boolPtr(false), Rebound: boolPtr(false),
	}
}

func clientReturning(t *testing.T, response any, exitCode int) (*Client, *string) {
	t.Helper()
	data, err := json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	var requestJSON string
	c := New(t.TempDir())
	c.run = func(_ context.Context, got string) ([]byte, []byte, int, error) {
		requestJSON = got
		return data, nil, exitCode, nil
	}
	return c, &requestJSON
}

func TestAdmitWorkerAcceptsCompleteMatchingIdentity(t *testing.T) {
	in := admissionRequest()
	c, captured := clientReturning(t, admittedResponse(t, in), 0)
	got, err := c.AdmitWorker(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if got.IssueBody != "authoritative issue body" {
		t.Fatalf("issue body = %q", got.IssueBody)
	}
	var sent wireRequest
	if err := decodeExact([]byte(*captured), &sent); err != nil {
		t.Fatal(err)
	}
	if sent.Schema != requestSchema || sent.Repository != in.Repository || sent.Route.Fallback != "none" {
		t.Fatalf("request = %#v", sent)
	}
}

func TestAdmitWorkerRejectsMismatchedSuccessIdentity(t *testing.T) {
	in := admissionRequest()
	tests := map[string]func(*successResponse){
		"project":    func(v *successResponse) { v.Project = "other" },
		"repository": func(v *successResponse) { v.Repository = "owner/other" },
		"session":    func(v *successResponse) { v.SessionID = "other-worker" },
		"ticket":     func(v *successResponse) { v.Ticket = "github:owner/sample#11" },
		"operation":  func(v *successResponse) { v.Operation = "restore" },
		"route":      func(v *successResponse) { v.Route.Model = "other" },
		"claim":      func(v *successResponse) { v.ClaimID = "" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			response := admittedResponse(t, in)
			mutate(&response)
			c, _ := clientReturning(t, response, 0)
			if _, err := c.AdmitWorker(context.Background(), in); err == nil {
				t.Fatal("accepted mismatched response")
			}
		})
	}
}

func TestAdmitWorkerRejectsRefusalMalformedAndUnavailable(t *testing.T) {
	in := admissionRequest()
	t.Run("scope conflict", func(t *testing.T) {
		c, _ := clientReturning(t, failureResponse{OK: boolPtr(false), Command: commandName, Admitted: boolPtr(false), Reason: "scope_conflict", Error: "foreground owner holds path"}, 3)
		if _, err := c.AdmitWorker(context.Background(), in); err == nil || !strings.Contains(err.Error(), "scope_conflict") {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("unknown response field", func(t *testing.T) {
		response := admittedResponse(t, in)
		data, err := json.Marshal(response)
		if err != nil {
			t.Fatal(err)
		}
		data = append(data[:len(data)-1], []byte(`,"extra":true}`)...)
		c := New(t.TempDir())
		c.run = func(context.Context, string) ([]byte, []byte, int, error) { return data, nil, 0, nil }
		if _, err := c.AdmitWorker(context.Background(), in); err == nil {
			t.Fatal("accepted unknown response field")
		}
	})
	t.Run("unavailable", func(t *testing.T) {
		c := New(t.TempDir())
		c.run = func(context.Context, string) ([]byte, []byte, int, error) {
			return nil, nil, -1, errors.New("executable not found")
		}
		if _, err := c.AdmitWorker(context.Background(), in); err == nil {
			t.Fatal("accepted unavailable helper")
		}
	})
	t.Run("timeout", func(t *testing.T) {
		c := New(t.TempDir())
		c.timeout = time.Millisecond
		c.run = func(ctx context.Context, _ string) ([]byte, []byte, int, error) {
			<-ctx.Done()
			return nil, nil, -1, ctx.Err()
		}
		if _, err := c.AdmitWorker(context.Background(), in); err == nil || !strings.Contains(err.Error(), "timed out") {
			t.Fatalf("error = %v", err)
		}
	})
}

func boolPtr(value bool) *bool { return &value }

func TestAdmitWorkerRejectsInvalidNativeRequest(t *testing.T) {
	for name, mutate := range map[string]func(*ports.WorkerAdmissionRequest){
		"repository": func(v *ports.WorkerAdmissionRequest) { v.Repository = "https://example.com/owner/sample" },
		"ticket":     func(v *ports.WorkerAdmissionRequest) { v.IssueID = "github:owner/other#10" },
		"session":    func(v *ports.WorkerAdmissionRequest) { v.SessionID = " " },
		"route":      func(v *ports.WorkerAdmissionRequest) { v.Route.Model = "" },
		"operation":  func(v *ports.WorkerAdmissionRequest) { v.Operation = "delete" },
	} {
		t.Run(name, func(t *testing.T) {
			in := admissionRequest()
			mutate(&in)
			c := New(t.TempDir())
			c.run = func(context.Context, string) ([]byte, []byte, int, error) {
				t.Fatal("helper called for invalid request")
				return nil, nil, 0, nil
			}
			if _, err := c.AdmitWorker(context.Background(), in); err == nil {
				t.Fatal("accepted invalid request")
			}
		})
	}
}

func TestAdmitWorkerDefaultBudgetAllowsFreshMultiHistoryChecks(t *testing.T) {
	for _, useFallback := range []bool{false, true} {
		t.Run(map[bool]string{false: "new client", true: "unset timeout"}[useFallback], func(t *testing.T) {
			in := admissionRequest()
			response, err := json.Marshal(admittedResponse(t, in))
			if err != nil {
				t.Fatal(err)
			}
			c := New(t.TempDir())
			if useFallback {
				c.timeout = 0
			}
			c.run = func(ctx context.Context, _ string) ([]byte, []byte, int, error) {
				deadline, bounded := ctx.Deadline()
				remaining := time.Until(deadline)
				// The real six-history admission checks took 41s. Inspect the
				// actual command context instead of waiting that long in a test.
				if !bounded || remaining <= 41*time.Second || remaining > 90*time.Second {
					t.Fatalf("fresh-check budget = %s, bounded=%v", remaining, bounded)
				}
				return response, nil, 0, nil
			}
			out, err := c.AdmitWorker(context.Background(), in)
			if err != nil || out.IssueBody != "authoritative issue body" {
				t.Fatalf("admission = %+v, %v", out, err)
			}
		})
	}
}

func TestAdmitWorkerPreservesEarlierCallerDeadlineAndCancellation(t *testing.T) {
	for _, cancelEarly := range []bool{false, true} {
		t.Run(map[bool]string{false: "deadline", true: "cancellation"}[cancelEarly], func(t *testing.T) {
			parent, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			parentDeadline, _ := parent.Deadline()
			c := New(t.TempDir())
			c.run = func(ctx context.Context, _ string) ([]byte, []byte, int, error) {
				if deadline, _ := ctx.Deadline(); !deadline.Equal(parentDeadline) {
					t.Fatalf("caller deadline changed: %s -> %s", parentDeadline, deadline)
				}
				if cancelEarly {
					cancel()
				}
				<-ctx.Done()
				return nil, nil, -1, ctx.Err()
			}
			out, err := c.AdmitWorker(parent, admissionRequest())
			want := context.DeadlineExceeded
			if cancelEarly {
				want = context.Canceled
			}
			if !errors.Is(err, want) || out.IssueBody != "" {
				t.Fatalf("canceled admission = %+v, %v; want %v", out, err, want)
			}
		})
	}
}

func TestAdmitWorkerDeadlineCancelsRealHelperProcess(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("helper fixture uses a POSIX launcher")
	}
	bin := t.TempDir()
	started := filepath.Join(t.TempDir(), "started")
	script := "#!/bin/sh\nprintf started > \"$AO_TEST_ADMISSION_STARTED\"\nexec sleep 30\n"
	if err := os.WriteFile(filepath.Join(bin, executableName), []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AO_TEST_ADMISSION_STARTED", started)
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	c := New(t.TempDir())
	c.timeout = 500 * time.Millisecond
	before := time.Now()
	out, err := c.AdmitWorker(context.Background(), admissionRequest())
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "helper timed out") || out.IssueBody != "" {
		t.Fatalf("timed-out subprocess admission = %+v, %v", out, err)
	}
	if elapsed := time.Since(before); elapsed > 3*time.Second {
		t.Fatalf("helper did not exit promptly after its deadline: %s", elapsed)
	}
	if data, err := os.ReadFile(started); err != nil || string(data) != "started" {
		t.Fatalf("real helper never started: %q, %v", data, err)
	}
}
