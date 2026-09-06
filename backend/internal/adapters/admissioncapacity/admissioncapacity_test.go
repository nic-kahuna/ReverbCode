package admissioncapacity

import (
	"context"
	"encoding/json"
	"errors"
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
