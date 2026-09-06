package custody

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

type authorityRequest struct {
	Schema             string  `json:"schema"`
	Project            string  `json:"project"`
	ClaimID            string  `json:"claim_id"`
	SessionID          string  `json:"session_id"`
	AttemptID          string  `json:"attempt_id"`
	Operation          string  `json:"operation"`
	ExpectedGeneration *int64  `json:"expected_generation"`
	Result             *string `json:"result"`
}

type Accepted struct {
	Schema          string  `json:"schema"`
	OK              bool    `json:"ok"`
	Command         string  `json:"command"`
	Admitted        bool    `json:"admitted"`
	WriteAuthorized bool    `json:"write_authorized"`
	Generation      int64   `json:"registry_generation"`
	IssueBody       *string `json:"issue_body"`
	Reason          string  `json:"reason"`
	Error           string  `json:"error"`
	Claim           struct {
		ClaimID  string `json:"claim_id"`
		Executor string `json:"executor"`
		Project  string `json:"project"`
		Owner    string `json:"owner"`
		Native   struct {
			AttemptID  string          `json:"attempt_id"`
			SessionID  string          `json:"session_id"`
			Operation  string          `json:"operation"`
			Phase      string          `json:"phase"`
			Generation int64           `json:"generation"`
			Scope      json.RawMessage `json:"scope"`
			Source     struct {
				Evidence struct {
					Route domain.AgentRoute `json:"route"`
				} `json:"evidence"`
			} `json:"source"`
		} `json:"native"`
	} `json:"claim"`
}

type Authority interface {
	Transition(context.Context, string, Attempt, *string) (Accepted, error)
}

// CommandAuthority uses the existing single local authority. No JSON request can
// select an executable, data directory, registry or injected resolver.
type CommandAuthority struct{ dataDir, executable string }

func NewCommandAuthority(dataDir string) (*CommandAuthority, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}
	return &CommandAuthority{dataDir: dataDir, executable: filepath.Join(home, ".local", "bin", "ao-execution-capacity")}, nil
}

func (c *CommandAuthority) Transition(ctx context.Context, command string, a Attempt, result *string) (Accepted, error) {
	if command != "native-reserve" && command != "native-launch" && command != "native-result" {
		return Accepted{}, ErrUnknown
	}
	var expected *int64
	if a.ClaimGeneration > 0 {
		v := a.ClaimGeneration
		expected = &v
	}
	req := authorityRequest{Schema: "ao-capacity-native-request/v1", Project: a.Project, ClaimID: a.ClaimID, SessionID: a.SessionID, AttemptID: a.AttemptID, Operation: a.Operation, ExpectedGeneration: expected, Result: result}
	payload, err := json.Marshal(req)
	if err != nil {
		return Accepted{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, c.executable, command, "--request-json", string(payload))
	cmd.Env = append(withoutEnv(os.Environ(), "AO_DATA_DIR"), "AO_DATA_DIR="+c.dataDir)
	var stdout, stderr boundedBuffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err = cmd.Run()
	var out Accepted
	if e := checkpointUniqueJSON(stdout.Bytes()); e != nil {
		return out, fmt.Errorf("%w: ambiguous authority JSON: %v", ErrAdmission, e)
	}
	var fields map[string]json.RawMessage
	if e := json.Unmarshal(stdout.Bytes(), &fields); e != nil || fields == nil {
		return out, ErrAdmission
	}
	for _, key := range []string{"schema", "ok", "command", "admitted", "write_authorized"} {
		if _, exists := fields[key]; !exists {
			return out, fmt.Errorf("%w: authority omitted %s", ErrAdmission, key)
		}
	}
	if decodeErr := json.Unmarshal(stdout.Bytes(), &out); decodeErr != nil {
		return out, fmt.Errorf("%w: invalid authority response: %v", ErrAdmission, decodeErr)
	}
	if err != nil || !out.OK || !out.Admitted || out.WriteAuthorized || out.Schema != "ao-capacity-native-result/v1" || out.Command != command {
		return out, fmt.Errorf("%w: %s: %s", ErrAdmission, out.Reason, out.Error)
	}
	for _, key := range []string{"registry_generation", "claim", "issue_body"} {
		if _, exists := fields[key]; !exists {
			return out, fmt.Errorf("%w: successful authority omitted %s", ErrAdmission, key)
		}
	}
	claim := out.Claim
	n := claim.Native
	if out.Generation < 1 || claim.ClaimID != a.ClaimID || claim.Project != a.Project || claim.Owner != a.SessionID || claim.Executor != "ao" || n.AttemptID != a.AttemptID || n.SessionID != a.SessionID || n.Operation != a.Operation || n.Generation < 1 {
		return out, fmt.Errorf("%w: authority identity mismatch", ErrAdmission)
	}
	expectedPhase := map[string]string{"native-reserve": "preparing", "native-launch": "launching"}[command]
	if command == "native-result" {
		if result == nil {
			return out, ErrAdmission
		}
		expectedPhase = *result
	}
	if n.Phase != expectedPhase {
		return out, fmt.Errorf("%w: authority phase mismatch", ErrAdmission)
	}
	if command != "native-result" {
		if out.IssueBody == nil || *out.IssueBody == "" || len(n.Scope) == 0 || string(n.Scope) == "null" || n.Source.Evidence.Route.Harness == "" || n.Source.Evidence.Route.Model == "" {
			return out, fmt.Errorf("%w: incomplete accepted source", ErrAdmission)
		}
	}
	return out, nil
}

func withoutEnv(env []string, key string) []string {
	out := make([]string, 0, len(env))
	for _, v := range env {
		if !strings.HasPrefix(v, key+"=") {
			out = append(out, v)
		}
	}
	return out
}
