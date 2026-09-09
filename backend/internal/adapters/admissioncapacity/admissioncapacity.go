// Package admissioncapacity invokes the installed desktop-projects capacity
// helper for native worker admission. It is a strict command adapter: the
// executable and command are fixed, input is one JSON argument, and every
// response field is validated before the daemon accepts ownership.
package admissioncapacity

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

const (
	executableName = "ao-execution-capacity"
	requestSchema  = "ao-worker-admission/v1"
	commandName    = "admit-worker"
	// Fresh ownership and retained-worker checks can span several repositories
	// and histories. Bound their aggregate work without truncating normal
	// multi-history admission; an earlier caller deadline still takes precedence.
	defaultTimeout = 90 * time.Second
)

var componentPattern = regexp.MustCompile(`^[a-z0-9_.-]+$`)
var ticketPattern = regexp.MustCompile(`^github:([a-z0-9_.-]+)/([a-z0-9_.-]+)#[1-9]\d*$`)

type commandRunner func(context.Context, string) ([]byte, []byte, int, error)

// Client is the production implementation of ports.WorkerAdmitter.
type Client struct {
	dataDir string
	timeout time.Duration
	run     commandRunner
}

var _ ports.WorkerAdmitter = (*Client)(nil)

// New returns an adapter pinned to the installed ao-execution-capacity binary.
func New(dataDir string) *Client {
	c := &Client{dataDir: dataDir, timeout: defaultTimeout}
	c.run = c.runCommand
	return c
}

type wireRoute struct {
	Harness         string `json:"harness"`
	Model           string `json:"model"`
	ReasoningEffort string `json:"reasoningEffort"`
	Fallback        string `json:"fallback"`
}

type wireRequest struct {
	Schema     string    `json:"schema"`
	Project    string    `json:"project"`
	Repository string    `json:"repository"`
	SessionID  string    `json:"session_id"`
	Ticket     string    `json:"ticket"`
	Operation  string    `json:"operation"`
	Route      wireRoute `json:"route"`
}

type successResponse struct {
	OK          bool      `json:"ok"`
	Command     string    `json:"command"`
	Admitted    bool      `json:"admitted"`
	Project     string    `json:"project"`
	Repository  string    `json:"repository"`
	SessionID   string    `json:"session_id"`
	Ticket      string    `json:"ticket"`
	Operation   string    `json:"operation"`
	Route       wireRoute `json:"route"`
	ClaimID     string    `json:"claim_id"`
	IssueBody   string    `json:"issue_body"`
	AlreadyHeld *bool     `json:"already_held"`
	Rebound     *bool     `json:"rebound"`
}

type failureResponse struct {
	OK       *bool  `json:"ok"`
	Command  string `json:"command"`
	Admitted *bool  `json:"admitted"`
	Reason   string `json:"reason"`
	Error    string `json:"error"`
}

// AdmitWorker executes one bounded admission request. Any command, decoding,
// or identity uncertainty is returned as an error and must block worker effects.
func (c *Client) AdmitWorker(ctx context.Context, in ports.WorkerAdmissionRequest) (ports.WorkerAdmissionResult, error) {
	req, repo, err := buildRequest(in)
	if err != nil {
		return ports.WorkerAdmissionResult{}, err
	}
	payload, err := json.Marshal(req)
	if err != nil {
		return ports.WorkerAdmissionResult{}, fmt.Errorf("worker admission: encode request: %w", err)
	}
	timeout := c.timeout
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	callCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	stdout, stderr, exitCode, runErr := c.run(callCtx, string(payload))
	if runErr != nil {
		if errors.Is(callCtx.Err(), context.DeadlineExceeded) {
			return ports.WorkerAdmissionResult{}, fmt.Errorf("worker admission: helper timed out: %w", callCtx.Err())
		}
		return ports.WorkerAdmissionResult{}, fmt.Errorf("worker admission: helper unavailable or failed: %w", runErr)
	}
	if exitCode != 0 {
		var denied failureResponse
		if err := decodeExact(stdout, &denied); err != nil {
			return ports.WorkerAdmissionResult{}, fmt.Errorf("worker admission: malformed refusal (exit %d): %w", exitCode, err)
		}
		if denied.OK == nil || *denied.OK || denied.Admitted == nil || *denied.Admitted || denied.Command != commandName || strings.TrimSpace(denied.Reason) == "" || strings.TrimSpace(denied.Error) == "" {
			return ports.WorkerAdmissionResult{}, fmt.Errorf("worker admission: malformed refusal (exit %d)", exitCode)
		}
		return ports.WorkerAdmissionResult{}, fmt.Errorf("worker admission refused (%s): %s", denied.Reason, denied.Error)
	}
	if len(bytes.TrimSpace(stderr)) != 0 {
		return ports.WorkerAdmissionResult{}, errors.New("worker admission: helper wrote stderr on success")
	}
	var out successResponse
	if err := decodeExact(stdout, &out); err != nil {
		return ports.WorkerAdmissionResult{}, fmt.Errorf("worker admission: malformed success: %w", err)
	}
	if err := validateSuccess(req, repo, out); err != nil {
		return ports.WorkerAdmissionResult{}, fmt.Errorf("worker admission: invalid success: %w", err)
	}
	return ports.WorkerAdmissionResult{IssueBody: out.IssueBody}, nil
}

func (c *Client) runCommand(ctx context.Context, requestJSON string) ([]byte, []byte, int, error) {
	cmd := exec.CommandContext(ctx, executableName, commandName, "--request-json", requestJSON)
	cmd.Env = withEnv(os.Environ(), "AO_DATA_DIR", c.dataDir)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err == nil {
		return stdout.Bytes(), stderr.Bytes(), 0, nil
	}
	if ctx.Err() != nil {
		return stdout.Bytes(), stderr.Bytes(), -1, ctx.Err()
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return stdout.Bytes(), stderr.Bytes(), exitErr.ExitCode(), nil
	}
	return stdout.Bytes(), stderr.Bytes(), -1, err
}

func buildRequest(in ports.WorkerAdmissionRequest) (wireRequest, string, error) {
	project := string(in.ProjectID)
	sessionID := string(in.SessionID)
	if strings.TrimSpace(project) == "" || strings.TrimSpace(project) != project {
		return wireRequest{}, "", errors.New("worker admission: project identity is invalid")
	}
	if strings.TrimSpace(sessionID) == "" || strings.TrimSpace(sessionID) != sessionID {
		return wireRequest{}, "", errors.New("worker admission: session identity is invalid")
	}
	repo, err := canonicalRepository(in.Repository)
	if err != nil {
		return wireRequest{}, "", fmt.Errorf("worker admission: %w", err)
	}
	issueMatch := ticketPattern.FindStringSubmatch(string(in.IssueID))
	if len(issueMatch) != 3 || issueMatch[1]+"/"+issueMatch[2] != repo {
		return wireRequest{}, "", errors.New("worker admission: ticket identity does not match repository")
	}
	if err := in.Route.Validate(); err != nil {
		return wireRequest{}, "", fmt.Errorf("worker admission: route: %w", err)
	}
	if in.Operation != ports.WorkerAdmissionSpawn && in.Operation != ports.WorkerAdmissionRestore {
		return wireRequest{}, "", errors.New("worker admission: operation is invalid")
	}
	route := wireRoute{Harness: string(in.Route.Harness), Model: in.Route.Model, ReasoningEffort: string(in.Route.ReasoningEffort), Fallback: "none"}
	return wireRequest{Schema: requestSchema, Project: project, Repository: in.Repository, SessionID: sessionID, Ticket: string(in.IssueID), Operation: string(in.Operation), Route: route}, repo, nil
}

func validateSuccess(req wireRequest, repo string, out successResponse) error {
	if !out.OK || !out.Admitted || out.Command != commandName {
		return errors.New("success flags are inconsistent")
	}
	if out.Project != req.Project || out.Repository != repo || out.SessionID != req.SessionID || out.Ticket != req.Ticket || out.Operation != req.Operation || out.Route != req.Route {
		return errors.New("response identity does not match request")
	}
	if out.AlreadyHeld == nil || out.Rebound == nil {
		return errors.New("response replay flags are missing")
	}
	if *out.AlreadyHeld && *out.Rebound {
		return errors.New("response cannot be both already-held and rebound")
	}
	if strings.TrimSpace(out.ClaimID) == "" || out.IssueBody == "" {
		return errors.New("accepted claim identity or issue body is empty")
	}
	return nil
}

func decodeExact(data []byte, out any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(out); err != nil {
		return err
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("response contains trailing data")
	}
	return nil
}

func canonicalRepository(raw string) (string, error) {
	value := strings.TrimSpace(raw)
	if value == "" || value != raw {
		return "", errors.New("repository identity is invalid")
	}
	var slug string
	switch {
	case strings.HasPrefix(value, "git@github.com:"):
		slug = strings.TrimPrefix(value, "git@github.com:")
	case strings.Contains(value, "://"):
		u, err := url.Parse(value)
		if err != nil || !strings.EqualFold(u.Hostname(), "github.com") || u.RawQuery != "" || u.Fragment != "" {
			return "", errors.New("repository is not a canonical GitHub origin")
		}
		slug = strings.TrimPrefix(u.Path, "/")
	default:
		slug = value
	}
	slug = strings.TrimSuffix(slug, ".git")
	parts := strings.Split(slug, "/")
	if len(parts) != 2 {
		return "", errors.New("repository is not an owner/repo GitHub identity")
	}
	parts[0], parts[1] = strings.ToLower(parts[0]), strings.ToLower(parts[1])
	if !componentPattern.MatchString(parts[0]) || !componentPattern.MatchString(parts[1]) {
		return "", errors.New("repository contains invalid identity characters")
	}
	return parts[0] + "/" + parts[1], nil
}

func withEnv(environ []string, key, value string) []string {
	prefix := key + "="
	out := make([]string, 0, len(environ)+1)
	for _, item := range environ {
		if !strings.HasPrefix(item, prefix) {
			out = append(out, item)
		}
	}
	return append(out, prefix+value)
}
