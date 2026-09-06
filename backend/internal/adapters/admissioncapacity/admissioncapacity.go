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
	"math"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

const (
	executableName = "ao-execution-capacity"
	requestSchema  = "ao-worker-admission/v1"
	commandName    = "admit-worker"
	defaultTimeout = 30 * time.Second
)

var (
	componentPattern = regexp.MustCompile(`^[a-z0-9_.-]+$`)
	ticketPattern    = regexp.MustCompile(`^github:([a-z0-9_.-]+)/([a-z0-9_.-]+)#([1-9][0-9]*)$`)
	digestPattern    = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

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

type wireSource struct {
	Kind       string    `json:"kind"`
	BodySHA256 string    `json:"body_sha256"`
	Route      wireRoute `json:"route"`
}

type wireScope struct {
	Repository        string     `json:"repository"`
	CanonicalCheckout string     `json:"canonical_checkout"`
	Issue             int64      `json:"issue"`
	IntendedPaths     []string   `json:"intended_paths"`
	ResourceIntents   []string   `json:"resource_intents"`
	Source            wireSource `json:"source"`
}

type wireClaim struct {
	ClaimID            string    `json:"claim_id"`
	Executor           string    `json:"executor"`
	Project            string    `json:"project"`
	Owner              string    `json:"owner"`
	Ticket             string    `json:"ticket"`
	Resources          []string  `json:"resources"`
	ResourceIntents    []string  `json:"resource_intents"`
	RequiredDiskBytes  int64     `json:"required_disk_bytes"`
	PressureAccounting string    `json:"pressure_accounting"`
	CreatedAt          string    `json:"created_at"`
	CreatedEpoch       float64   `json:"created_epoch"`
	Scope              wireScope `json:"scope"`
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
	Claim       wireClaim `json:"claim"`
	IssueBody   string    `json:"issue_body"`
	AlreadyHeld bool      `json:"already_held"`
	Rebound     bool      `json:"rebound"`
}

type failureResponse struct {
	OK       bool   `json:"ok"`
	Command  string `json:"command"`
	Admitted bool   `json:"admitted"`
	Reason   string `json:"reason"`
	Error    string `json:"error"`
}

// AdmitWorker executes one bounded admission request. Any command, decoding,
// or identity uncertainty is returned as an error and must block worker effects.
func (c *Client) AdmitWorker(ctx context.Context, in ports.WorkerAdmissionRequest) (ports.WorkerAdmissionResult, error) {
	req, repo, issue, err := buildRequest(in)
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
		if denied.OK || denied.Admitted || denied.Command != commandName || strings.TrimSpace(denied.Reason) == "" || strings.TrimSpace(denied.Error) == "" {
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
	if err := validateSuccess(req, repo, issue, out); err != nil {
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
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return stdout.Bytes(), stderr.Bytes(), exitErr.ExitCode(), nil
	}
	return stdout.Bytes(), stderr.Bytes(), -1, err
}

func buildRequest(in ports.WorkerAdmissionRequest) (wireRequest, string, int64, error) {
	project := string(in.ProjectID)
	sessionID := string(in.SessionID)
	if strings.TrimSpace(project) == "" || strings.TrimSpace(project) != project {
		return wireRequest{}, "", 0, errors.New("worker admission: project identity is invalid")
	}
	if strings.TrimSpace(sessionID) == "" || strings.TrimSpace(sessionID) != sessionID {
		return wireRequest{}, "", 0, errors.New("worker admission: session identity is invalid")
	}
	repo, err := canonicalRepository(in.Repository)
	if err != nil {
		return wireRequest{}, "", 0, fmt.Errorf("worker admission: %w", err)
	}
	issueMatch := ticketPattern.FindStringSubmatch(string(in.IssueID))
	if issueMatch == nil || issueMatch[1]+"/"+issueMatch[2] != repo {
		return wireRequest{}, "", 0, errors.New("worker admission: ticket identity does not match repository")
	}
	issue, err := strconv.ParseInt(issueMatch[3], 10, 64)
	if err != nil || issue <= 0 {
		return wireRequest{}, "", 0, errors.New("worker admission: ticket number is invalid")
	}
	if err := in.Route.Validate(); err != nil {
		return wireRequest{}, "", 0, fmt.Errorf("worker admission: route: %w", err)
	}
	if in.Operation != ports.WorkerAdmissionSpawn && in.Operation != ports.WorkerAdmissionRestore {
		return wireRequest{}, "", 0, errors.New("worker admission: operation is invalid")
	}
	route := wireRoute{Harness: string(in.Route.Harness), Model: in.Route.Model, ReasoningEffort: string(in.Route.ReasoningEffort), Fallback: "none"}
	return wireRequest{Schema: requestSchema, Project: project, Repository: in.Repository, SessionID: sessionID, Ticket: string(in.IssueID), Operation: string(in.Operation), Route: route}, repo, issue, nil
}

func validateSuccess(req wireRequest, repo string, issue int64, out successResponse) error {
	if !out.OK || !out.Admitted || out.Command != commandName {
		return errors.New("success flags are inconsistent")
	}
	if out.Project != req.Project || out.Repository != repo || out.SessionID != req.SessionID || out.Ticket != req.Ticket || out.Operation != req.Operation || out.Route != req.Route {
		return errors.New("response identity does not match request")
	}
	if out.AlreadyHeld && out.Rebound {
		return errors.New("response cannot be both already-held and rebound")
	}
	claim := out.Claim
	if strings.TrimSpace(claim.ClaimID) == "" || claim.Executor != "ao" || claim.Project != req.Project || claim.Owner != req.SessionID || claim.Ticket != req.Ticket {
		return errors.New("claim identity does not match request")
	}
	if claim.Resources == nil || len(claim.Resources) != 0 || claim.ResourceIntents == nil || claim.RequiredDiskBytes < 0 || strings.TrimSpace(claim.PressureAccounting) == "" || math.IsNaN(claim.CreatedEpoch) || math.IsInf(claim.CreatedEpoch, 0) || claim.CreatedEpoch < 0 {
		return errors.New("claim accounting is invalid")
	}
	if _, err := time.Parse(time.RFC3339, claim.CreatedAt); err != nil {
		return errors.New("claim creation time is invalid")
	}
	scope := claim.Scope
	if scope.Repository != repo || scope.Issue != issue || scope.IntendedPaths == nil || scope.ResourceIntents == nil || !slices.Equal(claim.ResourceIntents, scope.ResourceIntents) {
		return errors.New("claim scope does not match request")
	}
	if !filepath.IsAbs(scope.CanonicalCheckout) || filepath.Clean(scope.CanonicalCheckout) != scope.CanonicalCheckout {
		return errors.New("claim checkout is not canonical and absolute")
	}
	if !strictlySorted(scope.IntendedPaths) || !strictlySorted(scope.ResourceIntents) {
		return errors.New("claim scope lists are not canonical")
	}
	if scope.Source.Kind != "issue" || !digestPattern.MatchString(scope.Source.BodySHA256) || scope.Source.Route != req.Route || out.IssueBody == "" {
		return errors.New("claim source does not match request")
	}
	return nil
}

func decodeExact(data []byte, out any) error {
	if err := rejectDuplicateKeys(data); err != nil {
		return err
	}
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

func rejectDuplicateKeys(data []byte) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	var walk func() error
	walk = func() error {
		tok, err := dec.Token()
		if err != nil {
			return err
		}
		delim, ok := tok.(json.Delim)
		if !ok {
			return nil
		}
		switch delim {
		case '{':
			seen := map[string]struct{}{}
			for dec.More() {
				keyToken, err := dec.Token()
				if err != nil {
					return err
				}
				key, ok := keyToken.(string)
				if !ok {
					return errors.New("object key is not a string")
				}
				if _, exists := seen[key]; exists {
					return fmt.Errorf("duplicate field %q", key)
				}
				seen[key] = struct{}{}
				if err := walk(); err != nil {
					return err
				}
			}
			_, err = dec.Token()
			return err
		case '[':
			for dec.More() {
				if err := walk(); err != nil {
					return err
				}
			}
			_, err = dec.Token()
			return err
		default:
			return errors.New("invalid JSON delimiter")
		}
	}
	return walk()
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

func strictlySorted(values []string) bool {
	for i, value := range values {
		if strings.TrimSpace(value) == "" || strings.TrimSpace(value) != value {
			return false
		}
		if i > 0 && values[i-1] >= value {
			return false
		}
	}
	return true
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
