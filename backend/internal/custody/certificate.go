package custody

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
)

var certificateIDPattern = regexp.MustCompile(`^[a-f0-9]{32}$`)

type CertificateRequest struct {
	Project       string `json:"project"`
	SessionID     string `json:"session_id"`
	AttemptID     string `json:"attempt_id"`
	Generation    int64  `json:"generation"`
	RequestID     string `json:"request_id"`
	CertificateID string `json:"certificate_id"`
}
type CertificateExport struct {
	Schema            string `json:"schema"`
	CertificateID     string `json:"certificate_id"`
	CertificateSHA256 string `json:"certificate_sha256"`
	PayloadBase64     string `json:"payload_base64"`
}
type PreservationPayload struct {
	ExecutionOrigin        string              `json:"execution_origin"`
	Schema                 string              `json:"schema"`
	Project                string              `json:"project"`
	SessionID              string              `json:"session_id"`
	AttemptID              string              `json:"attempt_id"`
	Generation             int64               `json:"generation"`
	RequestID              string              `json:"request_id"`
	ForegroundID           string              `json:"foreground_id"`
	Candidate              Candidate           `json:"candidate"`
	ProviderID             string              `json:"provider_id"`
	Route                  json.RawMessage     `json:"route"`
	Transcript             string              `json:"transcript"`
	TurnID                 string              `json:"turn_id"`
	CompletedContextSHA256 string              `json:"completed_context_sha256"`
	Members                []ProcessIdentity   `json:"members"`
	Children               []ChildPreservation `json:"children"`
}

func hashBytes(b []byte) string { sum := sha256.Sum256(b); return hex.EncodeToString(sum[:]) }

func (c *Coordinator) certificateBytes(a Attempt) ([]byte, PreservationPayload, error) {
	var payload PreservationPayload
	if a.Certificate == nil || !certificateIDPattern.MatchString(a.Certificate.ID) {
		return nil, payload, ErrUnknown
	}
	path := filepath.Join(c.DataDir, "custody", "certificates", a.Certificate.ID+".json")
	info, err := os.Lstat(path)
	if err != nil {
		return nil, payload, err
	}
	if !info.Mode().IsRegular() || info.Size() > maxCandidateCapture {
		return nil, payload, ErrUnknown
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, payload, err
	}
	if hashBytes(b) != a.Certificate.SHA256 {
		return nil, payload, fmt.Errorf("%w: certificate bytes changed", ErrUnknown)
	}
	if err = json.Unmarshal(b, &payload); err != nil {
		return nil, payload, err
	}
	if payload.Schema != "ao-custody-preservation/v1" || payload.Project != a.Project || payload.SessionID != a.SessionID || payload.AttemptID != a.AttemptID || payload.Generation != a.Generation || payload.RequestID != a.RequestID || payload.ForegroundID != a.ForegroundID || payload.ProviderID != a.ProviderID || payload.TurnID != a.TurnID {
		return nil, payload, ErrConflict
	}
	return b, payload, nil
}

// VerifyStoredCertificate never upgrades missing process evidence to quiescence.
func (c *Coordinator) VerifyStoredCertificate(ctx context.Context, a Attempt) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if a.Retired || a.Phase != "quiesced" || a.Fence == "" || a.Certificate == nil || len(a.Operations) != 0 {
		return ErrUnknown
	}
	_, p, err := c.certificateBytes(a)
	if err != nil {
		return err
	}
	if err = c.verifyChildren(ctx, a); err != nil {
		return err
	}
	if !sameChildPreservation(p.Children, childPreservations(a)) {
		return ErrConflict
	}
	if p.ExecutionOrigin == "preparing_without_runtime" {
		if a.RuntimeHandleID != nil || p.ProviderID != "" || p.Transcript != "" || p.TurnID != "" || p.CompletedContextSHA256 != "" || len(p.Members) != 0 || len(p.Children) != 0 {
			return ErrUnknown
		}
		for _, member := range a.Preparations {
			gone, e := ProcessGone(member)
			if e != nil {
				return e
			}
			if !gone {
				return ErrUnknown
			}
		}
		return nil
	}
	if p.ExecutionOrigin != "retained_provider" || len(p.Members) < 2 {
		return ErrUnknown
	}
	actualTree, err := c.runtimeTree(ctx, a)
	if err != nil {
		return err
	}
	if err = sameMembers(p.Members, actualTree, true); err != nil {
		return err
	}
	for _, member := range p.Members {
		actual, err := ObserveProcess(member.PID)
		if err != nil {
			return err
		}
		if !actual.Same(member) {
			return ErrConflict
		}
		if member.Role != "pane_parent" && actual.Status != 4 {
			return fmt.Errorf("%w: writer is not stopped", ErrUnknown)
		}
	}
	return nil
}

func (c *Coordinator) ExportCertificate(ctx context.Context, q CertificateRequest) (CertificateExport, error) {
	var out CertificateExport
	a, ok, err := c.Store.GetAttempt(ctx, q.SessionID, q.AttemptID)
	if err != nil {
		return out, err
	}
	if !ok || a.Project != q.Project || a.Generation != q.Generation || a.RequestID != q.RequestID || a.Certificate == nil || a.Certificate.ID != q.CertificateID {
		return out, ErrConflict
	}
	b, _, err := c.certificateBytes(a)
	if err != nil {
		return out, err
	}
	return CertificateExport{Schema: "ao-custody-certificate-export/v1", CertificateID: a.Certificate.ID, CertificateSHA256: a.Certificate.SHA256, PayloadBase64: base64.StdEncoding.EncodeToString(b)}, nil
}

func (c *Coordinator) writeCertificate(a Attempt, payload PreservationPayload) (Certificate, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return Certificate{}, err
	}
	data = append(data, '\n')
	if len(data) > maxCandidateCapture {
		return Certificate{}, fmt.Errorf("%w: bounded preservation capture exceeds %d bytes", ErrUnknown, maxCandidateCapture)
	}
	id, err := identifier()
	if err != nil {
		return Certificate{}, err
	}
	dir := filepath.Join(c.DataDir, "custody", "certificates")
	if err = os.MkdirAll(dir, 0o700); err != nil {
		return Certificate{}, err
	}
	if err = preservationSpace(dir, int64(len(data))+(1<<20)); err != nil {
		return Certificate{}, err
	}
	path := filepath.Join(dir, id+".json")
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return Certificate{}, err
	}
	defer func() { _ = f.Close() }()
	if _, err = f.Write(data); err != nil {
		return Certificate{}, err
	}
	if err = f.Sync(); err != nil {
		return Certificate{}, err
	}
	if err = f.Close(); err != nil {
		return Certificate{}, err
	}
	directory, err := os.Open(dir)
	if err != nil {
		return Certificate{}, err
	}
	defer func() { _ = directory.Close() }()
	if err = directory.Sync(); err != nil {
		return Certificate{}, err
	}
	return Certificate{ID: id, SHA256: hashBytes(data), PreservationComplete: true, WriterStopped: true}, nil
}

type ChildPreservation struct {
	Operation              string            `json:"operation"`
	HandleID               string            `json:"handle_id"`
	ProviderID             string            `json:"provider_id"`
	Transcript             string            `json:"transcript"`
	TurnID                 string            `json:"turn_id"`
	CompletedContextSHA256 string            `json:"completed_context_sha256"`
	Members                []ProcessIdentity `json:"members"`
}

func childPreservations(a Attempt) []ChildPreservation {
	out := []ChildPreservation{}
	for _, ch := range a.Children {
		out = append(out, ChildPreservation{ch.Operation, ch.HandleID, ch.ProviderID, ch.Transcript, ch.TurnID, ch.CompletedContextSHA256, ch.Members})
	}
	return out
}
func sameChildPreservation(a, b []ChildPreservation) bool {
	x, _ := json.Marshal(a)
	y, _ := json.Marshal(b)
	return string(x) == string(y)
}
