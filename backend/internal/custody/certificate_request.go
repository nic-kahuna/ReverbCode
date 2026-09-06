package custody

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
)

type CertificateRefRequest struct {
	Schema            string `json:"schema" enum:"ao-custody-certificate-ref/v1"`
	Project           string `json:"project"`
	SessionID         string `json:"session_id"`
	AttemptID         string `json:"attempt_id"`
	CertificateID     string `json:"certificate_id"`
	CertificateSHA256 string `json:"certificate_sha256"`
}

// CertificateExportRequest is a strict union, never a partially populated
// full-identity request. Ref lookup derives its immutable identity server-side.
type CertificateExportRequest struct {
	Full *CertificateRequest    `json:"-"`
	Ref  *CertificateRefRequest `json:"-"`
}

func (CertificateExportRequest) JSONSchemaOneOf() []interface{} {
	return []interface{}{CertificateRequest{}, CertificateRefRequest{}}
}
func (q *CertificateExportRequest) UnmarshalJSON(b []byte) error {
	if err := checkpointUniqueJSON(b); err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(b, &fields); err != nil {
		return err
	}
	if fields == nil {
		return ErrUnknown
	}
	decoder := json.NewDecoder(bytes.NewReader(b))
	decoder.DisallowUnknownFields()
	if _, ref := fields["schema"]; ref {
		var in CertificateRefRequest
		if err := decoder.Decode(&in); err != nil {
			return err
		}
		if len(fields) != 6 || in.Schema != "ao-custody-certificate-ref/v1" || in.Project == "" || in.SessionID == "" || in.AttemptID == "" || in.CertificateID == "" || in.CertificateSHA256 == "" {
			return ErrUnknown
		}
		q.Ref = &in
		q.Full = nil
		return nil
	}
	var in CertificateRequest
	if err := decoder.Decode(&in); err != nil {
		return err
	}
	if len(fields) != 6 || in.Project == "" || in.SessionID == "" || in.AttemptID == "" || in.Generation < 1 || in.RequestID == "" || in.CertificateID == "" {
		return ErrUnknown
	}
	q.Full = &in
	q.Ref = nil
	return nil
}
func (c *Coordinator) ExportCertificateRef(ctx context.Context, q CertificateRefRequest) (CertificateExport, error) {
	var out CertificateExport
	if q.Schema != "ao-custody-certificate-ref/v1" {
		return out, ErrUnknown
	}
	a, ok, err := c.Store.GetAttempt(ctx, q.SessionID, q.AttemptID)
	if err != nil {
		return out, err
	}
	if !ok || a.Project != q.Project || a.Certificate == nil || a.Certificate.ID != q.CertificateID || a.Certificate.SHA256 != q.CertificateSHA256 {
		return out, ErrConflict
	}
	b, _, err := c.certificateBytes(a)
	if err != nil {
		return out, err
	}
	return CertificateExport{Schema: "ao-custody-certificate-export/v1", CertificateID: a.Certificate.ID, CertificateSHA256: a.Certificate.SHA256, PayloadBase64: base64.StdEncoding.EncodeToString(b)}, nil
}
