package controllers

import (
	"context"
	"errors"
	"net/http"

	"github.com/aoagents/agent-orchestrator/backend/internal/custody"
	"github.com/aoagents/agent-orchestrator/backend/internal/httpd/apispec"
	"github.com/aoagents/agent-orchestrator/backend/internal/httpd/envelope"
	"github.com/go-chi/chi/v5"
)

type CustodyService interface {
	ExportCertificateRef(context.Context, custody.CertificateRefRequest) (custody.CertificateExport, error)
	Handback(context.Context, custody.HandbackRequest) (custody.CustodyStatus, error)
	Request(context.Context, custody.CustodyRequest) (custody.CustodyStatus, error)
	Status(context.Context, custody.CustodyRequest) (custody.CustodyStatus, error)
	Checkpoint(context.Context, custody.CustodyRequest) (custody.CustodyStatus, error)
	Verify(context.Context, custody.CustodyRequest) (custody.CustodyStatus, error)
	Hook(context.Context, custody.HookRequest) (custody.HookResult, error)
	Evidence(context.Context, custody.EvidenceRequest) (custody.Evidence, error)
	ExportCertificate(context.Context, custody.CertificateRequest) (custody.CertificateExport, error)
}
type CustodyController struct{ Service CustodyService }

func (c *CustodyController) Register(r chi.Router) {
	r.Post("/admission/evidence", c.evidence)
	r.Post("/custody/certificate", c.certificate)
	r.Post("/custody/request", c.operation("request"))
	r.Post("/custody/status", c.operation("status"))
	r.Post("/custody/checkpoint", c.operation("checkpoint"))
	r.Post("/custody/verify", c.operation("verify"))
	r.Post("/custody/hook", c.hook)
	r.Post("/custody/handback", c.handback)
}
func (c *CustodyController) evidence(w http.ResponseWriter, r *http.Request) {
	if c.Service == nil {
		apispec.NotImplemented(w, r, http.MethodPost, "/api/v1/admission/evidence")
		return
	}
	var in custody.EvidenceRequest
	if err := decodeJSONStrict(r, &in); err != nil {
		envelope.WriteAPIError(w, r, 400, "bad_request", "INVALID_JSON", "Invalid evidence request", nil)
		return
	}
	out, err := c.Service.Evidence(r.Context(), in)
	if err != nil {
		writeCustodyError(w, r, err)
		return
	}
	envelope.WriteJSON(w, 200, out)
}
func (c *CustodyController) certificate(w http.ResponseWriter, r *http.Request) {
	if c.Service == nil {
		apispec.NotImplemented(w, r, http.MethodPost, "/api/v1/custody/certificate")
		return
	}
	var in custody.CertificateExportRequest
	if err := decodeJSONStrict(r, &in); err != nil {
		envelope.WriteAPIError(w, r, 400, "bad_request", "INVALID_JSON", "Invalid certificate request", nil)
		return
	}
	var out custody.CertificateExport
	var err error
	if in.Ref != nil {
		out, err = c.Service.ExportCertificateRef(r.Context(), *in.Ref)
	} else if in.Full != nil {
		out, err = c.Service.ExportCertificate(r.Context(), *in.Full)
	} else {
		err = custody.ErrUnknown
	}
	if err != nil {
		writeCustodyError(w, r, err)
		return
	}
	envelope.WriteJSON(w, 200, out)
}
func writeCustodyError(w http.ResponseWriter, r *http.Request, err error) {
	code := "AO_CUSTODY_UNKNOWN"
	for _, candidate := range []error{custody.ErrFenced, custody.ErrConflict, custody.ErrUnsupported, custody.ErrAdmission} {
		if errors.Is(err, candidate) {
			code = candidate.Error()
			break
		}
	}
	envelope.WriteAPIError(w, r, http.StatusConflict, "conflict", code, err.Error(), nil)
}

func (c *CustodyController) operation(name string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if c.Service == nil {
			apispec.NotImplemented(w, r, http.MethodPost, "/api/v1/custody/"+name)
			return
		}
		var in custody.CustodyRequest
		if err := decodeJSONStrict(r, &in); err != nil {
			envelope.WriteAPIError(w, r, 400, "bad_request", "INVALID_JSON", "Invalid custody request", nil)
			return
		}
		var out custody.CustodyStatus
		var err error
		switch name {
		case "request":
			out, err = c.Service.Request(r.Context(), in)
		case "status":
			out, err = c.Service.Status(r.Context(), in)
		case "checkpoint":
			out, err = c.Service.Checkpoint(r.Context(), in)
		case "verify":
			out, err = c.Service.Verify(r.Context(), in)
		}
		if err != nil {
			writeCustodyError(w, r, err)
			return
		}
		envelope.WriteJSON(w, 200, out)
	}
}
func (c *CustodyController) hook(w http.ResponseWriter, r *http.Request) {
	if c.Service == nil {
		apispec.NotImplemented(w, r, http.MethodPost, "/api/v1/custody/hook")
		return
	}
	var in custody.HookRequest
	if err := decodeJSONStrict(r, &in); err != nil {
		envelope.WriteAPIError(w, r, 400, "bad_request", "INVALID_JSON", "Invalid native hook", nil)
		return
	}
	out, err := c.Service.Hook(r.Context(), in)
	if err != nil {
		writeCustodyError(w, r, err)
		return
	}
	envelope.WriteJSON(w, 200, out)
}

func (c *CustodyController) handback(w http.ResponseWriter, r *http.Request) {
	if c.Service == nil {
		apispec.NotImplemented(w, r, http.MethodPost, "/api/v1/custody/handback")
		return
	}
	var in custody.HandbackRequest
	if err := decodeJSONStrict(r, &in); err != nil {
		envelope.WriteAPIError(w, r, 400, "bad_request", "INVALID_JSON", "Invalid native handback", nil)
		return
	}
	out, err := c.Service.Handback(r.Context(), in)
	if err != nil {
		writeCustodyError(w, r, err)
		return
	}
	envelope.WriteJSON(w, 200, out)
}
