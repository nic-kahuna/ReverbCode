package apispec_test

import (
	"reflect"
	"sort"
	"strings"
	"testing"

	yaml "gopkg.in/yaml.v3"

	"github.com/aoagents/agent-orchestrator/backend/internal/httpd/apispec"
)

type contractSchema struct {
	Properties map[string]yaml.Node `yaml:"properties"`
	Required   []string             `yaml:"required"`
}

type contractOperation struct {
	Responses map[string]yaml.Node `yaml:"responses"`
}

type contractDocument struct {
	Paths      map[string]map[string]contractOperation `yaml:"paths"`
	Components struct {
		Schemas map[string]contractSchema `yaml:"schemas"`
	} `yaml:"components"`
}

func TestBootProofSchemasPreserveIdentityFields(t *testing.T) {
	doc := parseContractDocument(t)
	for schemaName, fields := range map[string][]string{
		"DaemonHealthResponse": {"executablePath", "workingDirectory", "dataDir", "build", "fence"},
		"DaemonReadyResponse":  {"executablePath", "workingDirectory", "dataDir", "build", "fence"},
		"RecoveryStatusResponse": {
			"executablePath", "workingDirectory", "dataDir", "build", "fence", "inventory",
		},
		"DaemonVersionResponse": {"executablePath", "build"},
	} {
		schema, ok := doc.Components.Schemas[schemaName]
		if !ok {
			t.Errorf("missing component schema %s", schemaName)
			continue
		}
		for _, field := range fields {
			if _, ok := schema.Properties[field]; !ok {
				t.Errorf("%s properties missing %q", schemaName, field)
			}
			if !contains(schema.Required, field) {
				t.Errorf("%s required missing %q", schemaName, field)
			}
		}
	}
}

func TestRecoveryOperationsDeclareStableResponses(t *testing.T) {
	doc := parseContractDocument(t)
	want := map[string][]string{
		"GET /healthz":                              {"200", "423"},
		"GET /readyz":                               {"200", "423"},
		"GET /version":                              {"200", "423"},
		"GET /api/v1/recovery":                      {"200", "423"},
		"GET /api/v1/recovery/projects":             {"200", "423", "503"},
		"GET /api/v1/recovery/projects/{projectId}": {"200", "404", "423", "503"},
		"GET /api/v1/recovery/projects/{projectId}/workspace-repos":            {"200", "423", "503"},
		"GET /api/v1/recovery/projects/{projectId}/workspace-repos/{repoName}": {"200", "404", "423", "503"},
		"GET /api/v1/recovery/sessions":                                        {"200", "423", "503"},
		"GET /api/v1/recovery/sessions/{sessionId}":                            {"200", "404", "423", "503"},
		"GET /api/v1/recovery/sessions/{sessionId}/worktrees":                  {"200", "423", "503"},
		"GET /api/v1/recovery/sessions/{sessionId}/worktrees/{repoName}":       {"200", "404", "423", "503"},
		"POST /api/v1/recovery/clear":                                          {"200", "400", "403", "409", "423", "500"},
	}
	for route, wantStatuses := range want {
		method, path := splitRoute(t, route)
		op, ok := doc.Paths[path][strings.ToLower(method)]
		if !ok {
			t.Errorf("missing operation %s", route)
			continue
		}
		got := make([]string, 0, len(op.Responses))
		for status := range op.Responses {
			got = append(got, status)
		}
		sort.Strings(got)
		sort.Strings(wantStatuses)
		if !reflect.DeepEqual(got, wantStatuses) {
			t.Errorf("%s responses = %v, want %v", route, got, wantStatuses)
		}
	}
}

func TestRecoveryClearRequestSchemaIsExactCASIdentity(t *testing.T) {
	doc := parseContractDocument(t)
	schema, ok := doc.Components.Schemas["RecoveryClearRequest"]
	if !ok {
		t.Fatal("missing RecoveryClearRequest component schema")
	}
	want := []string{"activationId", "generation", "payloadSha256", "protocolVersion"}
	properties := make([]string, 0, len(schema.Properties))
	for name := range schema.Properties {
		properties = append(properties, name)
	}
	sort.Strings(properties)
	sort.Strings(schema.Required)
	if !reflect.DeepEqual(properties, want) {
		t.Errorf("RecoveryClearRequest properties = %v, want exact %v", properties, want)
	}
	if !reflect.DeepEqual(schema.Required, want) {
		t.Errorf("RecoveryClearRequest required = %v, want exact %v", schema.Required, want)
	}
	if _, exists := doc.Paths["/api/v1/recovery/activate"]; exists {
		t.Error("recovery activate route must not be documented")
	}
}

func parseContractDocument(t *testing.T) contractDocument {
	t.Helper()
	var doc contractDocument
	if err := yaml.Unmarshal(apispec.Default().YAML(), &doc); err != nil {
		t.Fatalf("parse embedded OpenAPI contract: %v", err)
	}
	return doc
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func splitRoute(t *testing.T, route string) (method, path string) {
	t.Helper()
	for i, char := range route {
		if char == ' ' {
			return route[:i], route[i+1:]
		}
	}
	t.Fatalf("invalid route key %q", route)
	return "", ""
}
