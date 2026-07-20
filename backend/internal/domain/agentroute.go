package domain

import (
	"fmt"
	"strings"
)

// AgentRoute is a complete per-session launch selection. A non-nil route is
// explicit: AO must not fill a missing field from project or global defaults.
type AgentRoute struct {
	Harness         AgentHarness    `json:"harness"`
	Model           string          `json:"model"`
	ReasoningEffort ReasoningEffort `json:"reasoningEffort"`
}

// AgentLaunchRoute records the provider arguments AO resolved for a launch.
// Empty model or reasoning fields mean AO intentionally left that choice to
// the provider default; unlike AgentRoute, this is an observation rather than
// a complete explicit request.
type AgentLaunchRoute struct {
	Harness         AgentHarness    `json:"harness"`
	Model           string          `json:"model,omitempty"`
	ReasoningEffort ReasoningEffort `json:"reasoningEffort,omitempty"`
}

// Validate requires the complete route contract used by explicit spawn and
// native tracker intake.
func (r AgentRoute) Validate() error {
	if r.Harness == "" {
		return fmt.Errorf("route harness is required")
	}
	if !r.Harness.IsKnown() {
		return fmt.Errorf("route harness %q is unknown", r.Harness)
	}
	if strings.TrimSpace(r.Model) == "" {
		return fmt.Errorf("route model is required")
	}
	if strings.TrimSpace(r.Model) != r.Model {
		return fmt.Errorf("route model must not have leading or trailing whitespace")
	}
	if r.ReasoningEffort == "" {
		return fmt.Errorf("route reasoning effort is required")
	}
	if err := r.ReasoningEffort.Validate(); err != nil {
		return fmt.Errorf("route: %w", err)
	}
	return nil
}

// AgentConfig returns the provider configuration carried by the route.
func (r AgentRoute) AgentConfig() AgentConfig {
	return AgentConfig{Model: r.Model, ReasoningEffort: r.ReasoningEffort}
}
