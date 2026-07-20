package domain

import "testing"

func TestAgentRouteValidate(t *testing.T) {
	valid := AgentRoute{Harness: HarnessClaudeCode, Model: "claude-fable-5", ReasoningEffort: ReasoningEffortMedium}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid route rejected: %v", err)
	}
	for name, route := range map[string]AgentRoute{
		"missing harness": {Model: "m", ReasoningEffort: ReasoningEffortMedium},
		"unknown harness": {Harness: "bogus", Model: "m", ReasoningEffort: ReasoningEffortMedium},
		"missing model":   {Harness: HarnessCodex, ReasoningEffort: ReasoningEffortMedium},
		"missing effort":  {Harness: HarnessCodex, Model: "m"},
		"unknown effort":  {Harness: HarnessCodex, Model: "m", ReasoningEffort: "enormous"},
	} {
		t.Run(name, func(t *testing.T) {
			if err := route.Validate(); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}
