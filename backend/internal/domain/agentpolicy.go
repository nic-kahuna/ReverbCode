package domain

// AgentPolicy is the daemon-wide availability policy for agent ids. It is
// intentionally separate from the supported harness vocabularies: disabling an
// agent prevents new processes from launching without making persisted sessions
// or project configuration unreadable.
type AgentPolicy struct {
	disabled map[string]struct{}
}

// NewAgentPolicy builds an immutable availability policy from agent ids.
func NewAgentPolicy(disabled []AgentHarness) AgentPolicy {
	set := make(map[string]struct{}, len(disabled))
	for _, harness := range disabled {
		set[string(harness)] = struct{}{}
	}
	return AgentPolicy{disabled: set}
}

// IsDisabled reports whether an agent id is disabled. Worker and reviewer
// harnesses share string ids where they refer to the same executable, so the
// policy applies to both vocabularies.
func (p AgentPolicy) IsDisabled(agentID string) bool {
	_, disabled := p.disabled[agentID]
	return disabled
}

// HasDisabledAgents reports whether the policy requires fail-closed startup
// proof. It lets reconciliation distinguish ordinary inventory failures from
// failures that prevent proving a disabled runtime is absent.
func (p AgentPolicy) HasDisabledAgents() bool {
	return len(p.disabled) > 0
}
