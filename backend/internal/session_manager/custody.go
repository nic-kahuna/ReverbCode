package sessionmanager

import (
	"context"
	"fmt"
	"strings"

	"github.com/aoagents/agent-orchestrator/backend/internal/custody"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

func (m *Manager) managedProject(ctx context.Context, id string) (bool, error) {
	if store, ok := m.store.(interface {
		ManagedProject(context.Context, string) (bool, error)
	}); ok {
		return store.ManagedProject(ctx, id)
	}
	return false, nil
}
func (m *Manager) protectManaged(ctx context.Context, id domain.SessionID) error {
	rec, ok, err := m.store.GetSession(ctx, id)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	managed, err := m.managedProject(ctx, string(rec.ProjectID))
	if err != nil {
		return err
	}
	if managed {
		return custody.ErrFenced
	}
	return nil
}

// spawnManaged uses the same adapters and workspace, but all preparation is
// preceded by durable native reservation and every failure retains recovery debt.
// It never runs the generic spawn compensation which destroys partial work.
func (m *Manager) spawnManaged(ctx context.Context, cfg ports.SpawnConfig, project domain.ProjectRecord, rec domain.SessionRecord) (domain.SessionRecord, error) {
	if m.custody == nil {
		return rec, custody.ErrAdmission
	}
	a, ctx, err := m.custody.Begin(ctx, rec, "spawn")
	if err != nil {
		return rec, err
	}
	cfg.Prompt = a.IssueBody
	cfg.Route = a.Route
	cfg.Harness = a.Route.Harness
	if m.policy.IsDisabled(string(cfg.Harness)) {
		return rec, ErrAgentDisabled
	}
	agent, ok := m.agents.Agent(cfg.Harness)
	if !ok {
		return rec, ErrUnknownHarness
	}
	config := effectiveAgentConfig(cfg.Kind, project.Config, cfg.Harness, cfg.Route)
	if err = config.Validate(); err != nil {
		return rec, err
	}
	if validator, ok := agent.(ports.AgentConfigValidator); ok {
		if err = validator.ValidateAgentConfig(ctx, config); err != nil {
			return rec, err
		}
	}
	prompt, systemPrompt, err := m.buildSpawnTexts(ctx, cfg)
	if err != nil {
		return rec, err
	}
	branch := cfg.Branch
	if branch == "" {
		branch = defaultSpawnBranch(rec.ID, cfg.Kind, sessionPrefix(project), project.Kind.WithDefault())
	}
	var ws ports.WorkspaceInfo
	if a.Workspace != "" {
		ws = ports.WorkspaceInfo{SessionID: rec.ID, ProjectID: rec.ProjectID, Path: a.Workspace, Branch: a.WorkspaceBranch}
	}
	err = m.custody.Operation(ctx, string(rec.ID), "workspace", func() error {
		var createErr error
		ws, _, createErr = m.createSessionWorkspace(ctx, project, cfg, rec.ID, branch)
		if createErr != nil {
			return createErr
		}
		return m.custody.SetWorkspace(ctx, string(rec.ID), ws.Path)
	})
	if err != nil {
		return rec, err
	}
	rec.Harness = cfg.Harness
	rec.Metadata.Branch = ws.Branch
	rec.Metadata.WorkspacePath = ws.Path
	rec.Metadata.Prompt = prompt
	rec.Metadata.RequestedRoute = cfg.Route
	rec.Metadata.LaunchRoute = &domain.AgentLaunchRoute{Harness: cfg.Harness, Model: config.Model, ReasoningEffort: config.ReasoningEffort}
	if err = m.store.UpdateSession(ctx, rec); err != nil {
		return rec, err
	}
	if err = m.custody.Operation(ctx, string(rec.ID), "provision", func() error {
		if e := applySymlinks(project.Path, ws.Path, project.Config.Symlinks); e != nil {
			return e
		}
		for _, command := range project.Config.PostCreate {
			if strings.TrimSpace(command) == "" {
				continue
			}
			if _, _, e := custody.RunPreparedCommand(ctx, ws.Path, "/bin/sh", "-c", command); e != nil {
				return e
			}
		}
		return nil
	}); err != nil {
		return rec, err
	}
	if err = m.custody.Operation(ctx, string(rec.ID), "agent_prepare", func() error { return m.prepareWorkspace(ctx, agent, rec.ID, ws.Path, systemPrompt, config) }); err != nil {
		return rec, err
	}
	launch := ports.LaunchConfig{DataDir: m.dataDir, SessionID: string(rec.ID), WorkspacePath: ws.Path, Kind: cfg.Kind, Prompt: prompt, SystemPrompt: systemPrompt, IssueID: string(cfg.IssueID), Config: config, Permissions: config.Permissions}
	delivery, err := agent.GetPromptDeliveryStrategy(ctx, launch)
	if err != nil {
		return rec, err
	}
	if delivery == ports.PromptDeliveryAfterStart {
		launch.Prompt = ""
	}
	argv, err := agent.GetLaunchCommand(ctx, launch)
	if err != nil {
		return rec, err
	}
	if err = m.validateAgentBinary(argv); err != nil {
		return rec, err
	}
	runtimeCfg := ports.RuntimeConfig{SessionID: rec.ID, WorkspacePath: ws.Path, Argv: argv, Env: m.runtimeEnv(rec.ID, cfg.ProjectID, cfg.IssueID, project.Config.Env)}
	handle, err := m.runtime.HandleFor(runtimeCfg)
	if err != nil {
		return rec, err
	}
	accepted := a
	if a.Phase != "launching" {
		accepted, ctx, err = m.custody.CommitLaunch(ctx, string(rec.ID), handle)
		if err != nil {
			return rec, err
		}
	}
	if accepted.Route == nil || *accepted.Route != *a.Route {
		return rec, fmt.Errorf("%w: accepted route changed after preparation", custody.ErrAdmission)
	}
	// Pin the final accepted source, not the earlier tracker-list body.
	cfg.Prompt = accepted.IssueBody
	prompt, _, err = m.buildSpawnTexts(ctx, cfg)
	if err != nil {
		return rec, err
	}
	if delivery != ports.PromptDeliveryAfterStart {
		launch.Prompt = prompt
		runtimeCfg.Argv, err = agent.GetLaunchCommand(ctx, launch)
		if err != nil {
			return rec, err
		}
	}
	runtimeCfg.Env["AO_NATIVE_ATTEMPT_ID"] = accepted.AttemptID
	runtimeCfg.Env["AO_NATIVE_GENERATION"] = fmt.Sprint(accepted.Generation)
	actual, err := m.runtime.Create(ctx, runtimeCfg)
	if err != nil {
		return rec, err
	}
	if actual.ID != handle.ID {
		return rec, custody.ErrUnknown
	}
	metadata := domain.SessionMetadata{Branch: ws.Branch, WorkspacePath: ws.Path, RuntimeHandleID: handle.ID, Prompt: prompt}
	if err = m.lcm.MarkSpawned(ctx, rec.ID, metadata); err != nil {
		return rec, err
	}
	if delivery == ports.PromptDeliveryAfterStart && prompt != "" {
		if err = m.deliverAfterStartPrompt(ctx, agent, launch, handle, rec.ID, prompt); err != nil {
			return rec, err
		}
	}
	if err = m.custody.ObserveStarted(ctx, string(rec.ID), handle); err != nil {
		return rec, err
	}
	return m.getRecord(ctx, rec.ID)
}

func (m *Manager) managedSession(ctx context.Context, id domain.SessionID) (bool, error) {
	rec, ok, err := m.store.GetSession(ctx, id)
	if err != nil || !ok {
		return false, err
	}
	return m.managedProject(ctx, string(rec.ProjectID))
}

// launchPrepared is explicit first-provider launch in a retained, prepared
// candidate. It never creates/restores a workspace or fabricates a conversation.
func (m *Manager) launchPrepared(ctx context.Context, a custody.Attempt, message string) (custody.Attempt, error) {
	rec, ok, err := m.store.GetSession(ctx, domain.SessionID(a.SessionID))
	if err != nil {
		return a, err
	}
	if !ok {
		return a, ErrNotFound
	}
	project, ok, err := m.store.GetProject(ctx, a.Project)
	if err != nil {
		return a, err
	}
	if !ok || a.Route == nil || a.Workspace == "" || a.ProviderID != "" {
		return a, custody.ErrUnknown
	}
	cfg := ports.SpawnConfig{ProjectID: rec.ProjectID, IssueID: rec.IssueID, Kind: rec.Kind, Harness: a.Route.Harness, Route: a.Route, Prompt: a.IssueBody, Branch: rec.Metadata.Branch}
	if m.policy.IsDisabled(string(cfg.Harness)) {
		return a, ErrAgentDisabled
	}
	agent, ok := m.agents.Agent(cfg.Harness)
	if !ok {
		return a, ErrUnknownHarness
	}
	config := effectiveAgentConfig(cfg.Kind, project.Config, cfg.Harness, cfg.Route)
	if err = config.Validate(); err != nil {
		return a, err
	}
	prompt, system, err := m.buildSpawnTexts(ctx, cfg)
	if err != nil {
		return a, err
	}
	if message != "" {
		prompt += "\n\nForeground continuation:\n" + message
	}
	if err = m.custody.Operation(ctx, a.SessionID, "agent_prepare", func() error { return m.prepareWorkspace(ctx, agent, rec.ID, a.Workspace, system, config) }); err != nil {
		return a, err
	}
	launch := ports.LaunchConfig{DataDir: m.dataDir, SessionID: a.SessionID, WorkspacePath: a.Workspace, Kind: rec.Kind, Prompt: prompt, SystemPrompt: system, IssueID: string(rec.IssueID), Config: config, Permissions: config.Permissions}
	argv, err := agent.GetLaunchCommand(ctx, launch)
	if err != nil {
		return a, err
	}
	if err = m.validateAgentBinary(argv); err != nil {
		return a, err
	}
	runtimeCfg := ports.RuntimeConfig{SessionID: rec.ID, WorkspacePath: a.Workspace, Argv: argv, Env: m.runtimeEnv(rec.ID, rec.ProjectID, rec.IssueID, project.Config.Env)}
	h, err := m.runtime.HandleFor(runtimeCfg)
	if err != nil {
		return a, err
	}
	accepted, admitted, err := m.custody.CommitLaunch(ctx, a.SessionID, h)
	if err != nil {
		return a, err
	}
	if accepted.Route == nil || *accepted.Route != *a.Route {
		return accepted, custody.ErrAdmission
	}
	cfg.Prompt = accepted.IssueBody
	launch.Prompt, _, err = m.buildSpawnTexts(ctx, cfg)
	if err != nil {
		return accepted, err
	}
	if message != "" {
		launch.Prompt += "\n\nForeground continuation:\n" + message
	}
	runtimeCfg.Argv, err = agent.GetLaunchCommand(ctx, launch)
	if err != nil {
		return accepted, err
	}
	runtimeCfg.Env["AO_NATIVE_ATTEMPT_ID"] = accepted.AttemptID
	runtimeCfg.Env["AO_NATIVE_GENERATION"] = fmt.Sprint(accepted.Generation)
	actual, err := m.runtime.Create(admitted, runtimeCfg)
	if err != nil {
		return accepted, err
	}
	if actual.ID != h.ID {
		return accepted, custody.ErrUnknown
	}
	if err = m.lcm.MarkSpawned(admitted, rec.ID, domain.SessionMetadata{Branch: rec.Metadata.Branch, WorkspacePath: a.Workspace, RuntimeHandleID: h.ID, Prompt: launch.Prompt}); err != nil {
		return accepted, err
	}
	if err = m.custody.ObserveStarted(ctx, a.SessionID, h); err != nil {
		return accepted, err
	}
	final, _, err := m.custody.Store.CurrentAttempt(ctx, a.SessionID)
	return final, err
}

func (m *Manager) RetryManagedIntake(ctx context.Context, id domain.SessionID) (bool, error) {
	if m.custody == nil {
		return false, nil
	}
	return m.custody.Retryable(ctx, string(id))
}
