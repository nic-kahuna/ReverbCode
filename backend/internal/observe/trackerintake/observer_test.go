package trackerintake

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

const reflex323DispatchRouteBody = `## Outcome

Keep the workbook mapping durable.

## Dispatch route

- harness: claude-code
- model: claude-fable-5
- reasoning-effort: high
- fallback: none

## Routing rationale

- vehicle: single worker`

func TestPollSpawnsWorkerForEligibleIssue(t *testing.T) {
	store := &fakeStore{
		projects: []domain.ProjectRecord{{
			ID:            "demo",
			RepoOriginURL: "https://github.com/acme/demo.git",
			Config: domain.ProjectConfig{TrackerIntake: domain.TrackerIntakeConfig{
				Enabled:  true,
				Assignee: "alice",
			}},
		}},
	}
	tracker := &fakeTracker{issues: []domain.Issue{{
		ID:        domain.TrackerID{Provider: domain.TrackerProviderGitHub, Native: "acme/demo#12"},
		Title:     "Fix login",
		Body:      "The login form submits twice.",
		State:     domain.IssueOpen,
		URL:       "https://github.com/acme/demo/issues/12",
		Labels:    []string{"agent-ready"},
		Assignees: []string{"alice"},
	}}}
	spawner := &fakeSpawner{}

	if err := New(singleResolver(tracker), store, spawner, Config{Logger: discardLogger()}).Poll(context.Background()); err != nil {
		t.Fatalf("Poll() error = %v", err)
	}
	if len(spawner.calls) != 1 {
		t.Fatalf("spawn calls = %d, want 1", len(spawner.calls))
	}
	call := spawner.calls[0]
	if call.ProjectID != "demo" || call.Kind != domain.KindWorker {
		t.Fatalf("spawn config = %+v", call)
	}
	if call.Route != nil {
		t.Fatalf("unrouted issue unexpectedly produced route: %#v", call.Route)
	}
	if call.IssueID != "github:acme/demo#12" {
		t.Fatalf("IssueID = %q, want canonical github id", call.IssueID)
	}
	if !strings.Contains(call.Prompt, "Fix login") || !strings.Contains(call.Prompt, "The login form submits twice.") {
		t.Fatalf("prompt missing issue context:\n%s", call.Prompt)
	}
	if len(tracker.filters) != 1 {
		t.Fatalf("tracker filters = %d, want 1", len(tracker.filters))
	}
	if got := tracker.filters[0]; got.State != domain.ListOpen || got.Assignee != "alice" || len(got.Labels) != 0 {
		t.Fatalf("tracker filter = %+v", got)
	}
}

func TestPollPassesCanonicalDispatchRoute(t *testing.T) {
	store := &fakeStore{projects: []domain.ProjectRecord{{
		ID: "demo", RepoOriginURL: "https://github.com/acme/demo.git",
		Config: domain.ProjectConfig{TrackerIntake: domain.TrackerIntakeConfig{Enabled: true, Assignee: "alice"}},
	}}}
	tracker := &fakeTracker{issues: []domain.Issue{{
		ID:    domain.TrackerID{Provider: domain.TrackerProviderGitHub, Native: "acme/demo#12"},
		Title: "Fix login", State: domain.IssueOpen, Assignees: []string{"alice"},
		Body: "Dispatch route\n- harness: claude-code\n- model: claude-fable-5\n- reasoning-effort: medium\n- fallback: none\n\nImplement it.",
	}}}
	spawner := &fakeSpawner{}

	if err := New(singleResolver(tracker), store, spawner, Config{Logger: discardLogger()}).Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(spawner.calls) != 1 || spawner.calls[0].Route == nil {
		t.Fatalf("spawn calls = %#v, want one routed spawn", spawner.calls)
	}
	route := spawner.calls[0].Route
	if route.Harness != domain.HarnessClaudeCode || route.Model != "claude-fable-5" || route.ReasoningEffort != domain.ReasoningEffortMedium {
		t.Fatalf("route = %#v", route)
	}
}

func TestPollPassesMarkdownDispatchRouteFromReflex323(t *testing.T) {
	store := &fakeStore{projects: []domain.ProjectRecord{{
		ID: "demo", RepoOriginURL: "https://github.com/acme/demo.git",
		Config: domain.ProjectConfig{TrackerIntake: domain.TrackerIntakeConfig{Enabled: true, Assignee: "alice"}},
	}}}
	tracker := &fakeTracker{issues: []domain.Issue{{
		ID:    domain.TrackerID{Provider: domain.TrackerProviderGitHub, Native: "acme/demo#323"},
		Title: "Keep mappings durable", State: domain.IssueOpen, Assignees: []string{"alice"},
		Body: reflex323DispatchRouteBody,
	}}}
	spawner := &fakeSpawner{}

	if err := New(singleResolver(tracker), store, spawner, Config{Logger: discardLogger()}).Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(spawner.calls) != 1 || spawner.calls[0].Route == nil {
		t.Fatalf("spawn calls = %#v, want one routed spawn", spawner.calls)
	}
	route := spawner.calls[0].Route
	if route.Harness != domain.HarnessClaudeCode || route.Model != "claude-fable-5" || route.ReasoningEffort != domain.ReasoningEffortHigh {
		t.Fatalf("route = %#v, want claude-code/claude-fable-5/high", route)
	}
}

func TestPollRejectsMalformedDispatchRouteWithoutSpawning(t *testing.T) {
	store := &fakeStore{projects: []domain.ProjectRecord{{
		ID: "demo", RepoOriginURL: "https://github.com/acme/demo.git",
		Config: domain.ProjectConfig{TrackerIntake: domain.TrackerIntakeConfig{Enabled: true, Assignee: "alice"}},
	}}}
	tracker := &fakeTracker{issues: []domain.Issue{{
		ID:    domain.TrackerID{Provider: domain.TrackerProviderGitHub, Native: "acme/demo#12"},
		State: domain.IssueOpen, Assignees: []string{"alice"},
		Body: "## Dispatch route\n\n- harness: claude-code\n- model: claude-fable-5\n- fallback: none",
	}}}
	spawner := &fakeSpawner{}
	if err := New(singleResolver(tracker), store, spawner, Config{Logger: discardLogger()}).Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(spawner.calls) != 0 {
		t.Fatalf("malformed route spawned: %#v", spawner.calls)
	}
}

func TestPollRejectsMalformedMarkdownHeaderWithoutSpawning(t *testing.T) {
	store := &fakeStore{projects: []domain.ProjectRecord{{
		ID: "demo", RepoOriginURL: "https://github.com/acme/demo.git",
		Config: domain.ProjectConfig{TrackerIntake: domain.TrackerIntakeConfig{Enabled: true, Assignee: "alice"}},
	}}}
	tracker := &fakeTracker{issues: []domain.Issue{{
		ID:    domain.TrackerID{Provider: domain.TrackerProviderGitHub, Native: "acme/demo#12"},
		State: domain.IssueOpen, Assignees: []string{"alice"},
		Body: "##Dispatch route\n\n- harness: codex\n- model: gpt-5.6-sol\n- reasoning-effort: ultra\n- fallback: none",
	}}}
	spawner := &fakeSpawner{}

	if err := New(singleResolver(tracker), store, spawner, Config{Logger: discardLogger()}).Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(spawner.calls) != 0 {
		t.Fatalf("malformed route header spawned: %#v", spawner.calls)
	}
}

func TestPollRejectsVisiblyRouteLookingMalformedHeadersWithoutSpawning(t *testing.T) {
	for name, body := range map[string]string{
		"bold ATX content":          "## **Dispatch route**\n\n- harness: codex\n- model: gpt-5.6-sol\n- reasoning-effort: ultra\n- fallback: none",
		"code ATX content":          "## `Dispatch route`\n\n- harness: codex\n- model: gpt-5.6-sol\n- reasoning-effort: ultra\n- fallback: none",
		"GFM strikethrough":         "## ~~Dispatch route~~\n\n- harness: codex\n- model: gpt-5.6-sol\n- reasoning-effort: ultra\n- fallback: none",
		"NBSP after closing hashes": "## Dispatch route ##\u00a0\n\n- harness: codex\n- model: gpt-5.6-sol\n- reasoning-effort: ultra\n- fallback: none",
		"collapsed ATX spaces":      "## Dispatch  route\n\n- harness: codex\n- model: gpt-5.6-sol\n- reasoning-effort: ultra\n- fallback: none",
		"multiline prose block":     "For example, a ticket can declare\nDispatch route\n- harness: codex\n- model: gpt-5.6-sol\n- reasoning-effort: ultra\n- fallback: none",
	} {
		t.Run(name, func(t *testing.T) {
			store := &fakeStore{projects: []domain.ProjectRecord{{
				ID: "demo", RepoOriginURL: "https://github.com/acme/demo.git",
				Config: domain.ProjectConfig{TrackerIntake: domain.TrackerIntakeConfig{Enabled: true, Assignee: "alice"}},
			}}}
			tracker := &fakeTracker{issues: []domain.Issue{{
				ID: domain.TrackerID{Provider: domain.TrackerProviderGitHub, Native: "acme/demo#12"}, State: domain.IssueOpen,
				Assignees: []string{"alice"}, Body: body,
			}}}
			spawner := &fakeSpawner{}

			if err := New(singleResolver(tracker), store, spawner, Config{Logger: discardLogger()}).Poll(context.Background()); err != nil {
				t.Fatal(err)
			}
			if len(spawner.calls) != 0 {
				t.Fatalf("visibly malformed route header spawned: %#v", spawner.calls)
			}
		})
	}
}

func TestParseDispatchRouteSupportedHeaders(t *testing.T) {
	cases := []struct {
		name   string
		header string
	}{
		{name: "plain", header: "Dispatch route"},
		{name: "heading level 1", header: "# Dispatch route"},
		{name: "heading level 2", header: "## Dispatch route"},
		{name: "heading level 3", header: "### Dispatch route"},
		{name: "heading level 4", header: "#### Dispatch route"},
		{name: "heading level 5", header: "##### Dispatch route"},
		{name: "heading level 6", header: "###### Dispatch route"},
		{name: "heading with closing sequence", header: "## Dispatch route ##"},
		{name: "heading with leading spaces", header: "   ## Dispatch route"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body := tc.header + "\n\n- harness: codex\n- model: gpt-5.6-sol\n- reasoning-effort: ultra\n- fallback: none"
			route, err := ParseDispatchRoute(body)
			if err != nil {
				t.Fatal(err)
			}
			want := domain.AgentRoute{Harness: domain.HarnessCodex, Model: "gpt-5.6-sol", ReasoningEffort: domain.ReasoningEffortUltra}
			if route == nil || *route != want {
				t.Fatalf("route = %#v, want %#v", route, want)
			}
		})
	}
}

func TestParseDispatchRouteRejectsMalformedRouteLookingHeaders(t *testing.T) {
	for name, header := range map[string]string{
		"missing heading whitespace": "##Dispatch route",
		"heading level 7":            "####### Dispatch route",
		"extra heading text":         "## Dispatch route details",
		"case drift":                 "## dispatch route",
		"non-Markdown leading space": "\u00a0## Dispatch route",
		"bold heading content":       "## **Dispatch route**",
		"code heading content":       "## `Dispatch route`",
		"linked heading content":     "## [Dispatch route](https://example.com)",
		"inline HTML content":        "## <em>Dispatch route</em>",
		"GFM strikethrough content":  "## ~~Dispatch route~~",
		"entity heading content":     "## Dispatch&#32;route",
		"collapsed heading spaces":   "## Dispatch  route",
		"NBSP after closing hashes":  "## Dispatch route ##\u00a0",
		"plain trailing colon":       "Dispatch route:",
		"plain closing hashes":       "Dispatch route ##",
		"plain extra text":           "Dispatch route details",
		"plain case drift":           "dispatch route",
		"emphasized plain content":   "Dispatch *route*",
		"soft break plain content":   "Dispatch\nroute",
		"setext heading":             "Dispatch route\n--------------",
	} {
		t.Run(name, func(t *testing.T) {
			body := header + "\n\n- harness: codex\n- model: gpt-5.6-sol\n- reasoning-effort: ultra\n- fallback: none"
			if route, err := ParseDispatchRoute(body); err == nil || route != nil {
				t.Fatalf("route=%#v err=%v, want malformed header rejection", route, err)
			}
		})
	}
}

func TestParseDispatchRouteRejectsRouteLookingLineInsideMultilineSetextHeading(t *testing.T) {
	body := "intro\nDispatch route:\n---\n\n- harness: codex\n- model: gpt-5.6-sol\n- reasoning-effort: ultra\n- fallback: none"
	if route, err := ParseDispatchRoute(body); err == nil || route != nil {
		t.Fatalf("route=%#v err=%v, want malformed setext rejection", route, err)
	}
}

func TestParseDispatchRouteIgnoresUnrelatedNarrativeMentions(t *testing.T) {
	for name, body := range map[string]string{
		"sentence":             "Explain why the Dispatch route parser is strict.",
		"unrelated heading":    "## Why Dispatch route parsing must fail closed",
		"heading router token": "## Dispatch router design",
		"heading routes token": "## Dispatch routes overview",
		"plain router token":   "Dispatch router internals",
		"plain routes token":   "Dispatch routes overview",
		"struck router token":  "## ~~Dispatch router~~",
	} {
		t.Run(name, func(t *testing.T) {
			route, err := ParseDispatchRoute(body)
			if err != nil || route != nil {
				t.Fatalf("route=%#v err=%v, want genuinely absent route", route, err)
			}
		})
	}
}

func TestParseDispatchRouteIgnoresMarkdownCodeExamples(t *testing.T) {
	for name, body := range map[string]string{
		"backtick fence":          "```markdown\n## Dispatch route\n\n- harness: claude-code\n- model: example\n- reasoning-effort: high\n- fallback: none\n```",
		"unclosed backtick fence": "```markdown\n## Dispatch route\n\n- harness: claude-code\n- model: example\n- reasoning-effort: high\n- fallback: none",
		"tilde fence":             "~~~text\nDispatch route\n- harness: claude-code\n- model: example\n- reasoning-effort: high\n- fallback: none\n~~~",
		"indented code":           "    ## Dispatch route\n\n    - harness: claude-code\n    - model: example\n    - reasoning-effort: high\n    - fallback: none",
		"block quote":             "> ## Dispatch route\n>\n> - harness: claude-code\n> - model: example\n> - reasoning-effort: high\n> - fallback: none",
		"list item":               "- Dispatch route\n  - harness: claude-code\n  - model: example\n  - reasoning-effort: high\n  - fallback: none",
	} {
		t.Run(name, func(t *testing.T) {
			route, err := ParseDispatchRoute(body)
			if err != nil || route != nil {
				t.Fatalf("route=%#v err=%v, want code example ignored", route, err)
			}
		})
	}
}

func TestParseDispatchRouteIgnoresFencedExampleBeforeCanonicalBlock(t *testing.T) {
	body := "```markdown\n## Dispatch route\n\n- harness: claude-code\n- model: example\n- reasoning-effort: high\n- fallback: none\n```\n\nDispatch route\n- harness: codex\n- model: gpt-5.6-sol\n- reasoning-effort: ultra\n- fallback: none"
	route, err := ParseDispatchRoute(body)
	if err != nil {
		t.Fatal(err)
	}
	want := domain.AgentRoute{Harness: domain.HarnessCodex, Model: "gpt-5.6-sol", ReasoningEffort: domain.ReasoningEffortUltra}
	if route == nil || *route != want {
		t.Fatalf("route = %#v, want %#v", route, want)
	}
}

func TestParseDispatchRouteDoesNotHideVisibleRouteAfterInvalidBacktickFence(t *testing.T) {
	body := "```demo`bad\n## Dispatch route\n\n- harness: codex\n- model: gpt-5.6-sol\n- reasoning-effort: ultra\n- fallback: none\n```"
	route, err := ParseDispatchRoute(body)
	if err != nil {
		t.Fatal(err)
	}
	want := domain.AgentRoute{Harness: domain.HarnessCodex, Model: "gpt-5.6-sol", ReasoningEffort: domain.ReasoningEffortUltra}
	if route == nil || *route != want {
		t.Fatalf("route = %#v, want %#v", route, want)
	}
}

func TestParseDispatchRouteStrictness(t *testing.T) {
	for name, body := range map[string]string{
		"duplicate plain and heading blocks": "Dispatch route\n- harness: codex\n- model: gpt-5.6-terra\n- reasoning-effort: medium\n- fallback: none\n\n## Dispatch route\n\n- harness: codex\n- model: gpt-5.6-terra\n- reasoning-effort: medium\n- fallback: none",
		"duplicate heading blocks":           "# Dispatch route\n\n- harness: codex\n- model: gpt-5.6-terra\n- reasoning-effort: medium\n- fallback: none\n\n###### Dispatch route\n\n- harness: codex\n- model: gpt-5.6-terra\n- reasoning-effort: medium\n- fallback: none",
		"valid plain plus malformed heading": "Dispatch route\n- harness: codex\n- model: gpt-5.6-terra\n- reasoning-effort: medium\n- fallback: none\n\n## Dispatch route details",
		"valid heading plus malformed plain": "## Dispatch route\n\n- harness: codex\n- model: gpt-5.6-terra\n- reasoning-effort: medium\n- fallback: none\n\nDispatch route:",
		"missing field":                      "Dispatch route\n- harness: codex\n- model: gpt-5.6-terra\n- fallback: none",
		"empty field":                        "Dispatch route\n- harness: codex\n- model:\n- reasoning-effort: medium\n- fallback: none",
		"duplicate field":                    "Dispatch route\n- harness: codex\n- harness: claude-code\n- model: gpt-5.6-terra\n- reasoning-effort: medium\n- fallback: none",
		"unknown field":                      "Dispatch route\n- harness: codex\n- model: gpt-5.6-terra\n- reasoning-effort: medium\n- fallback: none\n- surprise: yes",
		"unknown harness":                    "Dispatch route\n- harness: mystery\n- model: model\n- reasoning-effort: medium\n- fallback: none",
		"unknown reasoning effort":           "Dispatch route\n- harness: codex\n- model: gpt-5.6-terra\n- reasoning-effort: enormous\n- fallback: none",
		"fallback":                           "Dispatch route\n- harness: codex\n- model: gpt-5.6-terra\n- reasoning-effort: medium\n- fallback: claude-code",
	} {
		t.Run(name, func(t *testing.T) {
			if route, err := ParseDispatchRoute(body); err == nil || route != nil {
				t.Fatalf("route=%#v err=%v, want strict rejection", route, err)
			}
		})
	}
}

func TestPollSkipsExistingIssueSessionsAfterRestart(t *testing.T) {
	store := &fakeStore{
		projects: []domain.ProjectRecord{{
			ID:            "demo",
			RepoOriginURL: "https://github.com/acme/demo.git",
			Config:        domain.ProjectConfig{TrackerIntake: domain.TrackerIntakeConfig{Enabled: true, Assignee: "alice"}},
		}},
		sessions: []domain.SessionRecord{{ID: "demo-1", ProjectID: "demo", IssueID: "github:acme/demo#12"}},
	}
	tracker := &fakeTracker{issues: []domain.Issue{{
		ID:        domain.TrackerID{Provider: domain.TrackerProviderGitHub, Native: "acme/demo#12"},
		Title:     "Already running",
		State:     domain.IssueOpen,
		Assignees: []string{"alice"},
	}}}
	spawner := &fakeSpawner{}

	if err := New(singleResolver(tracker), store, spawner, Config{Logger: discardLogger()}).Poll(context.Background()); err != nil {
		t.Fatalf("Poll() error = %v", err)
	}
	if len(spawner.calls) != 0 {
		t.Fatalf("spawn calls = %d, want 0", len(spawner.calls))
	}
}

func TestPollReplacesTerminatedIssueSession(t *testing.T) {
	store := &fakeStore{
		projects: []domain.ProjectRecord{{
			ID:            "demo",
			RepoOriginURL: "https://github.com/acme/demo.git",
			Config:        domain.ProjectConfig{TrackerIntake: domain.TrackerIntakeConfig{Enabled: true, Assignee: "alice"}},
		}},
		sessions: []domain.SessionRecord{{
			ID:           "demo-1",
			ProjectID:    "demo",
			IssueID:      "github:acme/demo#12",
			IsTerminated: true,
		}},
	}
	tracker := &fakeTracker{issues: []domain.Issue{{
		ID:        domain.TrackerID{Provider: domain.TrackerProviderGitHub, Native: "acme/demo#12"},
		Title:     "Resume interrupted work",
		State:     domain.IssueOpen,
		Assignees: []string{"alice"},
	}}}
	spawner := &fakeSpawner{}

	if err := New(singleResolver(tracker), store, spawner, Config{Logger: discardLogger()}).Poll(context.Background()); err != nil {
		t.Fatalf("Poll() error = %v", err)
	}
	if len(spawner.calls) != 1 {
		t.Fatalf("spawn calls = %d, want 1 replacement", len(spawner.calls))
	}
	if spawner.calls[0].IssueID != "github:acme/demo#12" {
		t.Fatalf("replacement IssueID = %q", spawner.calls[0].IssueID)
	}
}

func TestPollSkipsSessionScanWhenIntakeDisabled(t *testing.T) {
	store := &fakeStore{
		projects:    []domain.ProjectRecord{{ID: "demo"}},
		sessionsErr: errors.New("session scan should not run"),
	}

	if err := New(singleResolver(&fakeTracker{}), store, &fakeSpawner{}, Config{Logger: discardLogger()}).Poll(context.Background()); err != nil {
		t.Fatalf("Poll() error = %v, want nil", err)
	}
}

func TestPollSkipsIneligibleAndInvalidProjects(t *testing.T) {
	store := &fakeStore{
		projects: []domain.ProjectRecord{
			{ID: "off", RepoOriginURL: "https://github.com/acme/off.git"},
			{ID: "broad", RepoOriginURL: "https://github.com/acme/broad.git", Config: domain.ProjectConfig{TrackerIntake: domain.TrackerIntakeConfig{Enabled: true}}},
			{ID: "missing-origin", Config: domain.ProjectConfig{TrackerIntake: domain.TrackerIntakeConfig{Enabled: true, Assignee: "alice"}}},
		},
	}
	tracker := &fakeTracker{issues: []domain.Issue{{
		ID:    domain.TrackerID{Provider: domain.TrackerProviderGitHub, Native: "acme/off#1"},
		Title: "ignored",
		State: domain.IssueOpen,
	}}}
	spawner := &fakeSpawner{}

	if err := New(singleResolver(tracker), store, spawner, Config{Logger: discardLogger()}).Poll(context.Background()); err != nil {
		t.Fatalf("Poll() error = %v", err)
	}
	if len(tracker.repos) != 0 {
		t.Fatalf("tracker was called for invalid/off projects: %+v", tracker.repos)
	}
	if len(spawner.calls) != 0 {
		t.Fatalf("spawn calls = %d, want 0", len(spawner.calls))
	}
}

func TestPollContinuesAfterTrackerAndSpawnFailures(t *testing.T) {
	store := &fakeStore{projects: []domain.ProjectRecord{
		{ID: "bad", RepoOriginURL: "https://github.com/acme/bad.git", Config: domain.ProjectConfig{TrackerIntake: domain.TrackerIntakeConfig{Enabled: true, Assignee: "alice"}}},
		{ID: "good", RepoOriginURL: "https://github.com/acme/good.git", Config: domain.ProjectConfig{TrackerIntake: domain.TrackerIntakeConfig{Enabled: true, Assignee: "alice"}}},
	}}
	tracker := &fakeTracker{
		failRepos: map[string]error{"acme/bad": errors.New("rate limited")},
		issuesByRepo: map[string][]domain.Issue{
			"acme/good": {
				{ID: domain.TrackerID{Provider: domain.TrackerProviderGitHub, Native: "acme/good#1"}, Title: "first", State: domain.IssueOpen, Assignees: []string{"alice"}},
				{ID: domain.TrackerID{Provider: domain.TrackerProviderGitHub, Native: "acme/good#2"}, Title: "second", State: domain.IssueOpen, Assignees: []string{"alice"}},
			},
		},
	}
	spawner := &fakeSpawner{failIssue: domain.IssueID("github:acme/good#1")}

	if err := New(singleResolver(tracker), store, spawner, Config{Logger: discardLogger()}).Poll(context.Background()); err != nil {
		t.Fatalf("Poll() error = %v", err)
	}
	if len(spawner.calls) != 2 {
		t.Fatalf("spawn attempts = %d, want 2", len(spawner.calls))
	}
	if spawner.calls[1].IssueID != "github:acme/good#2" {
		t.Fatalf("second spawn issue = %q", spawner.calls[1].IssueID)
	}
}

func TestPollBacksOffProjectAfterFailure(t *testing.T) {
	now := time.Date(2026, 6, 27, 10, 0, 0, 0, time.UTC)
	store := &fakeStore{projects: []domain.ProjectRecord{{
		ID:            "demo",
		RepoOriginURL: "https://github.com/acme/demo.git",
		Config:        domain.ProjectConfig{TrackerIntake: domain.TrackerIntakeConfig{Enabled: true, Assignee: "alice"}},
	}}}
	tracker := &fakeTracker{failRepos: map[string]error{"acme/demo": errors.New("rate limited")}}
	observer := New(singleResolver(tracker), store, &fakeSpawner{}, Config{
		Clock:          func() time.Time { return now },
		FailureBackoff: time.Minute,
		Logger:         discardLogger(),
	})

	if err := observer.Poll(context.Background()); err != nil {
		t.Fatalf("first Poll() error = %v", err)
	}
	if len(tracker.repos) != 1 {
		t.Fatalf("tracker calls after first poll = %d, want 1", len(tracker.repos))
	}

	if err := observer.Poll(context.Background()); err != nil {
		t.Fatalf("second Poll() error = %v", err)
	}
	if len(tracker.repos) != 1 {
		t.Fatalf("tracker calls during backoff = %d, want still 1", len(tracker.repos))
	}

	now = now.Add(time.Minute + time.Nanosecond)
	if err := observer.Poll(context.Background()); err != nil {
		t.Fatalf("third Poll() error = %v", err)
	}
	if len(tracker.repos) != 2 {
		t.Fatalf("tracker calls after backoff = %d, want 2", len(tracker.repos))
	}
}

func TestPollSkipsNonOpenIssueStates(t *testing.T) {
	store := &fakeStore{projects: []domain.ProjectRecord{{
		ID:            "demo",
		RepoOriginURL: "https://github.com/acme/demo.git",
		Config:        domain.ProjectConfig{TrackerIntake: domain.TrackerIntakeConfig{Enabled: true, Assignee: "alice"}},
	}}}
	tracker := &fakeTracker{issues: []domain.Issue{
		{ID: domain.TrackerID{Provider: domain.TrackerProviderGitHub, Native: "acme/demo#1"}, Title: "already active", State: domain.IssueInProgress, Assignees: []string{"alice"}},
		{ID: domain.TrackerID{Provider: domain.TrackerProviderGitHub, Native: "acme/demo#2"}, Title: "ready", State: domain.IssueOpen, Assignees: []string{"alice"}},
	}}
	spawner := &fakeSpawner{}

	if err := New(singleResolver(tracker), store, spawner, Config{Logger: discardLogger()}).Poll(context.Background()); err != nil {
		t.Fatalf("Poll() error = %v", err)
	}
	if len(spawner.calls) != 1 || spawner.calls[0].IssueID != "github:acme/demo#2" {
		t.Fatalf("spawn calls = %+v, want only open issue #2", spawner.calls)
	}
}

func TestPollAppliesLocalEligibilityFilter(t *testing.T) {
	store := &fakeStore{projects: []domain.ProjectRecord{{
		ID:            "demo",
		RepoOriginURL: "https://github.com/acme/demo.git",
		Config:        domain.ProjectConfig{TrackerIntake: domain.TrackerIntakeConfig{Enabled: true, Assignee: "alice"}},
	}}}
	tracker := &fakeTracker{issues: []domain.Issue{
		{ID: domain.TrackerID{Provider: domain.TrackerProviderGitHub, Native: "acme/demo#1"}, Title: "unassigned", State: domain.IssueOpen},
		{ID: domain.TrackerID{Provider: domain.TrackerProviderGitHub, Native: "acme/demo#2"}, Title: "wrong assignee", State: domain.IssueOpen, Assignees: []string{"bob"}},
		{ID: domain.TrackerID{Provider: domain.TrackerProviderGitHub, Native: "acme/demo#3"}, Title: "eligible", State: domain.IssueOpen, Labels: []string{"Agent-Ready"}, Assignees: []string{"Alice"}},
	}}
	spawner := &fakeSpawner{}

	if err := New(singleResolver(tracker), store, spawner, Config{Logger: discardLogger()}).Poll(context.Background()); err != nil {
		t.Fatalf("Poll() error = %v", err)
	}
	if len(spawner.calls) != 1 || spawner.calls[0].IssueID != "github:acme/demo#3" {
		t.Fatalf("spawn calls = %+v, want only eligible issue #3", spawner.calls)
	}
}

func TestIssueMatchesConfigAssigneeSpecialValues(t *testing.T) {
	assigned := domain.Issue{Assignees: []string{"alice"}}
	unassigned := domain.Issue{}
	if !issueMatchesConfig(assigned, domain.TrackerIntakeConfig{Assignee: "*"}) {
		t.Fatal("assigned issue should match assignee=*")
	}
	if issueMatchesConfig(unassigned, domain.TrackerIntakeConfig{Assignee: "*"}) {
		t.Fatal("unassigned issue should not match assignee=*")
	}
	if !issueMatchesConfig(unassigned, domain.TrackerIntakeConfig{Assignee: "none"}) {
		t.Fatal("unassigned issue should match assignee=none")
	}
	if issueMatchesConfig(assigned, domain.TrackerIntakeConfig{Assignee: "none"}) {
		t.Fatal("assigned issue should not match assignee=none")
	}
}

func TestBuildIssuePromptCapsLargeIssueBody(t *testing.T) {
	prompt := BuildIssuePrompt(domain.Issue{
		ID:    domain.TrackerID{Provider: domain.TrackerProviderGitHub, Native: "acme/demo#99"},
		Title: "Large issue",
		URL:   "https://github.com/acme/demo/issues/99",
		Body:  strings.Repeat("body ", 2000),
	})
	if len(prompt) > maxIntakePromptLen {
		t.Fatalf("prompt length = %d, want <= %d", len(prompt), maxIntakePromptLen)
	}
	if !strings.Contains(prompt, "Issue content truncated") {
		t.Fatalf("prompt missing truncation notice:\n%s", prompt)
	}
	if !strings.Contains(prompt, "https://github.com/acme/demo/issues/99") {
		t.Fatalf("prompt missing issue URL:\n%s", prompt)
	}
	if !strings.HasSuffix(prompt, intakePromptFooter) {
		t.Fatalf("prompt missing footer:\n%s", prompt)
	}
}

func TestTrackerRepoUsesConfiguredRepo(t *testing.T) {
	project := domain.ProjectRecord{
		ID:            "demo",
		RepoOriginURL: "https://github.com/wrong/repo.git",
		Config: domain.ProjectConfig{TrackerIntake: domain.TrackerIntakeConfig{
			Enabled:  true,
			Repo:     "acme/demo",
			Assignee: "alice",
		}},
	}
	repo, ok := trackerRepo(project, project.Config.TrackerIntake.WithDefaults())
	if !ok {
		t.Fatal("trackerRepo ok = false")
	}
	if repo.Native != "acme/demo" {
		t.Fatalf("repo.Native = %q, want acme/demo", repo.Native)
	}
}

func singleResolver(tracker ports.Tracker) TrackerResolver {
	return SingleTrackerResolver{Provider: domain.TrackerProviderGitHub, Adapter: tracker}
}

type fakeStore struct {
	projects    []domain.ProjectRecord
	sessions    []domain.SessionRecord
	sessionsErr error
}

func (f *fakeStore) ListProjects(context.Context) ([]domain.ProjectRecord, error) {
	return append([]domain.ProjectRecord(nil), f.projects...), nil
}

func (f *fakeStore) ListAllSessions(context.Context) ([]domain.SessionRecord, error) {
	return append([]domain.SessionRecord(nil), f.sessions...), f.sessionsErr
}

type fakeTracker struct {
	issues       []domain.Issue
	issuesByRepo map[string][]domain.Issue
	failRepos    map[string]error
	repos        []domain.TrackerRepo
	filters      []domain.ListFilter
}

func (f *fakeTracker) Get(context.Context, domain.TrackerID) (domain.Issue, error) {
	return domain.Issue{}, nil
}

func (f *fakeTracker) List(_ context.Context, repo domain.TrackerRepo, filter domain.ListFilter) ([]domain.Issue, error) {
	f.repos = append(f.repos, repo)
	f.filters = append(f.filters, filter)
	if err := f.failRepos[repo.Native]; err != nil {
		return nil, err
	}
	if f.issuesByRepo != nil {
		return append([]domain.Issue(nil), f.issuesByRepo[repo.Native]...), nil
	}
	return append([]domain.Issue(nil), f.issues...), nil
}

func (f *fakeTracker) Preflight(context.Context) error { return nil }

type fakeSpawner struct {
	calls     []ports.SpawnConfig
	failIssue domain.IssueID
}

func (f *fakeSpawner) Spawn(_ context.Context, cfg ports.SpawnConfig) (domain.Session, error) {
	f.calls = append(f.calls, cfg)
	if cfg.IssueID == f.failIssue {
		return domain.Session{}, errors.New("spawn failed")
	}
	return domain.Session{SessionRecord: domain.SessionRecord{ID: domain.SessionID(string(cfg.ProjectID) + "-1"), ProjectID: cfg.ProjectID, IssueID: cfg.IssueID, Kind: cfg.Kind}}, nil
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
