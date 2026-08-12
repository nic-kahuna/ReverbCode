// Package trackerintake implements the opt-in issue-intake observer. It polls a
// project's configured tracker for eligible issues and starts one worker session
// per issue, leaving PR/lifecycle handling to the existing observers.
package trackerintake

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"time"

	"github.com/yuin/goldmark"
	"github.com/yuin/goldmark/ast"
	"github.com/yuin/goldmark/extension"
	"github.com/yuin/goldmark/text"
	"github.com/yuin/goldmark/util"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/observe"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

const (
	// DefaultTickInterval is intentionally slower than runtime liveness checks:
	// intake is a backlog sweep, not an interactive status surface.
	DefaultTickInterval = time.Minute
	// DefaultFailureBackoff suppresses repeated polls for a project after an
	// intake failure. The observer retries automatically after this window.
	DefaultFailureBackoff = 5 * time.Minute
	// maxIntakePromptLen mirrors the session HTTP prompt limit. Intake uses the
	// session service directly, so it must enforce the same boundary itself.
	maxIntakePromptLen = 4096

	intakePromptTruncationNotice = "\n\n[Issue content truncated to fit the session prompt limit. Open the linked issue for the full details.]\n"
	intakePromptFooter           = "\nImplement the requested change in this repository, run the relevant checks, and open or update a pull request when ready."
)

var dispatchRouteMarkdownParser = goldmark.New(goldmark.WithExtensions(extension.GFM)).Parser()

// Store is the durable read surface the observer needs.
type Store interface {
	ListProjects(ctx context.Context) ([]domain.ProjectRecord, error)
	ListAllSessions(ctx context.Context) ([]domain.SessionRecord, error)
}

// Spawner is the session creation surface used by intake.
type Spawner interface {
	Spawn(ctx context.Context, cfg ports.SpawnConfig) (domain.Session, error)
}

// TrackerResolver picks the tracker adapter for a project's configured
// provider.
type TrackerResolver interface {
	Resolve(provider domain.TrackerProvider) (ports.Tracker, error)
}

// SingleTrackerResolver returns the same tracker for one specific provider and
// refuses every other provider. It exists so single-provider deployments don't
// need to construct a map.
type SingleTrackerResolver struct {
	Provider domain.TrackerProvider
	Adapter  ports.Tracker
}

// Resolve returns the wrapped adapter when the requested provider matches, or
// when the resolver was constructed without a provider pin.
func (s SingleTrackerResolver) Resolve(provider domain.TrackerProvider) (ports.Tracker, error) {
	if s.Adapter == nil {
		return nil, fmt.Errorf("tracker intake: no adapter for provider %q", provider)
	}
	if s.Provider == "" || provider == "" || provider == s.Provider {
		return s.Adapter, nil
	}
	return nil, fmt.Errorf("tracker intake: no adapter for provider %q", provider)
}

// Config holds optional observer knobs. Zero values use production defaults.
type Config struct {
	Tick           time.Duration
	FailureBackoff time.Duration
	Clock          func() time.Time
	Logger         *slog.Logger
}

// Observer polls configured projects and starts sessions for eligible issues.
type Observer struct {
	resolver       TrackerResolver
	store          Store
	spawner        Spawner
	tick           time.Duration
	failureBackoff time.Duration
	clock          func() time.Time
	logger         *slog.Logger
	backoffUntil   map[string]time.Time
}

// New constructs an Observer with safe defaults.
func New(resolver TrackerResolver, store Store, spawner Spawner, cfg Config) *Observer {
	o := &Observer{resolver: resolver, store: store, spawner: spawner, tick: cfg.Tick, failureBackoff: cfg.FailureBackoff, clock: cfg.Clock, logger: cfg.Logger, backoffUntil: map[string]time.Time{}}
	if o.tick <= 0 {
		o.tick = DefaultTickInterval
	}
	if o.failureBackoff <= 0 {
		o.failureBackoff = DefaultFailureBackoff
	}
	if o.clock == nil {
		o.clock = time.Now
	}
	if o.logger == nil {
		o.logger = slog.Default()
	}
	return o
}

// Start launches the observer loop. The first poll runs immediately inside the
// goroutine, keeping daemon startup non-blocking.
func (o *Observer) Start(ctx context.Context) <-chan struct{} {
	return observe.StartPollLoop(ctx, o.tick, o.Poll, o.logger, "tracker intake")
}

// Poll runs one synchronous intake pass. Store discovery failures are returned
// because they prevent the pass from knowing the current world; provider and
// spawn failures are logged and skipped so one bad issue/project does not block
// the rest of the daemon.
func (o *Observer) Poll(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if o.resolver == nil || o.store == nil || o.spawner == nil {
		return nil
	}
	now := o.clock().UTC()
	projects, err := o.store.ListProjects(ctx)
	if err != nil {
		return err
	}
	enabledProjects := make([]domain.ProjectRecord, 0, len(projects))
	for _, project := range projects {
		if project.Config.TrackerIntake.Enabled {
			enabledProjects = append(enabledProjects, project)
		}
	}
	if len(enabledProjects) == 0 {
		return nil
	}
	sessions, err := o.store.ListAllSessions(ctx)
	if err != nil {
		return err
	}
	seen := seenIssueIDs(sessions)
	for _, project := range enabledProjects {
		if err := ctx.Err(); err != nil {
			return err
		}
		if until, ok := o.backoffUntil[project.ID]; ok && now.Before(until) {
			o.logger.Debug("tracker intake: project in failure backoff", "project", project.ID, "until", until)
			continue
		}
		if failed := o.pollProject(ctx, project, seen); failed {
			o.backoffUntil[project.ID] = now.Add(o.failureBackoff)
		} else {
			delete(o.backoffUntil, project.ID)
		}
	}
	return nil
}

// pollProject returns failed=true for conditions that should be retried after a
// backoff window rather than logged on every poll.
func (o *Observer) pollProject(ctx context.Context, project domain.ProjectRecord, seen map[domain.IssueID]bool) (failed bool) {
	cfg := project.Config.TrackerIntake.WithDefaults()
	if !cfg.Enabled {
		return false
	}
	if err := cfg.Validate(); err != nil {
		o.logger.Warn("tracker intake: skipping project with invalid config", "project", project.ID, "err", err)
		return true
	}
	repo, ok := trackerRepo(project, cfg)
	if !ok {
		o.logger.Warn("tracker intake: skipping project without tracker scope", "project", project.ID, "provider", cfg.Provider, "origin", project.RepoOriginURL)
		return true
	}
	tracker, err := o.resolver.Resolve(cfg.Provider)
	if err != nil {
		o.logger.Warn("tracker intake: no adapter for provider", "project", project.ID, "provider", cfg.Provider, "err", err)
		return true
	}
	issues, err := tracker.List(ctx, repo, domain.ListFilter{
		State:    domain.ListOpen,
		Assignee: cfg.Assignee,
	})
	if err != nil {
		o.logger.Error("tracker intake: list issues failed", "project", project.ID, "repo", repo.Native, "err", err)
		return true
	}
	var spawnFailed bool
	for _, issue := range issues {
		if ctx.Err() != nil {
			return true
		}
		if issue.State != domain.IssueOpen {
			continue
		}
		if !issueMatchesConfig(issue, cfg) {
			continue
		}
		issueID := CanonicalIssueID(issue.ID)
		if issueID == "" || seen[issueID] {
			continue
		}
		route, err := ParseDispatchRoute(issue.Body)
		if err != nil {
			o.logger.Error("tracker intake: invalid dispatch route", "project", project.ID, "issue", issueID, "err", err)
			spawnFailed = true
			continue
		}
		if _, err := o.spawner.Spawn(ctx, ports.SpawnConfig{
			ProjectID: domain.ProjectID(project.ID),
			IssueID:   issueID,
			Kind:      domain.KindWorker,
			Route:     route,
			Prompt:    BuildIssuePrompt(issue),
		}); err != nil {
			o.logger.Error("tracker intake: spawn issue session failed", "project", project.ID, "issue", issueID, "err", err)
			spawnFailed = true
			continue
		}
		seen[issueID] = true
	}
	return spawnFailed
}

// ParseDispatchRoute reads the one canonical machine-readable route block from
// an issue body. No block preserves the project's normal default path. A
// present block is strict and complete so native intake cannot silently fill a
// missing route field from mutable defaults.
func ParseDispatchRoute(body string) (*domain.AgentRoute, error) {
	normalized := strings.ReplaceAll(body, "\r\n", "\n")
	source := []byte(normalized)
	lines := strings.Split(normalized, "\n")
	// Let Goldmark own CommonMark block classification. Only top-level
	// paragraphs and headings can declare an intake route; code examples and
	// nested quote/list content are ignored structurally.
	document := dispatchRouteMarkdownParser.Parse(text.NewReader(source))
	header := -1
	inspectHeaderLine := func(lineIndex int, allowPlain, allowATX bool) (bool, error) {
		if lineIndex < 0 || lineIndex >= len(lines) {
			return false, fmt.Errorf("invalid Dispatch route Markdown source position")
		}
		line := lines[lineIndex]
		matches, err := matchDispatchRouteHeader(line)
		if err != nil {
			return false, err
		}
		if !matches {
			return false, nil
		}
		isATX := strings.HasPrefix(strings.TrimSpace(line), "#")
		if isATX && !allowATX {
			return false, fmt.Errorf("invalid Dispatch route heading %q: malformed ATX syntax", strings.TrimSpace(line))
		}
		if !isATX && !allowPlain {
			return false, fmt.Errorf("invalid Dispatch route heading %q: only plain and ATX headers are supported", strings.TrimSpace(line))
		}
		if header >= 0 {
			return false, fmt.Errorf("multiple Dispatch route blocks")
		}
		header = lineIndex
		return true, nil
	}

	for node := document.FirstChild(); node != nil; node = node.NextSibling() {
		switch node := node.(type) {
		case *ast.Paragraph:
			rawAccepted := false
			for i := 0; i < node.Lines().Len(); i++ {
				lineIndex := markdownSourceLineIndex(normalized, node.Lines().At(i).Start)
				accepted, err := inspectHeaderLine(lineIndex, node.Lines().Len() == 1, false)
				if err != nil {
					return nil, err
				}
				rawAccepted = rawAccepted || accepted
			}
			if err := rejectVisiblyMalformedDispatchRoute(node, source, rawAccepted); err != nil {
				return nil, err
			}
		case *ast.Heading:
			if node.Lines().Len() == 0 {
				continue
			}
			// Goldmark uses Heading for both ATX and setext headings. ATX
			// nodes start at their opening hashes, before their content
			// segment; setext nodes start at the first content segment.
			if node.Pos() < node.Lines().At(0).Start {
				lineIndex := markdownSourceLineIndex(normalized, node.Pos())
				rawAccepted, err := inspectHeaderLine(lineIndex, false, true)
				if err != nil {
					return nil, err
				}
				if err := rejectVisiblyMalformedDispatchRoute(node, source, rawAccepted); err != nil {
					return nil, err
				}
				continue
			}
			for i := 0; i < node.Lines().Len(); i++ {
				lineIndex := markdownSourceLineIndex(normalized, node.Lines().At(i).Start)
				if _, err := inspectHeaderLine(lineIndex, false, false); err != nil {
					return nil, err
				}
			}
			if err := rejectVisiblyMalformedDispatchRoute(node, source, false); err != nil {
				return nil, err
			}
		}
	}
	if header < 0 {
		return nil, nil
	}

	values := map[string]string{}
	firstField := header + 1
	for firstField < len(lines) && strings.TrimSpace(lines[firstField]) == "" {
		firstField++
	}
	for i := firstField; i < len(lines); i++ {
		line := strings.TrimSpace(lines[i])
		if line == "" {
			break
		}
		if !strings.HasPrefix(line, "- ") {
			break
		}
		parts := strings.SplitN(strings.TrimSpace(strings.TrimPrefix(line, "- ")), ":", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid Dispatch route line %q", line)
		}
		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])
		switch key {
		case "harness", "model", "reasoning-effort", "fallback":
		default:
			return nil, fmt.Errorf("unknown Dispatch route field %q", key)
		}
		if _, exists := values[key]; exists {
			return nil, fmt.Errorf("duplicate Dispatch route field %q", key)
		}
		if value == "" {
			return nil, fmt.Errorf("dispatch route field %q is empty", key)
		}
		values[key] = value
	}

	for _, key := range []string{"harness", "model", "reasoning-effort", "fallback"} {
		if values[key] == "" {
			return nil, fmt.Errorf("dispatch route field %q is required", key)
		}
	}
	if values["fallback"] != "none" {
		return nil, fmt.Errorf("dispatch route fallback %q is unsupported; only none is allowed", values["fallback"])
	}
	route := &domain.AgentRoute{
		Harness:         domain.AgentHarness(values["harness"]),
		Model:           values["model"],
		ReasoningEffort: domain.ReasoningEffort(values["reasoning-effort"]),
	}
	if err := route.Validate(); err != nil {
		return nil, err
	}
	return route, nil
}

func markdownSourceLineIndex(source string, offset int) int {
	if offset < 0 || offset > len(source) {
		return -1
	}
	return strings.Count(source[:offset], "\n")
}

func rejectVisiblyMalformedDispatchRoute(node ast.Node, source []byte, rawAccepted bool) error {
	visibleText := markdownVisibleInlineText(node, source)
	visible := strings.Join(strings.Fields(visibleText), " ")
	if rawAccepted {
		if visible != "Dispatch route" {
			return fmt.Errorf("raw Dispatch route header has non-canonical visible content %q", visible)
		}
		return nil
	}
	candidates := append(strings.Split(visibleText, "\n"), visibleText)
	for _, line := range candidates {
		candidate := strings.Join(strings.Fields(line), " ")
		if looksLikeDispatchRouteHeading(candidate) {
			return fmt.Errorf("invalid visible Dispatch route header content %q", candidate)
		}
	}
	return nil
}

func markdownVisibleInlineText(node ast.Node, source []byte) string {
	var visible strings.Builder
	var appendNode func(ast.Node, bool)
	appendNode = func(node ast.Node, inCode bool) {
		switch node := node.(type) {
		case *ast.Text:
			value := node.Value(source)
			if !inCode && !node.IsRaw() {
				value = util.ResolveEntityNames(util.ResolveNumericReferences(util.UnescapePunctuations(value)))
			}
			visible.Write(value)
			if node.SoftLineBreak() || node.HardLineBreak() {
				visible.WriteByte('\n')
			}
			return
		case *ast.String:
			value := node.Value
			if !inCode && !node.IsRaw() && !node.IsCode() {
				value = util.ResolveEntityNames(util.ResolveNumericReferences(util.UnescapePunctuations(value)))
			}
			visible.Write(value)
			return
		case *ast.AutoLink:
			visible.Write(node.Label(source))
			return
		case *ast.RawHTML:
			return
		case *ast.CodeSpan:
			for child := node.FirstChild(); child != nil; child = child.NextSibling() {
				appendNode(child, true)
			}
			return
		}
		for child := node.FirstChild(); child != nil; child = child.NextSibling() {
			appendNode(child, inCode)
		}
	}
	for child := node.FirstChild(); child != nil; child = child.NextSibling() {
		appendNode(child, false)
	}
	return visible.String()
}

// matchDispatchRouteHeader accepts the canonical plain header and Markdown ATX
// headings with one through six opening hashes. A route-looking Markdown line
// with malformed heading syntax is an error so intake cannot silently downgrade
// a visibly requested route to the project's default.
func matchDispatchRouteHeader(line string) (bool, error) {
	trimmed := strings.TrimSpace(line)
	if trimmed == "Dispatch route" {
		return true, nil
	}

	hashes := 0
	for hashes < len(trimmed) && trimmed[hashes] == '#' {
		hashes++
	}
	if hashes == 0 {
		if looksLikeDispatchRouteHeading(trimmed) {
			return false, fmt.Errorf("invalid Dispatch route header content %q", trimmed)
		}
		return false, nil
	}

	remainder := trimmed[hashes:]
	content := strings.TrimSpace(remainder)
	if closingStart := strings.LastIndexAny(content, " \t"); closingStart >= 0 {
		closing := strings.TrimSpace(content[closingStart:])
		if closing != "" && strings.Trim(closing, "#") == "" {
			content = strings.TrimSpace(content[:closingStart])
		}
	}
	if !looksLikeDispatchRouteHeading(content) {
		return false, nil
	}
	if hashes > 6 {
		return false, fmt.Errorf("invalid Dispatch route heading level %d", hashes)
	}
	if remainder == "" || (remainder[0] != ' ' && remainder[0] != '\t') {
		return false, fmt.Errorf("invalid Dispatch route heading %q: whitespace is required after #", trimmed)
	}
	if content != "Dispatch route" {
		return false, fmt.Errorf("invalid Dispatch route heading content %q", content)
	}
	return true, nil
}

func looksLikeDispatchRouteHeading(content string) bool {
	const routeHeader = "dispatch route"
	lower := strings.ToLower(content)
	if lower == routeHeader {
		return true
	}
	if !strings.HasPrefix(lower, routeHeader) {
		return false
	}
	next := lower[len(routeHeader)]
	isLetter := next >= 'a' && next <= 'z'
	isDigit := next >= '0' && next <= '9'
	return !isLetter && !isDigit && next != '_'
}

func issueMatchesConfig(issue domain.Issue, cfg domain.TrackerIntakeConfig) bool {
	assignee := strings.TrimSpace(cfg.Assignee)
	switch {
	case assignee == "":
		return true
	case assignee == "*":
		return len(issue.Assignees) > 0
	case strings.EqualFold(assignee, "none"):
		return len(issue.Assignees) == 0
	default:
		return containsFold(issue.Assignees, assignee)
	}
}

func containsFold(values []string, needle string) bool {
	for _, value := range values {
		if strings.EqualFold(strings.TrimSpace(value), needle) {
			return true
		}
	}
	return false
}

func seenIssueIDs(sessions []domain.SessionRecord) map[domain.IssueID]bool {
	seen := make(map[domain.IssueID]bool, len(sessions))
	for _, sess := range sessions {
		if sess.IssueID != "" && !sess.IsTerminated {
			seen[sess.IssueID] = true
		}
	}
	return seen
}

// CanonicalIssueID stores tracker issue ids in sessions.issue_id with the
// provider included, so future providers cannot collide on native ids.
func CanonicalIssueID(id domain.TrackerID) domain.IssueID {
	provider := id.Provider
	if provider == "" {
		provider = domain.TrackerProviderGitHub
	}
	native := strings.TrimSpace(id.Native)
	if native == "" {
		return ""
	}
	return domain.IssueID(string(provider) + ":" + native)
}

// BuildIssuePrompt turns normalized issue facts into the worker's initial task.
func BuildIssuePrompt(issue domain.Issue) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Work on tracker issue %s.\n\n", CanonicalIssueID(issue.ID))
	if issue.Title != "" {
		fmt.Fprintf(&b, "Title: %s\n", issue.Title)
	}
	if issue.URL != "" {
		fmt.Fprintf(&b, "URL: %s\n", issue.URL)
	}
	if len(issue.Labels) > 0 {
		fmt.Fprintf(&b, "Labels: %s\n", strings.Join(issue.Labels, ", "))
	}
	if len(issue.Assignees) > 0 {
		fmt.Fprintf(&b, "Assignees: %s\n", strings.Join(issue.Assignees, ", "))
	}
	body := strings.TrimSpace(issue.Body)
	if body != "" {
		fmt.Fprintf(&b, "\nBody:\n%s\n", body)
	}
	b.WriteString(intakePromptFooter)
	return capIntakePrompt(b.String())
}

func capIntakePrompt(prompt string) string {
	if len(prompt) <= maxIntakePromptLen {
		return prompt
	}
	prefix := strings.TrimSuffix(prompt, intakePromptFooter)
	prefixBudget := maxIntakePromptLen - len(intakePromptTruncationNotice) - len(intakePromptFooter)
	if prefixBudget <= 0 {
		return truncateUTF8(prompt, maxIntakePromptLen)
	}
	return truncateUTF8(prefix, prefixBudget) + intakePromptTruncationNotice + intakePromptFooter
}

func truncateUTF8(s string, maxBytes int) string {
	if len(s) <= maxBytes {
		return s
	}
	cut := 0
	for i := range s {
		if i > maxBytes {
			break
		}
		cut = i
	}
	return s[:cut]
}

func trackerRepo(project domain.ProjectRecord, cfg domain.TrackerIntakeConfig) (domain.TrackerRepo, bool) {
	provider := cfg.Provider
	if provider == "" {
		provider = domain.TrackerProviderGitHub
	}
	if provider != domain.TrackerProviderGitHub {
		return domain.TrackerRepo{}, false
	}
	native := strings.TrimSpace(cfg.Repo)
	if native == "" {
		native = parseGitHubRepoNative(project.RepoOriginURL)
	}
	if native == "" {
		return domain.TrackerRepo{}, false
	}
	return domain.TrackerRepo{Provider: provider, Native: native}, true
}

func parseGitHubRepoNative(remote string) string {
	remote = strings.TrimSpace(remote)
	if remote == "" {
		return ""
	}
	if strings.HasPrefix(remote, "git@") {
		if _, rest, ok := strings.Cut(remote, ":"); ok {
			return cleanRepoPath(rest)
		}
	}
	if u, err := url.Parse(remote); err == nil && u.Host != "" {
		host := strings.TrimPrefix(strings.ToLower(u.Host), "www.")
		if host == "github.com" || strings.HasSuffix(host, ".github.com") || strings.HasSuffix(host, ".ghe.io") {
			return cleanRepoPath(u.Path)
		}
		return ""
	}
	return cleanRepoPath(remote)
}

func cleanRepoPath(path string) string {
	path = strings.Trim(strings.TrimSpace(path), "/")
	path = strings.TrimSuffix(path, ".git")
	parts := strings.Split(path, "/")
	if len(parts) < 2 {
		return ""
	}
	owner := strings.TrimSpace(parts[len(parts)-2])
	repo := strings.TrimSpace(parts[len(parts)-1])
	if owner == "" || repo == "" {
		return ""
	}
	return owner + "/" + repo
}
