package custody

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	githubtracker "github.com/aoagents/agent-orchestrator/backend/internal/adapters/tracker/github"
	"strings"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

type Coordinator struct {
	Store          Store
	Gate           *Gate
	Runtime        *RuntimeFacade
	Authority      Authority
	DataDir        string
	LaunchPrepared func(context.Context, Attempt, string) (Attempt, error)
	Activity       func(context.Context, domain.SessionID, ports.ActivitySignal) error
}

func New(store Store, runtime *RuntimeFacade, gate *Gate, authority Authority, dataDir string) *Coordinator {
	c := &Coordinator{Store: store, Runtime: runtime, Gate: gate, Authority: authority, DataDir: dataDir}
	runtime.coordinator = c
	return c
}
func identifier() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

func (c *Coordinator) Begin(ctx context.Context, rec domain.SessionRecord, operation string) (Attempt, context.Context, error) {
	if c.Authority == nil {
		return Attempt{}, ctx, ErrAdmission
	}
	if err := ProcessCapability(); err != nil {
		return Attempt{}, ctx, err
	}
	project, exists, err := c.Store.GetProject(ctx, string(rec.ProjectID))
	if err != nil || !exists {
		return Attempt{}, ctx, ErrUnknown
	}
	n, err := IssueNumber(string(rec.IssueID), project.Config.TrackerIntake.Repo)
	if err != nil {
		return Attempt{}, ctx, err
	}
	existing, found, err := c.Store.CurrentAttempt(ctx, string(rec.ID))
	if err != nil {
		return existing, ctx, err
	}
	if found {
		if existing.Project != string(rec.ProjectID) || existing.IssueNumber != n || existing.Operation != operation || existing.PredecessorAttemptID != "" || existing.Fence != "" || len(existing.Operations) > 0 || existing.LaunchEffectStarted {
			return existing, ctx, ErrFenced
		}
		if existing.Phase == "seed" {
			return c.reserveSeed(ctx, existing)
		}
		if existing.Phase == "preparing" || existing.Phase == "launching" {
			return existing, c.preparationContext(c.Gate.admittedContext(ctx, existing), existing.SessionID), nil
		}
		return existing, ctx, ErrConflict
	}
	id, err := identifier()
	if err != nil {
		return Attempt{}, ctx, err
	}
	a := Attempt{Project: string(rec.ProjectID), SessionID: string(rec.ID), AttemptID: id, Operation: operation, IssueNumber: n, Phase: "seed", ClaimID: string(rec.ProjectID) + ":" + string(rec.ID) + ":" + id, Members: []ProcessIdentity{}, Operations: []string{}, Blockers: []string{}}
	a, err = c.Store.CreateAttempt(ctx, a)
	if err != nil {
		return a, ctx, err
	}
	return c.reserveSeed(ctx, a)
}

func (c *Coordinator) reserveSeed(ctx context.Context, a Attempt) (Attempt, context.Context, error) {
	accepted, err := c.Authority.Transition(ctx, "native-reserve", a, nil)
	if err != nil {
		return a, ctx, err
	}
	a.ClaimGeneration = accepted.Claim.Native.Generation
	a.ClaimPhase = accepted.Claim.Native.Phase
	a.IssueBody = *accepted.IssueBody
	route := accepted.Claim.Native.Source.Evidence.Route
	a.Route = &route
	a.Phase = "preparing"
	a, err = c.Store.UpdateAttempt(ctx, a, a.Revision)
	if err != nil {
		return a, ctx, err
	}
	return a, c.preparationContext(c.Gate.admittedContext(ctx, a), a.SessionID), nil
}

// Operation publishes an in-flight native effect before it starts. The session
// lane is not held while waiting for it, so requests can durably fence later
// effects. Completion removes only this operation from this exact generation.
func (c *Coordinator) Operation(ctx context.Context, id, name string, fn func() error) error {
	var attempt string
	completed := false
	err := c.Gate.Effect(ctx, id, true, func() error {
		a, ok, e := c.Store.CurrentAttempt(ctx, id)
		if e != nil {
			return e
		}
		if !ok {
			return ErrUnknown
		}
		for _, op := range a.CompletedOperations {
			if op == name {
				completed = true
				return nil
			}
		}
		for _, op := range a.Operations {
			if op == name {
				return ErrConflict
			}
		}
		a.Operations = append(a.Operations, name)
		attempt = a.AttemptID
		_, e = c.Store.UpdateAttempt(ctx, a, a.Revision)
		return e
	})
	if err != nil {
		return err
	}
	if completed {
		return nil
	}
	effectErr := fn()
	// A failed operation remains explicit recovery debt. Failure/cancellation is
	// never evidence its child processes ceased or its partial writes vanished.
	if effectErr != nil {
		return effectErr
	}
	lane := c.Gate.lane(id)
	lane.Lock()
	defer lane.Unlock()
	a, ok, err := c.Store.CurrentAttempt(ctx, id)
	if err != nil {
		return err
	}
	if !ok || a.AttemptID != attempt {
		return ErrConflict
	}
	out := []string{}
	for _, op := range a.Operations {
		if op != name {
			out = append(out, op)
		}
	}
	a.Operations = out
	a.CompletedOperations = append(a.CompletedOperations, name)
	_, err = c.Store.UpdateAttempt(ctx, a, a.Revision)
	return err
}

func (c *Coordinator) SetWorkspace(ctx context.Context, id, path string) error {
	lane := c.Gate.lane(id)
	lane.Lock()
	defer lane.Unlock()
	a, ok, err := c.Store.CurrentAttempt(ctx, id)
	if err != nil {
		return err
	}
	if !ok || a.Phase != "preparing" {
		return ErrConflict
	}
	a.Workspace = path
	_, err = c.Store.UpdateAttempt(ctx, a, a.Revision)
	return err
}

func (c *Coordinator) CommitLaunch(ctx context.Context, id string, h ports.RuntimeHandle) (Attempt, context.Context, error) {
	a, ok, err := c.Store.CurrentAttempt(ctx, id)
	if err != nil {
		return a, ctx, err
	}
	if !ok || a.Phase != "preparing" || a.Fence != "" || len(a.Operations) != 0 {
		return a, ctx, ErrFenced
	}
	accepted, err := c.Authority.Transition(ctx, "native-launch", a, nil)
	if err != nil {
		return a, ctx, err
	}
	// Admission I/O deliberately occurs outside the lane. A request which won
	// during the helper call is seen by the final CAS/effect check below.
	lane := c.Gate.lane(id)
	lane.Lock()
	defer lane.Unlock()
	fresh, ok, err := c.Store.CurrentAttempt(ctx, id)
	if err != nil {
		return a, ctx, err
	}
	if !ok || fresh.AttemptID != a.AttemptID || fresh.Revision != a.Revision || fresh.Fence != "" {
		return a, ctx, ErrFenced
	}
	a.ClaimGeneration = accepted.Claim.Native.Generation
	a.ClaimPhase = accepted.Claim.Native.Phase
	a.Phase = "launching"
	a.RuntimeHandleID = &h.ID
	a.IssueBody = *accepted.IssueBody
	route := accepted.Claim.Native.Source.Evidence.Route
	a.Route = &route
	a, err = c.Store.UpdateAttempt(ctx, a, a.Revision)
	if err != nil {
		return a, ctx, err
	}
	return a, c.Gate.admittedContext(ctx, a), nil
}

func (c *Coordinator) ObserveStarted(ctx context.Context, id string, h ports.RuntimeHandle) error {
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		a, ok, err := c.Store.CurrentAttempt(ctx, id)
		if err != nil {
			return err
		}
		if !ok {
			return ErrUnknown
		}
		tree, e := c.runtimeTree(ctx, a)
		if e == nil && sameProvider(tree, tree) == nil {
			lane := c.Gate.lane(id)
			lane.Lock()
			fresh, exists, e := c.Store.CurrentAttempt(ctx, id)
			if e == nil && exists && fresh.AttemptID == a.AttemptID && fresh.Phase == "launching" {
				fresh.Members = tree
				fresh.Phase = "running"
				fresh, e = c.Store.UpdateAttempt(ctx, fresh, fresh.Revision)
			} else if e == nil {
				e = ErrConflict
			}
			lane.Unlock()
			if e != nil {
				return e
			}
			return c.settleRunning(ctx, fresh)

		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(30 * time.Millisecond):
		}
	}
	return fmt.Errorf("%w: provider generation did not become observable", ErrUnknown)
}

// IssueNumber binds the canonical tracker id to the registered repository.
func IssueNumber(id, repository string) (int, error) {
	if !strings.HasPrefix(id, "github:") {
		return 0, ErrUnknown
	}
	owner, repo, n, err := githubtracker.ParseIssueID(strings.TrimPrefix(id, "github:"))
	if err != nil || owner+"/"+repo != repository {
		return 0, ErrConflict
	}
	return n, nil
}

func (c *Coordinator) settleRunning(ctx context.Context, a Attempt) error {
	if a.Phase != "running" || a.Retired {
		return ErrConflict
	}
	if a.ClaimPhase == "running" {
		return nil
	}
	state := "running"
	accepted, err := c.Authority.Transition(ctx, "native-result", a, &state)
	if err != nil {
		return err
	}
	lane := c.Gate.lane(a.SessionID)
	lane.Lock()
	defer lane.Unlock()
	fresh, ok, err := c.Store.CurrentAttempt(ctx, a.SessionID)
	if err != nil {
		return err
	}
	if !ok || fresh.AttemptID != a.AttemptID || fresh.Phase != "running" {
		return ErrConflict
	}
	fresh.ClaimGeneration = accepted.Claim.Native.Generation
	fresh.ClaimPhase = accepted.Claim.Native.Phase
	_, err = c.Store.UpdateAttempt(ctx, fresh, fresh.Revision)
	return err
}

// Retryable marks inert unlaunched attempts for intake. Their exact durable
// claim/session is reused by Manager.Spawn; no seed deletion or new identity.
func (c *Coordinator) Retryable(ctx context.Context, id string) (bool, error) {
	a, ok, err := c.Store.CurrentAttempt(ctx, id)
	if err != nil || !ok {
		return false, err
	}
	return RetryableAttempt(a), nil
}

func RetryableAttempt(a Attempt) bool {
	return !a.Retired && a.Fence == "" && !a.LaunchEffectStarted && len(a.Operations) == 0 && a.PredecessorAttemptID == "" && (a.Phase == "seed" || a.Phase == "preparing" || a.Phase == "launching")
}
