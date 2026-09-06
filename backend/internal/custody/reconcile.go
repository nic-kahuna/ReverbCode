package custody

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"time"
)

// ClaimObservation is read-only authority state. It never confers launch/input
// permission and absence is not converted into a release or a fresh claim.
type ClaimObservation struct {
	Generation int64
	Phase      string
}
type authorityObserver interface {
	ObserveClaim(context.Context, Attempt) (ClaimObservation, error)
}

func (c *CommandAuthority) ObserveClaim(ctx context.Context, a Attempt) (ClaimObservation, error) {
	ctx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, c.executable, "status")
	cmd.Env = append(withoutEnv(os.Environ(), "AO_DATA_DIR"), "AO_DATA_DIR="+c.dataDir)
	var stdout, stderr boundedBuffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return ClaimObservation{}, fmt.Errorf("%w: claim reconciliation unavailable", ErrAdmission)
	}
	if err := checkpointUniqueJSON(stdout.Bytes()); err != nil {
		return ClaimObservation{}, err
	}
	var state struct {
		OK      bool   `json:"ok"`
		Command string `json:"command"`
		State   struct {
			Version int `json:"version"`
		} `json:"state"`
		Admission struct {
			Protocol   int    `json:"protocol"`
			Bootstrap  string `json:"bootstrap"`
			Generation int64  `json:"generation"`
		} `json:"admission"`
		Claims []json.RawMessage `json:"claims"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &state); err != nil {
		return ClaimObservation{}, err
	}
	if !state.OK || state.Command != "status" || state.State.Version != 5 || state.Admission.Protocol != 2 || state.Admission.Bootstrap != "complete" || state.Admission.Generation < 1 || state.Claims == nil {
		return ClaimObservation{}, ErrAdmission
	}
	found := false
	var out ClaimObservation
	for _, raw := range state.Claims {
		var accepted Accepted
		if err := json.Unmarshal(raw, &accepted.Claim); err != nil {
			return out, err
		}
		claim := accepted.Claim
		if claim.ClaimID != a.ClaimID {
			continue
		}
		n := claim.Native
		if found || claim.Executor != "ao" || claim.Project != a.Project || claim.Owner != a.SessionID || n.SessionID != a.SessionID || n.AttemptID != a.AttemptID || n.Operation != a.Operation || n.Generation < a.ClaimGeneration || n.Generation < 1 {
			return out, ErrConflict
		}
		switch n.Phase {
		case "preparing", "launching", "running", "quiesced":
		default:
			return out, ErrUnknown
		}
		out = ClaimObservation{n.Generation, n.Phase}
		found = true
	}
	if !found {
		return out, fmt.Errorf("%w: exact claim absent; preserved native debt is not released", ErrUnknown)
	}
	return out, nil
}

// reconcileQuiesced settles only a known stopped attempt. It may recover a
// committed lost reserve/launch/result acknowledgement, but never advances a
// provider, replays a command or treats a timeout as absence.
func (c *Coordinator) reconcileQuiesced(ctx context.Context, a Attempt) (Attempt, error) {
	reader, ok := c.Authority.(authorityObserver)
	if !ok {
		return a, nil
	}
	observed, err := reader.ObserveClaim(ctx, a)
	if err != nil {
		return a, err
	}
	lane := c.Gate.lane(a.SessionID)
	lane.Lock()
	defer lane.Unlock()
	fresh, exists, err := c.Store.CurrentAttempt(ctx, a.SessionID)
	if err != nil {
		return a, err
	}
	if !exists || fresh.AttemptID != a.AttemptID || fresh.Phase != "quiesced" || fresh.Retired {
		return fresh, ErrConflict
	}
	if err = c.VerifyStoredCertificate(ctx, fresh); err != nil {
		return fresh, err
	}
	if fresh.ClaimGeneration > observed.Generation {
		return fresh, ErrConflict
	}
	if fresh.ClaimGeneration == observed.Generation && fresh.ClaimPhase == observed.Phase {
		return fresh, nil
	}
	fresh.ClaimGeneration = observed.Generation
	fresh.ClaimPhase = observed.Phase
	return c.Store.UpdateAttempt(ctx, fresh, fresh.Revision)
}
