package domain

import "time"

// WorkerCheckpointPrompt is the only pane message permitted through an active
// worker scheduling hold. Keeping it fixed prevents the checkpoint path from
// becoming a general-purpose send bypass.
const WorkerCheckpointPrompt = "Checkpoint your current progress now. Record completed work, remaining work, blockers, and the exact branch and worktree state. Do not start another task."

// WorkerSchedulingHold is the durable fact that prevents new work from being
// scheduled into one session. Worker-only validation belongs to the manager;
// storage preserves the fact for the referenced session.
type WorkerSchedulingHold struct {
	SessionID SessionID `json:"sessionId"`
	HeldAt    time.Time `json:"heldAt"`
}
