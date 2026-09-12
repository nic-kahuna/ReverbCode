# Managed startup restore

Agent Orchestrator restores recoverable sessions automatically by default. A
project can opt into preserve-only startup reconciliation when another local
controller must re-attest its own ownership or scheduling facts before AO
resumes work:

```bash
ao project set-config <project-id> --startup-restore-policy preserve_only
```

The policy is generic. AO does not know about issue trackers, capacity, leases,
or any external controller rule; it only preserves local session identity and
waits for the existing exact-session restore action.

## Policy and state model

`startupRestorePolicy` has two values:

- `automatic` is the explicit default. Omitted and legacy project configs
  resolve to `automatic`, retaining the existing startup reconciliation and
  bulk restore behavior.
- `preserve_only` adopts an exact runtime that actually survived daemon/app
  restart. If the saved runtime is absent, AO marks the session terminal and
  journals its worktrees without starting an agent, sending input, replaying a
  prompt, stashing files, deleting a worktree, or normalizing Git state.

The session API and CLI project a marker-bearing session consistently as
non-running: `status: terminated`, `isTerminated: true`, activity `exited`, and
no attachable terminal handle. The durable session row is not rewritten by this
read projection. The response also includes this machine-readable substate:

```json
{
  "status": "terminated",
  "isTerminated": true,
  "recovery": {
    "state": "awaiting_recovery",
    "policy": "preserve_only",
    "runtimeState": "absent",
    "providerSessionSaved": true,
    "worktrees": [
      {
        "repoName": "__root__",
        "branch": "ao/example-1",
        "baseSha": "",
        "worktreePath": "/path/to/managed/example-1",
        "preservedRef": "",
        "state": "preserved"
      }
    ]
  }
}
```

`providerSessionSaved` intentionally reveals only whether AO has a provider
session identity, never the opaque identifier. Requested and effective launch
routes remain on the normal session response. `ao session get <id> --json` and
`ao session ls --include-terminated --json` expose the same inventory as the
REST GET/list endpoints, so a controller never needs direct SQLite access.
`runtimeState: absent` means AO proved the saved runtime was gone before
publishing exact recovery disposition. `runtimeState: unknown` means the
journal is partial or the durable session row still carries an uncommitted live
fact; callers must not infer that a process is safely stopped.

The worktree journal distinguishes exact physical dispositions:

- `preserved`: the exact persisted worktree must still exist and be registered
  at its persisted path and branch.
- `preserved_removed`: the exact path must be absent and the persisted local
  branch must be available for re-attachment.
- `preserved_partial` means a prior explicit attempt crossed its mutation
  boundary or could not durably complete the recovery transition. It is
  evidence, not permission to retry; AO fails closed until an operator safely
  disposes of the ambiguity.

These marker states are authoritative recovery provenance. Removing the
project, damaging or omitting its config, or changing its policy cannot turn a
managed marker into permission for bulk automatic restore. Routine session
cleanup also skips these worktrees; only an exact successful restore or an
explicit session disposition consumes the journal.

## Exact explicit restore

Resume only one re-attested session:

```bash
ao session restore <exact-session-id>
```

For a preserve-only project, AO serializes concurrent restore calls and proves
the exact project, role, terminal session, absent saved runtime, provider
session identity, persisted requested/effective route, every repo/path/branch/
ref journal row, native resume command, binary, and runtime prerequisites before
mutating a worktree or launching a process. Missing or incompatible evidence
returns HTTP 409 with code `SESSION_RESTORE_FAILED`, stable
`details.phase` and `details.reason`, conditional `details.runtimeStopped` when
a rollback was attempted, and provider-ID-safe worktree evidence.

The managed path never inherits current model/reasoning defaults, uses a fresh
agent launch, replays the saved prompt, moves a stray path, creates a replacement
branch, stashes/resets files, deletes a worktree, or performs a broad process
kill. A preserve-ref conflict or later launch failure leaves the session
terminal and the partial worktree journal visible; a retry cannot replay it.

Legacy sessions whose `launchRoute` is null remain awaiting recovery and fail
explicit preflight. An external caller must choose a safe disposition; AO does
not silently substitute current project defaults.

## Restart cases

- Graceful daemon/app shutdown currently leaves runtime sessions running. A
  surviving exact runtime is adopted on reopen under either policy; no process
  or prompt is started.
- After a crash-like restart, an absent runtime in an automatic project follows
  the historical capture/restore path. The same fact in a preserve-only project
  becomes an awaiting-recovery journal with no workspace mutation or launch.
- A runtime probe error is not proof of death. AO records conservative partial
  recovery evidence, keeps the durable live/unknown fact, holds startup mutation
  lanes behind the reconciliation gate, and does not launch or clean anything.
- An already-terminal automatic marker remains eligible for historical bulk
  restore. A preserve-only marker is converted to one of the rollback-safe
  preserved states and skipped until exact explicit restore.
- Missing, archived, corrupt-config, unknown-policy, route-null, provider-ID-
  missing, stray, branch-conflict, and partially recovered cases fail closed.

## Config round-trip, upgrade, and rollback

Project config remains the existing JSON object. Whole-object edits must retain
`startupRestorePolicy` and unrelated fields; the CLI's `--config-json` path and
project GET/PUT round-trip preserve it. A malformed stored config remains
readable as a degraded project but is never treated as automatic by startup
reconciliation. A validated `project set-config` is the explicit repair path.

Migration `0025` widens the existing `session_worktrees.state` check and copies
every session/repo/branch/base/path/ref/state value verbatim. Its down migration
deliberately leaves the widened table and preserved rows intact: narrowing the
check would require deleting or coercing recovery evidence. Older binaries do
not recognize `preserved`, `preserved_removed`, or `preserved_partial`, so their
existing automatic bulk-restore filter skips those rows instead of replaying
them. That filter alone is not binary-downgrade safety: older startup
reconciliation runs before it and can destructively normalize a raw-live
managed session. Do not launch or downgrade to an older executable while any
managed marker exists. First use the newer build to restore or explicitly
discard every managed session, or postpone the binary rollback. The no-op down
migration only preserves the journal bytes for reinstalling the newer build;
it does not make the older lifecycle implementation marker-aware. Reinstalling
the newer build is idempotent and preserves the same evidence.
Changing the project back to `automatic` changes future boot policy but
deliberately does not make existing managed markers bulk-eligible.

## Operational canary (rollout owner only)

Installation and operational rollout are separate from this source change.
Before enabling the policy on a real project, inventory every exact session,
runtime, route, worktree, branch, and preserved ref. Use disposable sessions—
never existing workers or unique recovery evidence—as the first canaries:

1. Verify omitted/`automatic` config still restores a disposable recoverable
   session exactly as the prior build.
2. Enable `preserve_only` on a disposable project, perform a daemon/app reopen
   with a surviving runtime, and verify adoption with zero new launches.
3. Simulate an absent disposable runtime and verify session GET/list report
   `terminated` plus `recovery.state=awaiting_recovery`, with files/branch/ref
   untouched and no prompt delivery.
4. Re-attest that disposable session externally, restore its exact ID, and
   verify one native provider resume with the persisted route and zero prompt
   replay.
5. Exercise a disposable stray/conflict and verify structured 409 evidence,
   no runtime creation, and no Git/worktree normalization.
6. Re-inventory the real project before any enablement. To roll back future
   behavior, disable the policy only after stopping rollout; existing managed
   markers remain quarantined and must be restored or discarded one session at
   a time under the newer build. Do not launch an older binary or delete/rewrite
   markers as a rollback shortcut.
