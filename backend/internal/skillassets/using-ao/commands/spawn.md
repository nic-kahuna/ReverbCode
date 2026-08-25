# ao spawn

Spawn a worker agent session in a registered project. The session runs the chosen agent in a fresh git worktree. Register the project first with `ao project add`.

## Syntax

```
ao spawn [flags]
```

## Flags

| Flag | Meaning | Default / Required |
|---|---|---|
| `--branch string` | Branch for the session worktree | `ao/<session-id>/root` |
| `--claim-pr string` | Immediately claim an existing PR for the spawned session | - |
| `--harness string` | Agent harness to use (see list below) | Project `worker.agent`; required if the project has none |
| `--issue string` | Issue id to associate with the session | - |
| `--model string` | Per-session model override; must be supplied with `--reasoning-effort` | Project/provider default |
| `--name string` | Display name shown in the sidebar (max 20 characters) | Derived from `--prompt` when omitted |
| `--no-takeover` | Refuse if another active session owns the claimed PR (requires `--claim-pr`) | - |
| `--project string` | Project id to spawn the session in | Required |
| `--prompt string` | Initial prompt for the agent | - |
| `--reasoning-effort string` | Per-session reasoning override; must be supplied with `--model` | Project/provider default |
| `--skip-agent-check` | Skip the advisory harness install/auth preflight | - |

`--agent` is an alias for `--harness`.

Run `ao agent ls` for the agents currently enabled by the daemon. The catalog is
authoritative because an operator may temporarily disable a shipped adapter.

`--model` and `--reasoning-effort` form one complete per-session route. AO rejects a partial pair before creating the session. AO validates the common effort vocabulary `low`, `medium`, `high`, `xhigh`, `max`, and `ultra`; each harness adapter may reject values its provider CLI does not support. AO passes the requested values to the provider without silently substituting a fallback.

## Examples

```bash
# Spawn a worker for issue 142 in the agent-orchestrator project
ao spawn --project agent-orchestrator --issue 142 --name "fix-session-leak" --prompt "Fix the session leak described in issue 142. Branch off upstream/main."
```

```bash
# Spawn one explicitly routed Codex worker
ao spawn --project agent-orchestrator --issue 142 --name "fix-session-leak" \
  --harness codex --model gpt-5.6-terra --reasoning-effort medium
```

```bash
# Spawn a worker and immediately claim an open PR
ao spawn --project agent-orchestrator --name "review-pr-88" --claim-pr 88 --harness codex
```
