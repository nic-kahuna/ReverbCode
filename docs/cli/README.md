# AO CLI

The `ao` CLI is a thin Go/Cobra client for the local Agent Orchestrator daemon.
It starts, discovers, inspects, and stops the daemon through the loopback HTTP
surface and the `running.json` handshake. It must not open SQLite directly or
call runtime, workspace, tracker, or agent adapters in-process.

When using the CLI directly from a shell, make sure the daemon is running first
with `ao start` or by opening the desktop app. Product commands such as
`ao agent ls` and `ao spawn` call the loopback daemon and will fail with a
"daemon is not running" error if no `running.json` points at a live process. From
a source checkout, build and run the local binary explicitly, for example:

```bash
cd backend
go build -o ./bin/ao ./cmd/ao
./bin/ao agent ls
```

## Current commands

Every product command resolves to a daemon HTTP route. Run `ao <command>
--help` for the authoritative flag shape.

### Daemon control

| Command                       | Purpose                                                                                                                           |
| ----------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| `ao start`                    | Open the compatible containing desktop app; standalone bootstrap requires no existing AO data.                                                                        |
| `ao stop`                     | Gracefully stop the daemon via loopback `POST /shutdown` after verifying daemon identity.                                         |
| `ao status` / `--json`        | Report daemon state from `running.json`, process liveness, `/healthz`, and `/readyz`.                                             |
| `ao doctor` / `--json`        | Check config, data directory, DB-file presence, daemon state, `git`, and (on Darwin/Linux) `tmux`; on Windows conpty is built in. |
| `ao completion <shell>`       | Generate completions for `bash`, `zsh`, `fish`, or `powershell`.                                                                  |
| `ao version` / `ao --version` | Print build metadata.                                                                                                             |
| `ao daemon`                   | Hidden internal daemon entrypoint used by the desktop app.                                                                             |

### Product commands

| Command                             | Daemon route                                   |
| ----------------------------------- | ---------------------------------------------- |
| `ao project add`                    | `POST /api/v1/projects`                        |
| `ao project ls`                     | `GET /api/v1/projects`                         |
| `ao project get <id>`               | `GET /api/v1/projects/{id}`                    |
| `ao project set-config <id>`        | `PUT /api/v1/projects/{id}/config`             |
| `ao project admission <id>`         | `GET /api/v1/projects/{id}/admission`          |
| `ao project admission <id> --paused=true\|false` | `PUT /api/v1/projects/{id}/admission` |
| `ao project rm <id>`                | `DELETE /api/v1/projects/{id}`                 |
| `ao agent ls`                       | `GET /api/v1/agents`                           |
| `ao agent ls --refresh`             | `POST /api/v1/agents/refresh`                  |
| `ao spawn`                          | `POST /api/v1/sessions`                        |
| `ao session ls`                     | `GET /api/v1/sessions`                         |
| `ao session get <id>`               | `GET /api/v1/sessions/{id}`                    |
| `ao session kill <id>`              | `POST /api/v1/sessions/{id}/kill`              |
| `ao session restore <id>`           | `POST /api/v1/sessions/{id}/restore`           |
| `ao session rename <id> <name>`     | `PATCH /api/v1/sessions/{id}`                  |
| `ao session cleanup`                | `POST /api/v1/sessions/cleanup`                |
| `ao session claim-pr <id> <pr-ref>` | `POST /api/v1/sessions/{id}/pr/claim`          |
| `ao orchestrator ls`                | `GET /api/v1/orchestrators`                    |
| `ao send`                           | `POST /api/v1/sessions/{id}/send`              |
| `ao send --require-admission`       | `POST /api/v1/sessions/{id}/send-admitted`     |
| `ao preview [url]`                  | `POST /api/v1/sessions/{id}/preview`           |
| `ao hooks <agent> <event>`          | `POST /api/v1/sessions/{id}/activity` (hidden) |

`ao project admission <id> --paused=true` durably pauses new launches for one
project; `--paused=false` allows them again. Omit the flag to read the current
state. `--json` returns `projectId`, `admissionPaused`, `scope` (always
`new_launches_only`), and `existingSessionsMayBeRunning` (always `true`). Existing
workers can still write after admission is paused. This command does not suspend
them, release their leases, or transfer ownership to a foreground task.
Ordinary `set-config` replacements, including `--clear`, preserve the admission
pause unless `--config-json` explicitly supplies `admissionPaused` as a boolean.

Background controllers use `ao send --require-admission --session <id>
--message <text>` to require open project admission through message delivery.
The daemon checks and sends under the same admission gate, so a pause cannot
commit between those steps. Paused or unknown admission rejects the message.
The CLI uses the distinct `/send-admitted` endpoint, so an older daemon rejects
the request before delivery. It never falls back to ordinary send.
Ordinary `ao send` remains available for direct communication. This delivery
check does not stop existing writers or transfer ownership.

A pause waits for an already admitted launch to finish before it reports success.
If the request times out while waiting, admission remains unchanged and that
launch may still be finishing. Read admission again and retry; a keeper can retain
its own pause intent between attempts. Admission pause never proves an existing
worker, reviewer, child process, or file writer has stopped. Resuming admission
permits future launches; it does not restore preserved sessions automatically.

`ao agent ls` prints the daemon-supported agent catalog with local install/auth
readiness. Use `--refresh` to rerun the bounded local probes and `--json` to
print the raw inventory response.

`ao spawn` resolves project context in this order: explicit `--project`,
`AO_PROJECT_ID`, `AO_SESSION_ID` (by fetching the current session from the
daemon), then the current working directory matched against registered project
paths. If `AO_SESSION_ID` is set but the session cannot be fetched, pass
`--project` explicitly.

If `--agent` / `--harness` is omitted, `ao spawn` uses the resolved project's
`worker.agent` config. Before spawning, the CLI refreshes the advisory agent
catalog and fails early when the selected agent is unsupported, not installed,
or unauthorized. It warns-but-continues when auth remains unknown because daemon
spawn remains the authoritative runtime validation point. Use
`--skip-agent-check` to bypass only this CLI-side preflight.

`ao preview` resolves its session from the `AO_SESSION_ID` environment variable
(it is meant to run inside a session), not a flag. With no argument it
autodetects an `index.html` in the session workspace; with a URL argument it
opens that URL verbatim (`file://`, `http`, `https`).

`go run .` in `backend/` remains a compatibility wrapper around the daemon.

PR and review actions (merge, resolve-comments, review execute/send) are
HTTP-only today and driven by the frontend; there are no `ao pr` / `ao review`
commands yet.

## Configuration

The CLI and daemon share the same environment-driven config:

| Var                   | Default              | Purpose                                            |
| --------------------- | -------------------- | -------------------------------------------------- |
| `AO_PORT`             | `3001`               | Loopback daemon port.                              |
| `AO_RUN_FILE`         | `~/.ao/running.json` | PID/port handshake.                                |
| `AO_DATA_DIR`         | `~/.ao/data`         | SQLite data directory.                             |
| `AO_REQUEST_TIMEOUT`  | `60s`                | REST request timeout.                              |
| `AO_SHUTDOWN_TIMEOUT` | `10s`                | Graceful shutdown cap.                             |
| `AO_AGENT`            | `codex`              | Compatibility/default agent adapter.               |
| `AO_DISABLED_AGENTS`  | empty                | Comma-separated agent ids the daemon must not run. |

`AO_DISABLED_AGENTS` is a reversible availability policy, not an uninstall.
Disabled agents are omitted from the catalog and cannot spawn, review, restore,
or survive boot reconciliation, while their adapters and historical records
remain readable. Remove an id from the variable and restart the daemon to
re-enable it. For example:

```bash
AO_AGENT=codex AO_DISABLED_AGENTS=claude-code ao start
```

The daemon always binds `127.0.0.1`.

## Manual smoke test

```bash
cd backend
go build -o /tmp/ao ./cmd/ao

tmp=$(mktemp -d)
export AO_RUN_FILE="$tmp/running.json"
export AO_DATA_DIR="$tmp/data"
export AO_PORT=3037

/tmp/ao status --json
/tmp/ao doctor
/tmp/ao start
/tmp/ao status --json
/tmp/ao stop
/tmp/ao status --json
rm -rf "$tmp"
```

## Adding new commands

Add a product command only when a daemon HTTP route owns the corresponding
mutation/read; the CLI must call that route rather than reimplementing daemon
behavior. Commands not yet exposed but with backend routes in place include
`ao events ...` (over the CDC/SSE endpoint) and CLI parity for PR/review
actions.

Do not port old in-process TypeScript CLI behavior that mixed command handling
with storage and runtime implementation details.

### Native startup compatibility and maintenance preparation

`ao compatibility --json` reads only the native marker at
`<AO_DATA_DIR>/compatibility.json` and reports the binary's supported protocol.
It creates no data and contacts no daemon. Its `inspectionOnly: true` result is
observational: an installer must hold the canonical data-directory `ao.lock`,
recheck the marker and retain that lock across app/helper replacement. It must
also independently verify the candidate artifact identity. An unsupported,
malformed or unavailable marker emits inspection JSON and a nonzero exit.

`ao prepare-start-paused --json` is the explicit offline preparation command.
Stop the app and daemon first. The command holds `ao.lock`, checks compatibility
before opening/migrating SQLite, persists admission pauses for every project
(including archived projects), and exits without starting any runtime,
lifecycle, observer, HTTP server or telemetry lane. Its versioned proof lists
the paused project IDs; it does not prove existing worker processes stopped.
Read-only `ao project admission <id> --json` also reports archived project
policy for verification; admission changes remain forbidden on archived rows.

`ao daemon --start-paused` (or `AO_START_PAUSED=true`) applies the same pause
before subsystem construction and defaults newly registered projects to paused
for that boot. An ordinary later start preserves the individual persisted
values. Explicit per-project resume remains possible and does not automatically
restore existing sessions. This installation/maintenance option is separate
from selective foreground interference policy and is not a suspension receipt.
Admission protects new launches and admission-guarded messages; ordinary sends
and existing worker/lifecycle activity still require the later session fence.

The strict canonical marker has one supported representation for protocol 1:

```json
{"schema":"ao-data-compatibility/v1","requiredProtocol":1}
```

The file ends in one newline. The native boot guard acquires the canonical
lifetime lock before marker bootstrap, ordinary migrations or subsystem
construction. A future capable build must durably ratchet this monotonic floor
before writing newer custody state, under the same ownership. Failure, including
uncertain persistence, forbids the later write. The ratchet never lowers the
floor or reconstructs a marker deleted while ownership is held. Removing the
marker or editing it by hand is unsupported; absence is only the legacy
bootstrap case. New/unknown fields, noncanonical bytes and future protocols
fail closed rather than being repaired.

A supported guard-aware older binary refuses a future protocol before database
or runtime mutation. A pre-guard archived executable does not know this marker
exists: supported launcher/installer/rollback checks must reject it before
replacement or launch. Native source integration alone does not establish an
installed rollback floor. Existing app location and updater-off policy remain
unchanged. No protection against malicious same-user executable replacement is
claimed.

`ao import --dry-run` preserves project/configuration rows even when
`AO_START_PAUSED=true`; it does not apply maintenance pauses. It retains the
existing import store lifecycle, which can bootstrap/migrate SQLite, and the
native compatibility marker may be initialized. It is a project-change preview,
not the fully observational operation that `ao compatibility --json` provides.
Offline pause preparation verifies the effective decoded admission policy inside
its transaction; ambiguous JSON keys that leave effective admission open cause
the entire pause transaction to roll back.

### Supported desktop launch

The installed macOS `ao start` binds to its own canonical
`Contents/Resources/daemon/ao` executable and opens only that containing app.
Discovery markers and other installed copies cannot substitute for it. It reads
compatibility before launch; the daemon rechecks under `ao.lock` before opening
SQLite. A standalone CLI refuses discovery/download/install when its selected
data directory contains any existing state, including legacy data without a
compatibility marker. Use the installed app's launcher or repair the signed
installation. An empty unmanaged setup retains the bootstrap download path.

Packaged desktop startup rejects `AO_DAEMON_COMMAND` and inspects compatibility
using its bundled native binary with the same resolved environment as daemon
launch. Failure leaves launch discovery unchanged and appears as a startup
error. Development command overrides remain supported. Managed builds use the
existing immutable updater-disabled policy to prevent self-relocation; only a
fresh unmanaged package may relocate itself. The signed installer owns managed
bundle replacement and compatible rollback under the data-directory lock.

These are cooperative supported-entrypoint protections, not a way to retrofit
old binaries: directly running a pre-guard archived app or an unsupported old
installer remains outside the boundary. Compatibility is a monotonic protocol
floor, not a global suspension switch. Compatible startup preserves individual
project pauses and can continue unrelated work.
