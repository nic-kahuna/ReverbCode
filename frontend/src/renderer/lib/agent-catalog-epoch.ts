import type { DaemonStatus } from "./daemon-status";

// Agent availability is process configuration, so a same-port daemon restart
// can still produce a different catalog. Include the process identity in the
// ready epoch and clear it while the supervisor reports a non-ready state.
export function agentCatalogDaemonEpoch(status: DaemonStatus): string | undefined {
	if (status.state !== "ready" || !status.port) return undefined;
	return `${status.port}:${status.pid ?? "unknown"}`;
}
