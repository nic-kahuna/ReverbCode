import { describe, expect, it } from "vitest";
import { agentCatalogDaemonEpoch } from "./agent-catalog-epoch";

describe("agentCatalogDaemonEpoch", () => {
	it("changes across a same-port daemon restart", () => {
		expect(agentCatalogDaemonEpoch({ state: "ready", port: 3037, pid: 101 })).toBe("3037:101");
		expect(agentCatalogDaemonEpoch({ state: "ready", port: 3037, pid: 202 })).toBe("3037:202");
	});

	it.each(["starting", "stopped", "error"] as const)("clears the epoch while the daemon is %s", (state) => {
		expect(agentCatalogDaemonEpoch({ state, port: 3037, pid: 101 })).toBeUndefined();
	});
});
