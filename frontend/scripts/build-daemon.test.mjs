// @vitest-environment node
import { describe, expect, it } from "vitest";
import { buildLdflags, resolveBuildMetadata } from "./build-daemon.mjs";

function fakeGit(outputs) {
	return (args) => outputs[args.join(" ")] ?? "";
}

describe("resolveBuildMetadata", () => {
	it("uses package and Git identity as deterministic local defaults", () => {
		const metadata = resolveBuildMetadata({
			env: {},
			packageVersion: "1.2.3",
			git: fakeGit({
				"rev-parse HEAD": "abc123",
				"show -s --format=%cI abc123": "2026-08-12T10:20:30-10:00",
			}),
		});
		expect(metadata).toEqual({
			version: "1.2.3",
			commit: "abc123",
			date: "2026-08-12T10:20:30-10:00",
			releaseRepo: "",
		});
	});

	it("prefers explicit release values and preserves the fork repository", () => {
		const metadata = resolveBuildMetadata({
			env: {
				AO_BUILD_VERSION: "9.8.7",
				AO_BUILD_COMMIT: "release-sha",
				AO_BUILD_DATE: "2026-08-12T00:00:00Z",
				AO_RELEASE_REPO: "fork-owner/ReverbCode",
				GITHUB_SHA: "ignored-github-sha",
			},
			packageVersion: "1.2.3",
			git: () => {
				throw new Error("Git should not be consulted when all values are explicit");
			},
		});
		expect(metadata).toEqual({
			version: "9.8.7",
			commit: "release-sha",
			date: "2026-08-12T00:00:00Z",
			releaseRepo: "fork-owner/ReverbCode",
		});
	});

	it("derives a reproducible RFC3339 date from SOURCE_DATE_EPOCH", () => {
		const metadata = resolveBuildMetadata({
			env: { SOURCE_DATE_EPOCH: "0" },
			packageVersion: "1.2.3",
			git: fakeGit({ "rev-parse HEAD": "abc123" }),
		});
		expect(metadata.date).toBe("1970-01-01T00:00:00.000Z");
	});

	it("does not fail a source-archive build when Git identity is unavailable", () => {
		expect(resolveBuildMetadata({ env: {}, packageVersion: "1.2.3", git: () => "" })).toEqual({
			version: "1.2.3",
			commit: "",
			date: "",
			releaseRepo: "",
		});
	});

	it("rejects malformed explicit provenance instead of passing ambiguous linker flags", () => {
		expect(() =>
			resolveBuildMetadata({
				env: { AO_BUILD_COMMIT: "sha with spaces" },
				packageVersion: "1.2.3",
				git: () => "",
			}),
		).toThrow(/whitespace/);
		expect(() =>
			resolveBuildMetadata({
				env: { AO_RELEASE_REPO: "missing-repository" },
				packageVersion: "1.2.3",
				git: () => "",
			}),
		).toThrow(/owner\/repository/);
	});
});

describe("buildLdflags", () => {
	it("stamps shared build identity and the selected release repository", () => {
		expect(
			buildLdflags({
				version: "1.2.3",
				commit: "abc123",
				date: "2026-08-12T00:00:00Z",
				releaseRepo: "fork-owner/ReverbCode",
			}),
		).toBe(
			"-X github.com/aoagents/agent-orchestrator/backend/internal/buildinfo.Version=1.2.3 " +
				"-X github.com/aoagents/agent-orchestrator/backend/internal/buildinfo.Commit=abc123 " +
				"-X github.com/aoagents/agent-orchestrator/backend/internal/buildinfo.Date=2026-08-12T00:00:00Z " +
				"-X github.com/aoagents/agent-orchestrator/backend/internal/cli.releaseRepo=fork-owner/ReverbCode",
		);
	});

	it("omits unavailable optional identity without inventing values", () => {
		expect(buildLdflags({ version: "dev", commit: "", date: "", releaseRepo: "" })).toBe(
			"-X github.com/aoagents/agent-orchestrator/backend/internal/buildinfo.Version=dev",
		);
	});
});
