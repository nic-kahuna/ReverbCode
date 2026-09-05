// @vitest-environment node
import { afterEach, describe, expect, it, vi } from "vitest";
import { mkdtemp, mkdir, realpath, rm, symlink, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { inspectBundledCompatibility, prepareDesktopStartup } from "./launch-compatibility";
import { resolveDaemonLaunch } from "../shared/daemon-launch";

const roots: string[] = [];
afterEach(async () => {
	await Promise.all(roots.splice(0).map((root) => rm(root, { recursive: true, force: true })));
});
async function fixture() {
	const root = await realpath(await mkdtemp(path.join(os.tmpdir(), "ao10-launch-")));
	roots.push(root);
	const env = {
		HOME: root,
		USERPROFILE: root,
		AO_DATA_DIR: path.join(root, "data"),
		AO_RUN_FILE: path.join(root, "running.json"),
	};
	const launch = resolveDaemonLaunch(env, true, root, root, process.platform)!;
	const proof = {
		schema: "ao-compatibility/v1",
		supportedProtocol: 1,
		markerPath: path.join(env.AO_DATA_DIR, "compatibility.json"),
		state: "missing_legacy",
		inspectionOnly: true,
		startPausedSupported: true,
	};
	return { root, env, launch, proof };
}

describe("bundled compatibility preflight", () => {
	it("executes the exact bundled binary with the daemon environment and no shell", async () => {
		const { env, launch, proof } = await fixture();
		const run = vi.fn(async () => ({ stdout: JSON.stringify(proof) }));
		await expect(inspectBundledCompatibility(launch, env, run)).resolves.toEqual({ freshData: true });
		expect(run).toHaveBeenCalledWith(launch.command, ["compatibility", "--json"], {
			cwd: launch.cwd,
			env,
			timeout: 10000,
			maxBuffer: 65536,
		});
	});
	it("binds absent children of symlinked directories and retains legacy data", async () => {
		const { root, env, launch, proof } = await fixture();
		await symlink(root, path.join(root, "alias"));
		env.AO_DATA_DIR = path.join(root, "alias", "data");
		const run = async () => ({ stdout: JSON.stringify(proof) });
		await expect(inspectBundledCompatibility(launch, env, run)).resolves.toEqual({ freshData: true });
		await mkdir(path.join(root, "data"));
		await writeFile(path.join(root, "data", "ao.db"), "preserve");
		await expect(inspectBundledCompatibility(launch, env, run)).resolves.toEqual({ freshData: false });
	});
	it("does not execute a packaged opaque override", async () => {
		const { env, root } = await fixture();
		const run = vi.fn();
		const launch = resolveDaemonLaunch(
			{ ...env, AO_DAEMON_COMMAND: "old-ao daemon" },
			true,
			root,
			root,
			process.platform,
		);
		await expect(inspectBundledCompatibility(launch, env, run)).rejects.toThrow("AO_START_REPAIR_REQUIRED");
		expect(run).not.toHaveBeenCalled();
	});
	it.each([
		{ state: "unsupported", requiredProtocol: 2 },
		{ state: "malformed" },
		{ markerPath: "/other/data/compatibility.json" },
		{ supportedProtocol: true },
		{ state: "compatible", requiredProtocol: true },
		{ state: "compatible", requiredProtocol: 2 },
		{ inspectionOnly: false },
		{ extra: "unrecognized" },
		{ requiredProtocol: 1 },
	])("rejects invalid proof %j", async (patch) => {
		const { env, launch, proof } = await fixture();
		await expect(
			inspectBundledCompatibility(launch, env, async () => ({ stdout: JSON.stringify({ ...proof, ...patch }) })),
		).rejects.toThrow();
	});
	it("accepts compatible guarded state and propagates binary refusal", async () => {
		const { env, launch, proof } = await fixture();
		await expect(
			inspectBundledCompatibility(launch, env, async () => ({
				stdout: JSON.stringify({ ...proof, state: "compatible", requiredProtocol: 1 }),
			})),
		).resolves.toEqual({ freshData: false });
		await expect(
			inspectBundledCompatibility(launch, env, async () => {
				throw new Error("AO_COMPATIBILITY_UNSUPPORTED");
			}),
		).rejects.toThrow("AO_COMPATIBILITY_UNSUPPORTED");
	});
});

describe("desktop startup effects", () => {
	it("keeps discovery and relocation untouched when compatibility fails", async () => {
		const writeDiscovery = vi.fn();
		const relocate = vi.fn();
		await expect(
			prepareDesktopStartup({
				isPackaged: true,
				installedVia: true,
				inspect: async () => {
					throw new Error("incompatible");
				},
				mayRelocate: () => true,
				writeDiscovery,
				relocate,
				report: vi.fn(),
			}),
		).rejects.toThrow("incompatible");
		expect(writeDiscovery).not.toHaveBeenCalled();
		expect(relocate).not.toHaveBeenCalled();
	});
	it("checks compatibility before provenance, relocation and final discovery", async () => {
		const calls: string[] = [];
		await prepareDesktopStartup({
			isPackaged: true,
			installedVia: true,
			inspect: async () => {
				calls.push("inspect");
				return { freshData: true };
			},
			mayRelocate: (fresh) => fresh,
			writeDiscovery: async () => {
				calls.push("discovery");
			},
			relocate: () => {
				calls.push("relocate");
			},
			report: vi.fn(),
		});
		expect(calls).toEqual(["inspect", "discovery", "relocate", "discovery"]);
	});
	it("preserves development startup and nonfatal discovery errors", async () => {
		const inspect = vi.fn();
		const report = vi.fn();
		await prepareDesktopStartup({
			isPackaged: false,
			installedVia: false,
			inspect,
			mayRelocate: () => false,
			writeDiscovery: async () => {
				throw new Error("write failed");
			},
			relocate: vi.fn(),
			report,
		});
		expect(inspect).not.toHaveBeenCalled();
		expect(report).toHaveBeenCalledOnce();
	});
});
