import { execFile } from "node:child_process";
import { lstat, realpath, readdir } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { promisify } from "node:util";
import type { DaemonLaunchSpec } from "../shared/daemon-launch";

const execute = promisify(execFile);

// Match native observational resolution, including absent children of aliases.
async function canonicalPath(value: string): Promise<string> {
	const absolute = path.resolve(value);
	try {
		await lstat(absolute);
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
		const parent = path.dirname(absolute);
		if (parent === absolute) throw error;
		return path.join(await canonicalPath(parent), path.basename(absolute));
	}
	return realpath(absolute);
}

export type CompatibilityRunner = (
	command: string,
	args: string[],
	options: {
		cwd: string;
		env: NodeJS.ProcessEnv;
		timeout: number;
		maxBuffer: number;
	},
) => Promise<{ stdout: string }>;

// Only the app's own bundled executable is eligible. No shell, discovery target,
// alternate daemon, SQLite reader or marker writer is used by this preflight.
export async function inspectBundledCompatibility(
	launch: DaemonLaunchSpec | null,
	env: NodeJS.ProcessEnv,
	run: CompatibilityRunner = execute,
): Promise<{ freshData: boolean }> {
	if (!launch || launch.source !== "bundled" || launch.shell) {
		throw new Error(
			"AO_START_REPAIR_REQUIRED: packaged AO requires its bundled daemon; remove AO_DAEMON_COMMAND and repair the signed installation if necessary.",
		);
	}
	const home = process.platform === "win32" ? env.USERPROFILE : env.HOME;
	const dataDir = env.AO_DATA_DIR || path.join(home || os.homedir(), ".ao", "data");
	const expectedMarker = path.join(await canonicalPath(path.resolve(launch.cwd, dataDir)), "compatibility.json");
	const { stdout } = await run(launch.command, ["compatibility", "--json"], {
		cwd: launch.cwd,
		env,
		timeout: 10_000,
		maxBuffer: 64 * 1024,
	});
	const value: unknown = JSON.parse(stdout);
	if (!value || typeof value !== "object" || Array.isArray(value))
		throw new Error("Invalid AO compatibility inspection.");
	const result = value as Record<string, unknown>;
	const keys = ["schema", "supportedProtocol", "markerPath", "state", "inspectionOnly", "startPausedSupported"];
	if (result.state === "compatible") keys.push("requiredProtocol");
	if (
		Object.keys(result).length !== keys.length ||
		!keys.every((key) => Object.hasOwn(result, key)) ||
		result.schema !== "ao-compatibility/v1" ||
		result.inspectionOnly !== true ||
		result.startPausedSupported !== true ||
		!Number.isSafeInteger(result.supportedProtocol) ||
		(result.supportedProtocol as number) < 1 ||
		result.markerPath !== expectedMarker ||
		(result.state !== "compatible" && result.state !== "missing_legacy") ||
		(result.state === "compatible" &&
			(!Number.isSafeInteger(result.requiredProtocol) ||
				(result.requiredProtocol as number) < 1 ||
				(result.requiredProtocol as number) > (result.supportedProtocol as number)))
	) {
		throw new Error(
			"AO_START_REPAIR_REQUIRED: invalid or incompatible bundled AO inspection for the selected data directory.",
		);
	}
	let freshData = false;
	if (result.state === "missing_legacy") {
		try {
			freshData = (await readdir(path.dirname(expectedMarker))).length === 0;
		} catch (error) {
			if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
			freshData = true;
		}
	}
	return { freshData };
}

// Keep the preflight and the discovery/relocation side effects in one ordered
// startup boundary. The caller still renders the ordinary startup error UI.
export async function prepareDesktopStartup(options: {
	isPackaged: boolean;
	inspect: () => Promise<{ freshData: boolean }>;
	mayRelocate: (freshData: boolean) => boolean;
	installedVia: boolean;
	writeDiscovery: () => Promise<void>;
	relocate: () => void;
	report: (error: unknown) => void;
}): Promise<void> {
	const freshData = options.isPackaged ? (await options.inspect()).freshData : false;
	const write = async () => {
		try {
			await options.writeDiscovery();
		} catch (error) {
			options.report(error);
		}
	};
	// Preserve first-install provenance across Electron's relocation/relaunch.
	if (options.installedVia) await write();
	if (options.mayRelocate(freshData)) {
		try {
			options.relocate();
		} catch (error) {
			options.report(error);
		}
	}
	await write();
}
