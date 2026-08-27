import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { readUpdateSettings, writeUpdateSettings } from "./update-settings";

const mocks = vi.hoisted(() => {
	const send = vi.fn();
	return {
		autoUpdater: {
			channel: "",
			allowPrerelease: false,
			allowDowngrade: false,
			autoDownload: false,
			autoInstallOnAppQuit: false,
			on: vi.fn(),
			checkForUpdates: vi.fn().mockResolvedValue(undefined),
			downloadUpdate: vi.fn().mockResolvedValue(undefined),
			quitAndInstall: vi.fn(),
		},
		app: { isPackaged: true },
		dialog: { showMessageBox: vi.fn() },
		window: {
			isDestroyed: vi.fn(() => false),
			webContents: { send },
		},
		send,
	};
});

vi.mock("electron-updater", () => ({ autoUpdater: mocks.autoUpdater }));
vi.mock("electron", () => ({
	app: mocks.app,
	BrowserWindow: { getAllWindows: () => [mocks.window] },
	dialog: mocks.dialog,
}));

describe("auto-updater package policy", () => {
	let dirs: string[] = [];

	beforeEach(() => {
		vi.resetModules();
		vi.clearAllMocks();
		mocks.app.isPackaged = true;
		Object.assign(mocks.autoUpdater, {
			channel: "",
			allowPrerelease: false,
			allowDowngrade: false,
			autoDownload: false,
			autoInstallOnAppQuit: false,
		});
	});

	afterEach(async () => {
		vi.unstubAllEnvs();
		await Promise.all(dirs.map((dir) => rm(dir, { recursive: true, force: true })));
		dirs = [];
	});

	async function tempDir(): Promise<string> {
		const dir = await mkdtemp(path.join(os.tmpdir(), "ao-updater-policy-"));
		dirs.push(dir);
		return dir;
	}

	it("blocks every updater and first-run prompt path without touching electron-updater", async () => {
		vi.stubEnv("AO_DISABLE_DESKTOP_UPDATES", "true");
		const stateDir = await tempDir();
		const updater = await import("./auto-updater");

		expect(updater.getUpdateStatus()).toEqual({
			state: "unsupported",
			message: "Desktop updates are disabled by this packaged build.",
			policy: "ao-desktop-updates-disabled/v1",
		});

		await updater.startAutoUpdates(stateDir);
		await updater.checkForUpdatesNow(stateDir);
		await updater.downloadUpdateNow();
		updater.quitAndInstallUpdate();
		await updater.ensureUpdatePrefs(stateDir);

		expect(mocks.autoUpdater.on).not.toHaveBeenCalled();
		expect(mocks.autoUpdater.checkForUpdates).not.toHaveBeenCalled();
		expect(mocks.autoUpdater.downloadUpdate).not.toHaveBeenCalled();
		expect(mocks.autoUpdater.quitAndInstall).not.toHaveBeenCalled();
		expect(mocks.dialog.showMessageBox).not.toHaveBeenCalled();
		expect(mocks.send).toHaveBeenCalledWith("updates:status", {
			state: "unsupported",
			message: "Desktop updates are disabled by this packaged build.",
			policy: "ao-desktop-updates-disabled/v1",
		});
	});

	it("preserves automatic, manual, download, install, and first-run behavior by default", async () => {
		vi.stubEnv("AO_DISABLE_DESKTOP_UPDATES", "");
		const stateDir = await tempDir();
		await writeUpdateSettings(stateDir, { enabled: true, channel: "latest", nightlyAck: false });
		const updater = await import("./auto-updater");

		expect(updater.getUpdateStatus()).toEqual({ state: "idle" });
		await updater.startAutoUpdates(stateDir);
		expect(mocks.autoUpdater.checkForUpdates).toHaveBeenCalledTimes(1);
		expect(mocks.autoUpdater.autoDownload).toBe(true);
		expect(mocks.autoUpdater.autoInstallOnAppQuit).toBe(true);

		await updater.checkForUpdatesNow(stateDir);
		expect(mocks.autoUpdater.checkForUpdates).toHaveBeenCalledTimes(2);
		expect(mocks.autoUpdater.autoDownload).toBe(false);

		await updater.downloadUpdateNow();
		expect(mocks.autoUpdater.downloadUpdate).toHaveBeenCalledTimes(1);
		updater.quitAndInstallUpdate();
		expect(mocks.autoUpdater.quitAndInstall).toHaveBeenCalledWith(false, true);

		const firstRunDir = await tempDir();
		mocks.dialog.showMessageBox.mockResolvedValue({ response: 1 });
		await updater.ensureUpdatePrefs(firstRunDir);
		expect(mocks.dialog.showMessageBox).toHaveBeenCalledTimes(1);
		expect(await readUpdateSettings(firstRunDir)).toEqual({
			enabled: false,
			channel: "latest",
			nightlyAck: false,
		});
	});
});
