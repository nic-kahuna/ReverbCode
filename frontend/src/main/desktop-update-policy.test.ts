import { afterEach, describe, expect, it, vi } from "vitest";
import path from "node:path";
import { loadConfigFromFile } from "vite";

describe("desktop update package policy", () => {
	afterEach(() => {
		vi.unstubAllEnvs();
		vi.resetModules();
	});

	it("defaults to enabled updates when the package setting is absent", async () => {
		vi.stubEnv("AO_DISABLE_DESKTOP_UPDATES", "");
		const policy = await import("./desktop-update-policy");

		expect(policy.DESKTOP_UPDATES_DISABLED).toBe(false);
		expect(policy.DESKTOP_UPDATE_POLICY_CANARY).toBe("ao-desktop-updates-enabled/v1");
	});

	it("only the exact package value true disables updates", async () => {
		const { desktopUpdatesDisabledFromValue } = await import("./desktop-update-policy");

		expect(desktopUpdatesDisabledFromValue("true")).toBe(true);
		expect(desktopUpdatesDisabledFromValue(undefined)).toBe(false);
		expect(desktopUpdatesDisabledFromValue("false")).toBe(false);
		expect(desktopUpdatesDisabledFromValue("TRUE")).toBe(false);
		expect(desktopUpdatesDisabledFromValue("1")).toBe(false);
	});

	it("normalizes the package setting into a main-bundle string literal", async () => {
		const configPath = path.resolve(process.cwd(), "vite.main.config.ts");

		vi.stubEnv("AO_DISABLE_DESKTOP_UPDATES", "true");
		const disabled = await loadConfigFromFile({ command: "build", mode: "production" }, configPath);
		expect(disabled?.config.define?.["process.env.AO_DISABLE_DESKTOP_UPDATES"]).toBe('"true"');
		expect(disabled?.config.define?.__AO_DESKTOP_UPDATE_POLICY_CANARY__).toBe('"ao-desktop-updates-disabled/v1"');

		vi.stubEnv("AO_DISABLE_DESKTOP_UPDATES", "false");
		const enabled = await loadConfigFromFile({ command: "build", mode: "production" }, configPath);
		expect(enabled?.config.define?.["process.env.AO_DISABLE_DESKTOP_UPDATES"]).toBe('"false"');
		expect(enabled?.config.define?.__AO_DESKTOP_UPDATE_POLICY_CANARY__).toBe('"ao-desktop-updates-enabled/v1"');
	});

	it("managed packages cannot relocate even on a fresh install", async () => {
		vi.stubEnv("AO_DISABLE_DESKTOP_UPDATES", "true");
		const policy = await import("./desktop-update-policy");
		expect(policy.mayRelocateDesktop("darwin", true, true)).toBe(false);
		process.env.AO_DISABLE_DESKTOP_UPDATES = "false";
		expect(policy.mayRelocateDesktop("darwin", true, true)).toBe(false);
	});
	it("general packages relocate only fresh packaged macOS installs", async () => {
		vi.stubEnv("AO_DISABLE_DESKTOP_UPDATES", "false");
		const policy = await import("./desktop-update-policy");
		expect(policy.mayRelocateDesktop("darwin", true, true)).toBe(true);
		expect(policy.mayRelocateDesktop("darwin", true, false)).toBe(false);
		expect(policy.mayRelocateDesktop("darwin", false, true)).toBe(false);
		expect(policy.mayRelocateDesktop("win32", true, true)).toBe(false);
	});

	it("exposes a stable unsupported status for disabled packages", async () => {
		vi.stubEnv("AO_DISABLE_DESKTOP_UPDATES", "true");
		const policy = await import("./desktop-update-policy");

		expect(policy.DESKTOP_UPDATES_DISABLED).toBe(true);
		expect(policy.DESKTOP_UPDATE_POLICY_CANARY).toBe("ao-desktop-updates-disabled/v1");
		expect(policy.desktopUpdatesDisabledStatus()).toEqual({
			state: "unsupported",
			message: "Desktop updates are disabled by this packaged build.",
			policy: "ao-desktop-updates-disabled/v1",
		});
	});
});
