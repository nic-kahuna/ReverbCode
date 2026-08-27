import type { UpdateStatus } from "./update-settings";

declare const __AO_DESKTOP_UPDATE_POLICY_CANARY__: string | undefined;

const DISABLED_MESSAGE = "Desktop updates are disabled by this packaged build.";

export function desktopUpdatesDisabledFromValue(value: string | undefined): boolean {
	return value === "true";
}

// vite.main.config.ts replaces this exact environment access with a normalized
// string literal. It is therefore immutable in a packaged main bundle while
// remaining directly testable before bundling.
export const DESKTOP_UPDATES_DISABLED = desktopUpdatesDisabledFromValue(process.env.AO_DISABLE_DESKTOP_UPDATES);

export const DESKTOP_UPDATE_POLICY_CANARY =
	typeof __AO_DESKTOP_UPDATE_POLICY_CANARY__ === "string"
		? __AO_DESKTOP_UPDATE_POLICY_CANARY__
		: `ao-desktop-updates-${DESKTOP_UPDATES_DISABLED ? "disabled" : "enabled"}/v1`;

export function desktopUpdatesDisabledStatus(): UpdateStatus {
	return {
		state: "unsupported",
		message: DISABLED_MESSAGE,
		policy: DESKTOP_UPDATE_POLICY_CANARY,
	};
}
