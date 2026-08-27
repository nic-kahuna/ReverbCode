import { defineConfig } from "vite";
import { desktopUpdatesDisabledFromValue } from "./src/main/desktop-update-policy";

// Forge's VitePlugin handles all main-process build configuration.
// Add overrides here only if needed (e.g. custom externals or aliases).
//
// Normalize the updater policy here so the main bundle receives a literal
// "true" or "false" string. Runtime environment changes cannot re-enable a
// package that was built with desktop updates disabled.
const desktopUpdatesDisabled = desktopUpdatesDisabledFromValue(process.env.AO_DISABLE_DESKTOP_UPDATES);

export default defineConfig({
	define: {
		"process.env.AO_DISABLE_DESKTOP_UPDATES": JSON.stringify(desktopUpdatesDisabled ? "true" : "false"),
		__AO_DESKTOP_UPDATE_POLICY_CANARY__: JSON.stringify(
			`ao-desktop-updates-${desktopUpdatesDisabled ? "disabled" : "enabled"}/v1`,
		),
	},
});
