export const AGENT_OPTIONS = [
	"claude-code",
	"codex",
	"aider",
	"opencode",
	"grok",
	"droid",
	"amp",
	"agy",
	"crush",
	"cursor",
	"qwen",
	"copilot",
	"goose",
	"auggie",
	"continue",
	"devin",
	"cline",
	"kimi",
	"kiro",
	"kilocode",
	"vibe",
	"pi",
	"autohand",
] as const;

// Reviewer adapters are a subset of the known agent vocabulary. Renderer menus
// must still intersect this capability list with the daemon's active catalog.
export const REVIEWER_AGENT_OPTIONS = ["claude-code", "codex", "opencode"] as const;
