import { mkdirSync, readFileSync, rmSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";

const scriptsDir = dirname(fileURLToPath(import.meta.url));
const frontendRoot = resolve(scriptsDir, "..");
const repoRoot = resolve(frontendRoot, "..");
const backendRoot = join(repoRoot, "backend");
const outDir = join(frontendRoot, "daemon");
const outPath = join(outDir, process.platform === "win32" ? "ao.exe" : "ao");

const modulePath = "github.com/aoagents/agent-orchestrator/backend";
const buildinfoPath = `${modulePath}/internal/buildinfo`;
const releaseRepoPath = `${modulePath}/internal/cli.releaseRepo`;

function optionalEnv(env, name) {
	const value = env[name]?.trim();
	return value || "";
}

function dateFromSourceEpoch(raw) {
	if (!/^\d+$/.test(raw)) {
		throw new Error(`SOURCE_DATE_EPOCH must be a non-negative integer, got ${JSON.stringify(raw)}`);
	}
	const milliseconds = Number(raw) * 1_000;
	const date = new Date(milliseconds);
	if (!Number.isSafeInteger(milliseconds) || Number.isNaN(date.getTime())) {
		throw new Error(`SOURCE_DATE_EPOCH is outside the supported date range: ${raw}`);
	}
	return date.toISOString();
}

function assertLinkerValue(name, value) {
	if (/[\s\0]/.test(value)) {
		throw new Error(`${name} must not contain whitespace or NUL bytes`);
	}
}

function readPackageVersion(packagePath) {
	const parsed = JSON.parse(readFileSync(packagePath, "utf8"));
	if (typeof parsed.version !== "string" || parsed.version.trim() === "") {
		throw new Error(`${packagePath} does not contain a non-empty version`);
	}
	return parsed.version.trim();
}

function gitCommand(args) {
	const result = spawnSync("git", args, {
		cwd: repoRoot,
		encoding: "utf8",
		stdio: ["ignore", "pipe", "ignore"],
	});
	if (result.error || result.status !== 0) return "";
	return result.stdout.trim();
}

// Resolve build identity without requiring a release environment. Explicit
// AO_BUILD_* values are useful for reproducible release/canary builds; ordinary
// local packaging falls back to package.json plus the current Git revision and
// commit timestamp. A source archive with no Git metadata still builds, with
// empty commit/date fields rather than invented provenance.
export function resolveBuildMetadata({ env = process.env, packageVersion, git = gitCommand } = {}) {
	const version = optionalEnv(env, "AO_BUILD_VERSION") || packageVersion?.trim() || "dev";
	const commit = optionalEnv(env, "AO_BUILD_COMMIT") || optionalEnv(env, "GITHUB_SHA") || git(["rev-parse", "HEAD"]);

	let date = optionalEnv(env, "AO_BUILD_DATE");
	const sourceDateEpoch = optionalEnv(env, "SOURCE_DATE_EPOCH");
	if (!date && sourceDateEpoch) {
		date = dateFromSourceEpoch(sourceDateEpoch);
	}
	if (!date) {
		date = git(["show", "-s", "--format=%cI", commit || "HEAD"]);
	}

	const releaseRepo = optionalEnv(env, "AO_RELEASE_REPO");
	for (const [name, value] of Object.entries({ version, commit, date, releaseRepo })) {
		if (value) assertLinkerValue(name, value);
	}
	if (releaseRepo && !/^[^/]+\/[^/]+$/.test(releaseRepo)) {
		throw new Error(`AO_RELEASE_REPO must be an owner/repository pair, got ${JSON.stringify(releaseRepo)}`);
	}
	return { version, commit, date, releaseRepo };
}

export function buildLdflags(metadata) {
	const values = [
		[`${buildinfoPath}.Version`, metadata.version],
		[`${buildinfoPath}.Commit`, metadata.commit],
		[`${buildinfoPath}.Date`, metadata.date],
		[releaseRepoPath, metadata.releaseRepo],
	];
	return values
		.filter(([, value]) => value)
		.map(([name, value]) => `-X ${name}=${value}`)
		.join(" ");
}

function buildDaemon() {
	const packageVersion = readPackageVersion(join(frontendRoot, "package.json"));
	const metadata = resolveBuildMetadata({ packageVersion });
	const ldflags = buildLdflags(metadata);

	rmSync(outDir, { recursive: true, force: true });
	mkdirSync(outDir, { recursive: true });

	const args = ["build", "-trimpath"];
	if (ldflags) args.push("-ldflags", ldflags);
	args.push("-o", outPath, "./cmd/ao");

	const result = spawnSync("go", args, {
		cwd: backendRoot,
		stdio: "inherit",
	});

	if (result.error) {
		console.error(`failed to start go build: ${result.error.message}`);
		process.exit(1);
	}

	if (result.status !== 0) {
		process.exit(result.status ?? 1);
	}
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
	buildDaemon();
}
