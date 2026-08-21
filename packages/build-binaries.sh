#!/usr/bin/env bash
# Cross-compile the Go `ao` binary (backend/cmd/ao) for every supported
# platform and drop each into the matching platform package's bin/ dir.
#
# Run this from any cwd before `npm publish`. It is the ONLY way the binaries
# get into the platform packages; they are gitignored and produced here, then
# shipped in each npm tarball via that package's `files` entry.
#
# CGO-free build (modernc.org/sqlite driver) so cross-compilation needs no C
# toolchain. Build identity is stamped from @aoagents/ao's package version plus
# the current Git revision/date. AO_BUILD_* can pin reproducible release values;
# AO_RELEASE_REPO preserves fork publishing while the unset default remains the
# production repository compiled into the CLI.
set -euo pipefail

# Repo layout: this script lives at <repo>/packages/build-binaries.sh.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
BACKEND_DIR="${REPO_ROOT}/backend"
BUILDINFO_PATH="github.com/aoagents/agent-orchestrator/backend/internal/buildinfo"
RELEASE_REPO_PATH="github.com/aoagents/agent-orchestrator/backend/internal/cli.releaseRepo"

package_version() {
  awk -F'"' '$2 == "version" { print $4; exit }' "${SCRIPT_DIR}/ao/package.json"
}

git_value() {
  git -C "${REPO_ROOT}" "$@" 2>/dev/null || true
}

source_epoch_date() {
  if ! command -v node >/dev/null; then
    printf 'SOURCE_DATE_EPOCH requires node to produce a portable RFC3339 build date\n' >&2
    return 1
  fi
  node -e '
    const raw = process.env.SOURCE_DATE_EPOCH;
    if (!/^\d+$/.test(raw || "")) process.exit(2);
    const ms = Number(raw) * 1000;
    const date = new Date(ms);
    if (!Number.isSafeInteger(ms) || Number.isNaN(date.getTime())) process.exit(2);
    process.stdout.write(date.toISOString());
  '
}

validate_linker_value() {
  local name="$1"
  local value="$2"
  if [[ "${value}" =~ [[:space:]] ]]; then
    printf '%s must not contain whitespace\n' "${name}" >&2
    return 1
  fi
}

BUILD_VERSION="${AO_BUILD_VERSION:-$(package_version)}"
BUILD_VERSION="${BUILD_VERSION:-dev}"
BUILD_COMMIT="${AO_BUILD_COMMIT:-${GITHUB_SHA:-}}"
if [[ -z "${BUILD_COMMIT}" ]]; then
  BUILD_COMMIT="$(git_value rev-parse HEAD)"
fi
BUILD_DATE="${AO_BUILD_DATE:-}"
if [[ -z "${BUILD_DATE}" && -n "${SOURCE_DATE_EPOCH:-}" ]]; then
  BUILD_DATE="$(source_epoch_date)"
fi
if [[ -z "${BUILD_DATE}" ]]; then
  BUILD_DATE="$(git_value show -s --format=%cI "${BUILD_COMMIT:-HEAD}")"
fi
RELEASE_REPO="${AO_RELEASE_REPO:-}"

validate_linker_value AO_BUILD_VERSION "${BUILD_VERSION}"
[[ -z "${BUILD_COMMIT}" ]] || validate_linker_value AO_BUILD_COMMIT "${BUILD_COMMIT}"
[[ -z "${BUILD_DATE}" ]] || validate_linker_value AO_BUILD_DATE "${BUILD_DATE}"
[[ -z "${RELEASE_REPO}" ]] || validate_linker_value AO_RELEASE_REPO "${RELEASE_REPO}"
if [[ -n "${RELEASE_REPO}" && ! "${RELEASE_REPO}" =~ ^[^/]+/[^/]+$ ]]; then
  printf 'AO_RELEASE_REPO must be an owner/repository pair, got %s\n' "${RELEASE_REPO}" >&2
  exit 1
fi

LDFLAGS="-X ${BUILDINFO_PATH}.Version=${BUILD_VERSION}"
[[ -z "${BUILD_COMMIT}" ]] || LDFLAGS+=" -X ${BUILDINFO_PATH}.Commit=${BUILD_COMMIT}"
[[ -z "${BUILD_DATE}" ]] || LDFLAGS+=" -X ${BUILDINFO_PATH}.Date=${BUILD_DATE}"
[[ -z "${RELEASE_REPO}" ]] || LDFLAGS+=" -X ${RELEASE_REPO_PATH}=${RELEASE_REPO}"

# pkg_dir : npm_os : npm_arch : GOOS : GOARCH : bin_name
TARGETS=(
  "ao-darwin-arm64:darwin:arm64:darwin:arm64:ao"
  "ao-darwin-x64:darwin:x64:darwin:amd64:ao"
  "ao-win32-x64:win32:x64:windows:amd64:ao.exe"
  "ao-linux-x64:linux:x64:linux:amd64:ao"
)

echo "Building ao binaries from ${BACKEND_DIR}/cmd/ao"
for t in "${TARGETS[@]}"; do
  IFS=":" read -r pkg npm_os npm_arch goos goarch bin <<<"$t"
  out="${SCRIPT_DIR}/${pkg}/bin/${bin}"
  mkdir -p "${SCRIPT_DIR}/${pkg}/bin"
  echo "  -> ${pkg} (GOOS=${goos} GOARCH=${goarch}) -> bin/${bin}"
  (cd "${BACKEND_DIR}" && CGO_ENABLED=0 GOOS="${goos}" GOARCH="${goarch}" \
    go build -trimpath -ldflags "${LDFLAGS}" -o "${out}" ./cmd/ao)
  chmod 0755 "${out}"
done

echo "Done. Built binaries:"
for t in "${TARGETS[@]}"; do
  IFS=":" read -r pkg _ _ _ _ bin <<<"$t"
  file "${SCRIPT_DIR}/${pkg}/bin/${bin}"
done
