#!/usr/bin/env node
// Resolves which packages in this monorepo need a release, and what version
// each one should get, from conventional-commit history scoped to each
// package's own folder.
//
// Invoked by .github/workflows/release.yaml's `plan` job. Can also be run
// locally to preview a release without touching git or npm:
//
//   GITHUB_REF_NAME=master node .github/scripts/plan-release.mjs --dry-run
//
// Design notes (see .claude/plans/this-project-is-locked-robust-finch.md for
// the full writeup this implements):
//
// - Package discovery is a *depth-1* glob (`*/package.json`). Every decoy
//   template manifest in this repo (leo-cli scaffolding like
//   `____DIRNAME____`, `__bot01_id__`) lives under `*/templates/**`, i.e.
//   depth >= 3, so the depth-1 rule excludes all of them without a path
//   blacklist. Belt-and-braces filters below still skip `private: true` and
//   placeholder names, in case that ever changes.
//
// - The version baseline is the highest *stable* (non-prerelease) version
//   published on npm, not `package.json`. npm is far ahead of the repo for
//   every one of these packages (e.g. common: package.json says 4.0.6, npm
//   has 5.0.3) and the npm `latest` dist-tag is not trustworthy either (it
//   points at prerelease versions for 3 of the 9 packages today). Reading
//   the full version list and filtering prereleases is the only baseline
//   that can't produce an EPUBLISHCONFLICT or roll a package backwards.
//
// - Version resolution is expressed as a monotonic max, not an if/else, so
//   that a later, more severe commit (e.g. a `BREAKING CHANGE:` footer added
//   after an earlier `feat:` was already committed as a minor bump) escalates
//   the version instead of being silently absorbed by an "already set" check:
//
//     candidate = increment(npmMax, cumulativeBumpAcrossFullRange)
//     next      = max(candidate, package.json version)
//     alreadySet = (next === package.json version)
//
// - This script never applies an rc suffix and never runs `npm publish`. It
//   only resolves the clean semver `next` version per package. The publish
//   job applies `x.y.z-rc.<run_id>` locally (never committed) and re-applies
//   the resolved version itself at checkout time via `npm version`, so it
//   does not depend on whether this script's commit-back step ran, was
//   skipped (dry-run), or was rejected by branch protection.

import { execFileSync } from 'node:child_process';
import { existsSync, readFileSync, readdirSync, appendFileSync } from 'node:fs';
import path from 'node:path';

const REPO_ROOT = process.cwd();

// ---------------------------------------------------------------------------
// CLI / env
// ---------------------------------------------------------------------------

const argv = process.argv.slice(2);
const flag = (name) => argv.includes(`--${name}`);
const opt = (name) => {
  const prefix = `--${name}=`;
  const hit = argv.find((a) => a.startsWith(prefix));
  return hit ? hit.slice(prefix.length) : undefined;
};

const DRY_RUN = flag('dry-run') || process.env.DRY_RUN === 'true';
const FORCE = flag('force') || process.env.FORCE === 'true';
const DEFAULT_BUMP = (opt('default-bump') || process.env.DEFAULT_BUMP || 'patch').toLowerCase();
const PACKAGES_FILTER = (opt('packages') || process.env.PACKAGES || '')
  .split(',')
  .map((s) => s.trim())
  .filter(Boolean);

const REF_NAME = process.env.GITHUB_REF_NAME || currentBranchName();
const EVENT_NAME = process.env.GITHUB_EVENT_NAME || 'workflow_dispatch';
const EVENT_PATH = process.env.GITHUB_EVENT_PATH;
const IS_MASTER = REF_NAME === 'master';

if (!['patch', 'minor', 'none'].includes(DEFAULT_BUMP)) {
  fail(`--default-bump/DEFAULT_BUMP must be patch|minor|none, got "${DEFAULT_BUMP}"`);
}

log(`plan-release: ref=${REF_NAME} event=${EVENT_NAME} master=${IS_MASTER} dryRun=${DRY_RUN} force=${FORCE} defaultBump=${DEFAULT_BUMP}`);
if (PACKAGES_FILTER.length) log(`plan-release: restricted to packages: ${PACKAGES_FILTER.join(', ')}`);

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function log(msg) {
  process.stderr.write(`${msg}\n`);
}

function fail(msg) {
  process.stderr.write(`::error::${msg}\n`);
  process.exit(1);
}

function git(args, opts = {}) {
  return execFileSync('git', args, { cwd: REPO_ROOT, encoding: 'utf8', ...opts }).trim();
}

function gitOrNull(args, opts = {}) {
  try {
    return git(args, opts);
  } catch {
    return null;
  }
}

// For a git command used purely as a predicate — `cat-file -e` exits 0 and prints
// NOTHING on success, so gitOrNull returns "" and `!""` is true, inverting the
// answer. Test the exit status instead. (Commands that print on success, like
// `rev-parse --verify`, are safe to test through gitOrNull.)
function gitOk(args, opts = {}) {
  try {
    git(args, opts);
    return true;
  } catch {
    return false;
  }
}

function currentBranchName() {
  return gitOrNull(['rev-parse', '--abbrev-ref', 'HEAD']) || 'HEAD';
}

// --- minimal semver (major.minor.patch[-prerelease]) -------------------------

function parseVersion(v) {
  const m = /^(\d+)\.(\d+)\.(\d+)(?:-([0-9A-Za-z.-]+))?/.exec(String(v).trim());
  if (!m) return null;
  return { major: +m[1], minor: +m[2], patch: +m[3], prerelease: m[4] || null };
}

function isStable(v) {
  const p = parseVersion(v);
  return !!p && !p.prerelease;
}

// Returns -1, 0, 1 for a<b, a==b, a>b. A release always outranks its own prerelease.
function compareVersions(a, b) {
  const pa = parseVersion(a);
  const pb = parseVersion(b);
  if (!pa || !pb) return 0;
  for (const k of ['major', 'minor', 'patch']) {
    if (pa[k] !== pb[k]) return pa[k] < pb[k] ? -1 : 1;
  }
  if (pa.prerelease === pb.prerelease) return 0;
  if (!pa.prerelease) return 1; // release beats prerelease
  if (!pb.prerelease) return -1;
  return pa.prerelease < pb.prerelease ? -1 : 1;
}

function maxVersion(a, b) {
  return compareVersions(a, b) >= 0 ? a : b;
}

function increment(version, bump) {
  const p = parseVersion(version);
  if (!p) fail(`Cannot parse version "${version}"`);
  if (bump === 'major') return `${p.major + 1}.0.0`;
  if (bump === 'minor') return `${p.major}.${p.minor + 1}.0`;
  return `${p.major}.${p.minor}.${p.patch + 1}`; // patch (and fallback)
}

const BUMP_RANK = { none: 0, patch: 1, minor: 2, major: 3 };
function strongerBump(a, b) {
  if (!a) return b;
  if (!b) return a;
  return BUMP_RANK[a] >= BUMP_RANK[b] ? a : b;
}

// ---------------------------------------------------------------------------
// Package discovery
// ---------------------------------------------------------------------------

const CONVENTIONAL_TYPES = new Set([
  'feat', 'fix', 'perf', 'refactor', 'revert',
  'docs', 'style', 'test', 'build', 'ci', 'chore',
]);

function discoverPackages() {
  const entries = readdirSync(REPO_ROOT, { withFileTypes: true })
    .filter((e) => e.isDirectory() && !e.name.startsWith('.'))
    .map((e) => e.name);

  const packages = [];
  for (const dir of entries) {
    const manifestPath = path.join(REPO_ROOT, dir, 'package.json');
    if (!existsSync(manifestPath)) continue;

    let manifest;
    try {
      manifest = JSON.parse(readFileSync(manifestPath, 'utf8'));
    } catch (e) {
      log(`plan-release: skipping ${dir} — unparseable package.json (${e.message})`);
      continue;
    }

    if (manifest.private) continue;
    if (!manifest.name || /__/.test(manifest.name)) continue; // placeholder templates

    if (PACKAGES_FILTER.length && !PACKAGES_FILTER.includes(dir) && !PACKAGES_FILTER.includes(manifest.name)) {
      continue;
    }

    packages.push({ dir, name: manifest.name, version: manifest.version, manifestPath });
  }
  return packages;
}

// ---------------------------------------------------------------------------
// Commit range + conventional-commit classification
// ---------------------------------------------------------------------------

function readEventBefore() {
  if (!EVENT_PATH || !existsSync(EVENT_PATH)) return null;
  try {
    const event = JSON.parse(readFileSync(EVENT_PATH, 'utf8'));
    const before = event.before;
    if (!before || /^0+$/.test(before)) return null;
    if (!gitOk(['cat-file', '-e', before])) return null; // not fetched / unborn
    return before;
  } catch {
    return null;
  }
}

function latestReleaseTag(pkgName) {
  const tags = gitOrNull(['tag', '--list', `${pkgName}@*`, '--merged', 'HEAD']);
  if (!tags) return null;
  const versions = tags
    .split('\n')
    .map((t) => t.trim())
    .filter(Boolean)
    .map((t) => t.slice(pkgName.length + 1))
    .filter((v) => parseVersion(v));
  if (!versions.length) return null;
  const best = versions.reduce((a, b) => (compareVersions(a, b) >= 0 ? a : b));
  return `${pkgName}@${best}`;
}

// Returns a git revision range expression suitable for `git log <range> -- dir`,
// or null to mean "no lower bound" (log from HEAD with no floor).
function resolveRange(pkgName) {
  if (!IS_MASTER) {
    const base = gitOrNull(['merge-base', 'origin/master', 'HEAD']) || gitOrNull(['merge-base', 'master', 'HEAD']);
    if (base) return `${base}..HEAD`;
    log(`plan-release: ${pkgName} — no merge-base with master found, falling back to HEAD~1..HEAD`);
    return gitOrNull(['rev-parse', 'HEAD~1']) ? 'HEAD~1..HEAD' : null;
  }

  const tag = latestReleaseTag(pkgName);
  if (tag) return `${tag}..HEAD`;

  if (EVENT_NAME === 'push') {
    const before = readEventBefore();
    if (before) return `${before}..HEAD`;
  }

  return gitOrNull(['rev-parse', 'HEAD~1']) ? 'HEAD~1..HEAD' : null;
}

const RS = '\x1e'; // record separator
const FS = '\x1f'; // field separator

function collectCommits(range, dir) {
  const revArgs = range ? [range] : ['HEAD'];
  const format = `${RS}%H${FS}%s${FS}%b`;
  const out = gitOrNull(['log', '--no-merges', `--format=${format}`, ...revArgs, '--', dir]);
  if (!out) return [];
  return out
    .split(RS)
    .map((rec) => rec.trim())
    .filter(Boolean)
    .map((rec) => {
      const [hash, subject = '', body = ''] = rec.split(FS);
      return { hash, subject, body };
    });
}

const SUBJECT_RE = /^([a-zA-Z]+)(?:\(([^)]*)\))?(!)?:\s*(.+)$/;
const BREAKING_RE = /BREAKING[ -]CHANGE:/;

function classifyCommits(commits) {
  let hasConventional = false;
  let cumBump = null;
  for (const { subject, body } of commits) {
    const m = SUBJECT_RE.exec(subject);
    if (!m) continue;
    const type = m[1].toLowerCase();
    if (!CONVENTIONAL_TYPES.has(type)) continue;

    hasConventional = true;
    const breaking = !!m[3] || BREAKING_RE.test(body);
    const bump = breaking ? 'major' : type === 'feat' ? 'minor' : 'patch';
    cumBump = strongerBump(cumBump, bump);
  }
  return { hasConventional, cumBump };
}

// ---------------------------------------------------------------------------
// npm baseline
// ---------------------------------------------------------------------------

// TEMPORARY per-package major-version ceilings. The next major line for each
// of these packages is being developed on the long-lived feature/aws-sdk-v3-again
// branch and is not ready to publish; this repo's automation must keep
// publishing the prior major until that work lands, or it will collide with
// versions already published from that branch. Remove (or raise) an entry
// once its package is cleared to move past the ceiling.
const VERSION_CEILINGS = {
  'leo-connector-common': 4,
  'leo-connector-elasticsearch': 2,
  'leo-connector-entity-table': 3,
  'leo-connector-mongo': 3,
  'leo-connector-mysql': 3,
  'leo-connector-oracle': 2,
  'leo-connector-postgres': 4,
  'leo-connector-redshift': 3,
  'leo-connector-sqlserver': 4,
};

function npmMaxStable(pkgName, fallback) {
  const ceiling = VERSION_CEILINGS[pkgName];
  try {
    const raw = execFileSync('npm', ['view', pkgName, 'versions', '--json'], { encoding: 'utf8' });
    const parsed = JSON.parse(raw);
    const versions = Array.isArray(parsed) ? parsed : [parsed];
    let stable = versions.filter(isStable);
    if (ceiling !== undefined) stable = stable.filter((v) => parseVersion(v).major <= ceiling);
    if (!stable.length) throw new Error(`no stable versions published${ceiling !== undefined ? ` at or below v${ceiling}` : ''}`);
    return stable.reduce((a, b) => (compareVersions(a, b) >= 0 ? a : b));
  } catch (e) {
    log(`plan-release: ${pkgName} — could not read npm versions (${e.message}); using package.json version ${fallback} as baseline`);
    return fallback;
  }
}

// ---------------------------------------------------------------------------
// Resolve each package
// ---------------------------------------------------------------------------

const packages = discoverPackages();
log(`plan-release: discovered ${packages.length} publishable package(s): ${packages.map((p) => p.dir).join(', ')}`);

const resolved = packages.map((pkg) => {
  const range = resolveRange(pkg.name);
  const commits = collectCommits(range, pkg.dir);
  const { hasConventional, cumBump } = classifyCommits(commits);

  let bump = hasConventional ? cumBump : (commits.length ? (DEFAULT_BUMP === 'none' ? null : DEFAULT_BUMP) : null);
  let included = bump !== null;
  if (!included && FORCE) {
    included = true;
    bump = DEFAULT_BUMP === 'none' ? 'patch' : DEFAULT_BUMP;
  }

  let npmMax = null;
  let candidate = null;
  let next = pkg.version;
  let alreadySet = true;
  let blockedReason = null;

  if (included) {
    npmMax = npmMaxStable(pkg.name, pkg.version);
    candidate = increment(npmMax, bump);

    const ceiling = VERSION_CEILINGS[pkg.name];
    next = maxVersion(candidate, pkg.version);

    if (ceiling !== undefined && parseVersion(next).major > ceiling) {
      // Guard the version that would actually be published, not just the bump
      // candidate. Two ways to cross the ceiling: the bump itself (usually a
      // BREAKING CHANGE commit), or a package.json already parked above it from
      // a stray manual bump — maxVersion carries that higher value straight into
      // the publish matrix, so checking `candidate` alone would wave it through.
      // Either way, exclude the package from this release rather than silently
      // publish into territory that's off-limits for now.
      blockedReason = `${next} would exceed the temporary v${ceiling} ceiling for ${pkg.name}`;
      log(`plan-release: ${pkg.name} — ${blockedReason}; excluding from this release`);
      included = false;
      bump = null;
      candidate = null;
      next = pkg.version;
      alreadySet = true;
    } else {
      alreadySet = compareVersions(pkg.version, candidate) >= 0;
    }
  }

  return {
    ...pkg,
    range: range || '(full history)',
    commitCount: commits.length,
    hasConventional,
    bump,
    included,
    npmMax,
    candidate,
    next,
    alreadySet,
    blockedReason,
    distTag: IS_MASTER ? 'latest' : 'rc',
  };
});

const released = resolved.filter((p) => p.included);

// ---------------------------------------------------------------------------
// Write version bumps + commit-back (skipped entirely in dry-run)
// ---------------------------------------------------------------------------

const changedDirs = [];

for (const pkg of released) {
  if (pkg.alreadySet) continue;
  log(`plan-release: ${pkg.name} ${pkg.version} -> ${pkg.next} (${pkg.bump})`);
  if (DRY_RUN) continue;

  execFileSync('npm', ['version', pkg.next, '--no-git-tag-version', '--allow-same-version'], {
    cwd: path.join(REPO_ROOT, pkg.dir),
    stdio: 'inherit',
  });
  changedDirs.push(pkg.dir);
}

let releaseSha = git(['rev-parse', 'HEAD']);

if (changedDirs.length && !DRY_RUN) {
  git(['config', 'user.name', 'github-actions[bot]']);
  git(['config', 'user.email', '41898282+github-actions[bot]@users.noreply.github.com']);

  for (const dir of changedDirs) {
    git(['add', path.join(dir, 'package.json'), path.join(dir, 'package-lock.json')]);
  }

  const bullets = released
    .filter((p) => changedDirs.includes(p.dir))
    .map((p) => `- ${p.name}@${p.next}`)
    .join('\n');
  git(['commit', '-m', `chore(release): version bump [skip ci]\n\n${bullets}`]);

  try {
    git(['push', 'origin', `HEAD:refs/heads/${REF_NAME}`]);
  } catch (e) {
    fail(
      `Failed to push the version-bump commit to "${REF_NAME}" (${e.message}). ` +
      `If branch protection is enabled, allow github-actions[bot] to push, or supply a PAT/App token to this job's checkout. ` +
      `Nothing was published — the run stops here by design.`,
    );
  }

  releaseSha = git(['rev-parse', 'HEAD']);
  log(`plan-release: pushed version bump commit ${releaseSha}`);
}

// ---------------------------------------------------------------------------
// Tags (master only)
// ---------------------------------------------------------------------------

if (IS_MASTER && !DRY_RUN && released.length) {
  const newTags = [];
  for (const pkg of released) {
    const tag = `${pkg.name}@${pkg.next}`;
    if (gitOrNull(['rev-parse', '-q', '--verify', `refs/tags/${tag}`])) {
      log(`plan-release: tag ${tag} already exists, skipping`);
      continue;
    }
    git(['tag', tag, releaseSha]);
    newTags.push(tag);
  }
  if (newTags.length) {
    try {
      git(['push', 'origin', ...newTags]);
      log(`plan-release: pushed tags: ${newTags.join(', ')}`);
    } catch (e) {
      fail(`Failed to push release tags (${e.message}).`);
    }
  }
}

// ---------------------------------------------------------------------------
// Outputs
// ---------------------------------------------------------------------------

const matrix = {
  include: released.map((p) => ({
    dir: p.dir,
    name: p.name,
    version: p.next,
    distTag: p.distTag,
    bump: p.bump,
  })),
};

const outputs = {
  matrix: JSON.stringify(matrix),
  'release-sha': releaseSha,
  'has-releases': String(released.length > 0),
};

if (process.env.GITHUB_OUTPUT) {
  for (const [key, value] of Object.entries(outputs)) {
    if (key === 'matrix') {
      appendFileSync(process.env.GITHUB_OUTPUT, `matrix<<PLAN_RELEASE_EOF\n${value}\nPLAN_RELEASE_EOF\n`);
    } else {
      appendFileSync(process.env.GITHUB_OUTPUT, `${key}=${value}\n`);
    }
  }
} else {
  log(`plan-release: GITHUB_OUTPUT not set, printing outputs instead:`);
  console.log(JSON.stringify(outputs, null, 2));
}

const summaryLines = [
  '| Package | Included | Bump | npm max | Next version | Already set | Commits |',
  '|---|---|---|---|---|---|---|',
  ...resolved.map((p) =>
    `| ${p.name} | ${p.included ? 'yes' : 'no'} | ${p.bump || '-'} | ${p.npmMax || '-'} | ${p.included ? p.next : '-'} | ${p.included ? (p.alreadySet ? 'yes' : 'no') : '-'} | ${p.commitCount} |`,
  ),
];

const blocked = resolved.filter((p) => p.blockedReason);
if (blocked.length) {
  summaryLines.push('', '**Blocked by version ceiling:**', ...blocked.map((p) => `- ${p.name}: ${p.blockedReason}`));
}

if (process.env.GITHUB_STEP_SUMMARY) {
  appendFileSync(process.env.GITHUB_STEP_SUMMARY, `## Release plan (${REF_NAME})\n\n${summaryLines.join('\n')}\n`);
} else {
  log(summaryLines.join('\n'));
}

log(`plan-release: ${released.length} package(s) will be released${DRY_RUN ? ' (dry run — no commit/push/tag performed)' : ''}`);
