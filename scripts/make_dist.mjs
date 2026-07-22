#!/usr/bin/env bun
// Build downloadable thinDB release bundles for the major platforms.
//
// Each bundle is self-contained: the thindb-server binary plus the exact
// matching Zig toolchain in a `zig/` directory beside it (the server compiles
// Zig UDF/TVF functions at runtime and hard-gates on the compiler version it
// was built with — server and toolchain must always ship together).
//
//   bun scripts/make_dist.mjs                 # all five targets
//   bun scripts/make_dist.mjs --targets linux-x86_64,windows-x86_64
//
// Outputs to .dist/:  thindb-<version>-<platform>.{tar.gz,zip} + SHA256SUMS
// Toolchain downloads are cached in .dist-cache/ (~50 MB per platform).
//
// Requires: zig (the pinned version), git, curl, GNU tar + gzip (Git Bash /
// Linux / macOS), bsdtar for the Windows zip (ships with Windows 10+).

import { spawnSync } from "node:child_process";
import { existsSync, mkdirSync, rmSync, copyFileSync, writeFileSync, statSync, readdirSync } from "node:fs";
import { join, resolve } from "node:path";

const REPO = resolve(import.meta.dir, "..");
const DIST = join(REPO, ".dist");
const CACHE = join(REPO, ".dist-cache");
const IS_WINDOWS_HOST = process.platform === "win32";
const BSDTAR = IS_WINDOWS_HOST ? "C:\\Windows\\System32\\tar.exe" : "bsdtar";

const TARGETS = [
  { triple: "x86_64-linux-gnu",  plat: "linux-x86_64",   zigOs: "linux",   zigArch: "x86_64",  ext: "tar.xz", archive: "tar.gz", exe: "thindb-server" },
  { triple: "aarch64-linux-gnu", plat: "linux-aarch64",  zigOs: "linux",   zigArch: "aarch64", ext: "tar.xz", archive: "tar.gz", exe: "thindb-server" },
  { triple: "x86_64-macos",      plat: "macos-x86_64",   zigOs: "macos",   zigArch: "x86_64",  ext: "tar.xz", archive: "tar.gz", exe: "thindb-server" },
  { triple: "aarch64-macos",     plat: "macos-aarch64",  zigOs: "macos",   zigArch: "aarch64", ext: "tar.xz", archive: "tar.gz", exe: "thindb-server" },
  { triple: "x86_64-windows",    plat: "windows-x86_64", zigOs: "windows", zigArch: "x86_64",  ext: "zip",    archive: "zip",    exe: "thindb-server.exe" },
];

function run(cmd, args, opts = {}) {
  const r = spawnSync(cmd, args, { cwd: REPO, encoding: "utf8", stdio: ["ignore", "pipe", "pipe"], ...opts });
  if (r.status !== 0) {
    throw new Error(`${cmd} ${args.join(" ")} failed (${r.status}):\n${r.stderr || r.stdout}`);
  }
  return (r.stdout || "").trim();
}

function fmtMB(bytes) {
  return (bytes / 1024 / 1024).toFixed(1) + " MB";
}

// msys GNU tar mishandles backslashed Windows paths even with --force-local.
function slash(p) {
  return p.replaceAll("\\", "/");
}

// --- version pins ----------------------------------------------------------
const zigVersion = run("zig", ["version"]);
let gitVersion;
try {
  gitVersion = run("git", ["describe", "--tags", "--always", "--dirty"]);
} catch {
  gitVersion = "unknown";
}
// Release builds pass THINDB_VERSION (x.y.z, computed by the workflow from
// the VERSION file + commit distance); local/dev builds fall back to the git
// describe string.
const version = (process.env.THINDB_VERSION || gitVersion).replace(/[^A-Za-z0-9._-]/g, "-");

const only = (() => {
  const i = process.argv.indexOf("--targets");
  if (i === -1) return null;
  return new Set(process.argv[i + 1].split(","));
})();
const targets = TARGETS.filter((t) => !only || only.has(t.plat) || only.has(t.triple));
if (targets.length === 0) {
  console.error(`no targets matched --targets; known: ${TARGETS.map((t) => t.plat).join(", ")}`);
  process.exit(1);
}

console.log(`thinDB dist: version=${version} zig=${zigVersion} targets=${targets.map((t) => t.plat).join(",")}`);
mkdirSync(DIST, { recursive: true });
mkdirSync(CACHE, { recursive: true });

// --- phase 1: cross-compile the server -------------------------------------
for (const t of targets) {
  console.log(`[build] ${t.triple}`);
  run("zig", ["build", "dist", `-Dtarget=${t.triple}`, "-Doptimize=ReleaseFast", "-Dstrip=true", "-p", join(DIST, "build", t.triple)], { stdio: ["ignore", "inherit", "inherit"] });
}

// --- phase 2: fetch matching toolchains (cached) ----------------------------
for (const t of targets) {
  const name = `zig-${t.zigArch}-${t.zigOs}-${zigVersion}.${t.ext}`;
  t.toolchainArchive = join(CACHE, name);
  if (existsSync(t.toolchainArchive)) {
    console.log(`[toolchain] cached ${name}`);
    continue;
  }
  const url = `https://ziglang.org/download/${zigVersion}/${name}`;
  console.log(`[toolchain] fetching ${url}`);
  run("curl", ["-fL", "--retry", "3", "-o", t.toolchainArchive + ".part", url]);
  run(IS_WINDOWS_HOST ? "cmd" : "mv", IS_WINDOWS_HOST
    ? ["/c", "move", "/y", t.toolchainArchive + ".part", t.toolchainArchive]
    : [t.toolchainArchive + ".part", t.toolchainArchive]);
}

// --- phase 3: assemble bundles ---------------------------------------------
const STAGE = join(DIST, "stage");
rmSync(STAGE, { recursive: true, force: true });
for (const t of targets) {
  t.bundleName = `thindb-${version}-${t.plat}`;
  const bundle = join(STAGE, t.bundleName);
  mkdirSync(join(bundle, "zig"), { recursive: true });

  copyFileSync(join(DIST, "build", t.triple, "bin", t.exe), join(bundle, t.exe));

  // The toolchain archives contain a zig-<arch>-<os>-<ver>/ top dir; strip it.
  if (t.ext === "zip") {
    run(BSDTAR, ["-xf", t.toolchainArchive, "--strip-components=1", "-C", join(bundle, "zig")]);
  } else {
    run("tar", ["--force-local", "-xf", slash(t.toolchainArchive), "--strip-components=1", "-C", slash(join(bundle, "zig"))]);
  }

  writeFileSync(join(bundle, "README.txt"),
`thinDB ${version} (${t.plat}, ${gitVersion})
==============================

Quick start:

    ${t.plat.startsWith("windows") ? "thindb-server.exe" : "./thindb-server"} --data-dir ./data --mysql-port 3306

Connect with any MySQL client:  mysql -h 127.0.0.1 -P 3306

The bundled zig/ directory is the compiler the server uses to build Zig
UDF/TVF functions at runtime. It must stay beside the server binary (or point
THINDB_ZIG_PATH at a zig executable of the exact same version, ${zigVersion}).
The server refuses mismatched compiler versions by design: compiled functions
share in-memory structs with the server, so only the identical toolchain is
safe.

Built from thinDB ${gitVersion}, Zig ${zigVersion}.
`);
  console.log(`[stage] ${t.bundleName}`);
}

// --- phase 4: archive -------------------------------------------------------
const sums = [];
for (const t of targets) {
  const out = join(DIST, `${t.bundleName}.${t.archive}`);
  rmSync(out, { force: true });
  console.log(`[pack] ${t.bundleName}.${t.archive}`);
  if (t.archive === "zip") {
    run(BSDTAR, ["-a", "-cf", out, "-C", STAGE, t.bundleName]);
  } else {
    // Two passes: base entries read-only, then re-append the two executables
    // with exec bits — NTFS staging loses POSIX modes, so set them explicitly
    // (archive readers honor the last entry for a duplicated path).
    const tarPath = out.replace(/\.gz$/, "");
    run("tar", ["--force-local", "-cf", slash(tarPath), "--owner=0", "--group=0", "--mode=u+rw,go+r,a+X", "-C", slash(STAGE), t.bundleName]);
    run("tar", ["--force-local", "-rf", slash(tarPath), "--owner=0", "--group=0", "--mode=a+rx,u+w", "-C", slash(STAGE),
      `${t.bundleName}/${t.exe}`, `${t.bundleName}/zig/zig`]);
    rmSync(out, { force: true });
    run("gzip", ["-9", "-f", slash(tarPath)]);
  }
  const sha = run("sha256sum", [slash(out)]).split(/\s+/)[0];
  sums.push(`${sha}  ${t.bundleName}.${t.archive}`);
  console.log(`       ${fmtMB(statSync(out).size)}  sha256=${sha.slice(0, 12)}…`);
}
writeFileSync(join(DIST, "SHA256SUMS"), sums.join("\n") + "\n");

rmSync(STAGE, { recursive: true, force: true });
console.log(`\ndone → ${DIST}`);
for (const f of readdirSync(DIST)) {
  if (f === "build" || f === "stage") continue;
  console.log(`  ${f}  ${fmtMB(statSync(join(DIST, f)).size)}`);
}
