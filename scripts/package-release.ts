// Builds the npm platform package for one platform: dist/npm/<os>-<cpu>/, holding the binary,
// artifact.json (version, target, sha256), LICENSE, THIRD_PARTY_NOTICES.md, licenses/ (the license
// text of every third-party crate) and package.json.
// The @alwith/runtime main package (packages/runtime) points at these five platform packages through
// optionalDependencies; an app's bun install installs only its own platform's, and at build time
// stageRuntime() verifies the sha and copies it into the sidecar.
//
// Usage: bun scripts/package-release.ts <rust-target-triple>
// Prerequisite: cargo build --release --locked -p alwith-runtime --target <triple> has already finished.
import { cpSync, existsSync, mkdirSync, readdirSync, readFileSync, rmSync, writeFileSync } from "node:fs"
import { dirname, join, resolve } from "node:path"

import { $ } from "bun"

import { checkWindowsRuntime } from "./check-windows-runtime"

const platforms: Record<string, [string, string]> = {
  "aarch64-apple-darwin": ["darwin", "arm64"],
  "x86_64-unknown-linux-gnu": ["linux", "x64"],
  "aarch64-unknown-linux-gnu": ["linux", "arm64"],
  "x86_64-pc-windows-msvc": ["win32", "x64"],
  "aarch64-pc-windows-msvc": ["win32", "arm64"]
}

const root = resolve(import.meta.dirname, "..")
const target = process.argv[2]
if (!target || !platforms[target])
  throw new Error(`Usage: bun scripts/package-release.ts <${Object.keys(platforms).join("|")}>`)
const [os, cpu] = platforms[target]
const exe = os === "win32" ? ".exe" : ""
const binary = join(root, "target", target, "release", `alwith-runtime${exe}`)
if (!existsSync(binary)) throw new Error(`Build first: ${binary} is missing`)
// Check the actual artifact before staging: runner-installed DLLs can hide dynamic CRT
// dependencies.
if (os === "win32") checkWindowsRuntime(binary)
const version = (readFileSync(join(root, "Cargo.toml"), "utf8").match(/^version = "(.+)"$/m) ?? [])[1]
if (!version) throw new Error("workspace version not found in Cargo.toml")

const stage = join(root, "dist", "npm", `${os}-${cpu}`)
rmSync(stage, { recursive: true, force: true })
mkdirSync(stage, { recursive: true })
cpSync(binary, join(stage, `alwith-runtime${exe}`))
cpSync(join(root, "LICENSE"), join(stage, "LICENSE"))

// Third-party licenses: collected for the crates actually linked for this platform, every license text
// included; a crate without a license file is filled in from a known upstream location, and the script
// fails if that is not possible -- a missing one is a distribution violation and must not pass silently.
const metadata = (await $`cargo metadata --format-version 1 --filter-platform ${target}`.cwd(root).json()) as {
  packages: Array<{
    name: string
    version: string
    source: string | null
    manifest_path: string
    license: string | null
    license_file: string | null
    repository: string | null
  }>
}
const notices = [
  "# Runtime third-party notices",
  "",
  "ALwith implementations remain proprietary. Third-party software retains its own licences.",
  ""
]
const known: Record<string, string> = {
  "https://github.com/whatisinternet/inflector": "LICENSE.md",
  "https://github.com/knurling-rs/defmt": "LICENSE-MIT",
  "https://github.com/madsmtm/objc2": "LICENSE.md",
  "https://github.com/specta-rs/specta": "LICENSE"
}
const packages = metadata.packages
  .filter((pkg) => pkg.source)
  .sort((a, b) => a.name.localeCompare(b.name) || a.version.localeCompare(b.version))
for (const pkg of packages) {
  const directory = dirname(pkg.manifest_path)
  const files = readdirSync(directory).filter((name) => /^(LICENSE|LICENCE|COPYING|NOTICE)([.-]|$)/i.test(name))
  if (pkg.license_file && !files.includes(pkg.license_file)) files.push(pkg.license_file)
  const dest = join(stage, "licenses", `${pkg.name}-${pkg.version}`)
  mkdirSync(dest, { recursive: true })
  if (files.length === 0) {
    const name = pkg.repository ? known[pkg.repository] : undefined
    if (!name)
      throw new Error(`Missing licence text for ${pkg.name}@${pkg.version} (${pkg.repository ?? "no repository"})`)
    const revision = (
      JSON.parse(readFileSync(join(directory, ".cargo_vcs_info.json"), "utf8")) as { git: { sha1: string } }
    ).git.sha1
    if (!/^[a-f0-9]{40}$/.test(revision)) throw new Error(`Invalid upstream revision for ${pkg.name}`)
    const url = `${pkg.repository!.replace("github.com", "raw.githubusercontent.com")}/${revision}/${name}`
    const response = await fetch(url)
    if (!response.ok) throw new Error(`Licence ${url}: HTTP ${response.status}`)
    writeFileSync(join(dest, name), await response.text())
    if (pkg.repository === "https://github.com/madsmtm/objc2") {
      const mit = await fetch(
        "https://raw.githubusercontent.com/madsmtm/objc2/1adc004a387af7491d30209063c9570cddb75981/LICENSE-MIT.txt"
      )
      if (!mit.ok) throw new Error("Failed to obtain objc2 MIT licence text")
      writeFileSync(join(dest, "LICENSE-MIT.txt"), await mit.text())
    }
    notices.push(`- ${pkg.name}@${pkg.version}: ${pkg.license}; licenses/${pkg.name}-${pkg.version}/; source: ${url}`)
    continue
  }
  for (const name of files) cpSync(join(directory, name), join(dest, name), { recursive: true })
  notices.push(
    `- ${pkg.name}@${pkg.version}: ${pkg.license ?? "see licence files"}; licenses/${pkg.name}-${pkg.version}/`
  )
}
writeFileSync(join(stage, "THIRD_PARTY_NOTICES.md"), notices.join("\n") + "\n")

const sourceRevision = (await $`git rev-parse HEAD`.cwd(root).text()).trim()
const sha256 = new Bun.CryptoHasher("sha256").update(readFileSync(join(stage, `alwith-runtime${exe}`))).digest("hex")
writeFileSync(
  join(stage, "artifact.json"),
  JSON.stringify({ version, target, binary: `alwith-runtime${exe}`, sha256, sourceRevision }, null, 2) + "\n"
)
writeFileSync(
  join(stage, "package.json"),
  JSON.stringify(
    {
      name: `@alwith/runtime-${os}-${cpu}`,
      version,
      os: [os],
      cpu: [cpu],
      description: `ALwith Runtime binary for ${target}; installed by @alwith/runtime, staged by its stageRuntime()`,
      license: "SEE LICENSE IN LICENSE",
      files: [`alwith-runtime${exe}`, "artifact.json", "LICENSE", "THIRD_PARTY_NOTICES.md", "licenses"],
      publishConfig: { access: "public", registry: "https://registry.npmjs.org" },
      repository: { type: "git", url: "git+https://github.com/ALwith-ai/alwith-runtime.git" }
    },
    null,
    2
  ) + "\n"
)
console.log(`${stage}: @alwith/runtime-${os}-${cpu}@${version}, ${packages.length} third-party crates`)
