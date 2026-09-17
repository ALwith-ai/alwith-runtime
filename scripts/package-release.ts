// 打一个平台的发布包:alwith-runtime-<target>.tar.gz,里面是二进制、LICENSE、THIRD_PARTY_NOTICES.md、
// licenses/(每个第三方 crate 的许可原文)、BUILD.json(版本、目标、源码 revision)。
// 应用的 stage 脚本要求这四样都在包里,缺一个就拒装。
//
// 用法:bun scripts/package-release.ts <rust-target-triple>
// 前提:cargo build --release --locked -p alwith-runtime --target <triple> 已经跑完。
import { cpSync, existsSync, mkdirSync, readdirSync, readFileSync, rmSync, writeFileSync } from "node:fs"
import { dirname, join, resolve } from "node:path"
import { $ } from "bun"

const root = resolve(import.meta.dirname, "..")
const target = process.argv[2]
if (!target) throw new Error("Usage: bun scripts/package-release.ts <rust-target-triple>")
const exe = target.includes("windows") ? ".exe" : ""
const binary = join(root, "target", target, "release", `alwith-runtime${exe}`)
if (!existsSync(binary)) throw new Error(`Build first: ${binary} is missing`)
const version = (readFileSync(join(root, "Cargo.toml"), "utf8").match(/^version = "(.+)"$/m) ?? [])[1]
if (!version) throw new Error("workspace version not found in Cargo.toml")

const stage = join(root, "dist", "stage")
rmSync(stage, { recursive: true, force: true })
mkdirSync(stage, { recursive: true })
cpSync(binary, join(stage, `alwith-runtime${exe}`))
cpSync(join(root, "LICENSE"), join(stage, "LICENSE"))

// 第三方许可:按这个平台实际链进来的 crate 收,原文一份不少;没有许可文件的 crate 用已知的上游位置补,
// 补不到就失败 —— 少一份就是分发违规,不能静默。
const metadata = await $`cargo metadata --format-version 1 --filter-platform ${target}`.cwd(root).json()
const notices = ["# Runtime third-party notices", "", "ALwith implementations remain proprietary. Third-party software retains its own licences.", ""]
const known: Record<string, string> = {
  "https://github.com/whatisinternet/inflector": "LICENSE.md",
  "https://github.com/knurling-rs/defmt": "LICENSE-MIT",
  "https://github.com/madsmtm/objc2": "LICENSE.md",
  "https://github.com/specta-rs/specta": "LICENSE"
}
const packages = (metadata.packages as Array<{ name: string; version: string; source: string | null; manifest_path: string; license: string | null; license_file: string | null; repository: string | null }>)
  .filter(pkg => pkg.source)
  .sort((a, b) => a.name.localeCompare(b.name) || a.version.localeCompare(b.version))
for (const pkg of packages) {
  const directory = dirname(pkg.manifest_path)
  const files = readdirSync(directory).filter(name => /^(LICENSE|LICENCE|COPYING|NOTICE)([.-]|$)/i.test(name))
  if (pkg.license_file && !files.includes(pkg.license_file)) files.push(pkg.license_file)
  const dest = join(stage, "licenses", `${pkg.name}-${pkg.version}`)
  mkdirSync(dest, { recursive: true })
  if (files.length === 0) {
    const name = pkg.repository ? known[pkg.repository] : undefined
    if (!name) throw new Error(`Missing licence text for ${pkg.name}@${pkg.version} (${pkg.repository ?? "no repository"})`)
    const revision = JSON.parse(readFileSync(join(directory, ".cargo_vcs_info.json"), "utf8")).git.sha1
    if (!/^[a-f0-9]{40}$/.test(revision)) throw new Error(`Invalid upstream revision for ${pkg.name}`)
    const url = `${pkg.repository!.replace("github.com", "raw.githubusercontent.com")}/${revision}/${name}`
    const response = await fetch(url)
    if (!response.ok) throw new Error(`Licence ${url}: HTTP ${response.status}`)
    writeFileSync(join(dest, name), await response.text())
    if (pkg.repository === "https://github.com/madsmtm/objc2") {
      const mit = await fetch("https://raw.githubusercontent.com/madsmtm/objc2/1adc004a387af7491d30209063c9570cddb75981/LICENSE-MIT.txt")
      if (!mit.ok) throw new Error("Failed to obtain objc2 MIT licence text")
      writeFileSync(join(dest, "LICENSE-MIT.txt"), await mit.text())
    }
    notices.push(`- ${pkg.name}@${pkg.version}: ${pkg.license}; licenses/${pkg.name}-${pkg.version}/; source: ${url}`)
    continue
  }
  for (const name of files) cpSync(join(directory, name), join(dest, name), { recursive: true })
  notices.push(`- ${pkg.name}@${pkg.version}: ${pkg.license ?? "see licence files"}; licenses/${pkg.name}-${pkg.version}/`)
}
writeFileSync(join(stage, "THIRD_PARTY_NOTICES.md"), notices.join("\n") + "\n")

const sourceRevision = (await $`git rev-parse HEAD`.cwd(root).text()).trim()
writeFileSync(join(stage, "BUILD.json"), JSON.stringify({ version, target, sourceRevision }, null, 2) + "\n")

const archive = join(root, "dist", `alwith-runtime-${target}.tar.gz`)
await $`tar -C ${stage} -czf ${archive} alwith-runtime${exe} LICENSE THIRD_PARTY_NOTICES.md licenses BUILD.json`
rmSync(stage, { recursive: true, force: true })
console.log(`${archive}: alwith-runtime ${version} for ${target}, ${packages.length} third-party crates`)
