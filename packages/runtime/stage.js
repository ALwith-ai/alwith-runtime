import { createRequire } from "node:module"
import { createHash } from "node:crypto"
import { chmodSync, cpSync, mkdirSync, readFileSync } from "node:fs"
import { dirname, join } from "node:path"
const require = createRequire(import.meta.url)
const targets = {
  "aarch64-apple-darwin": "darwin-arm64",
  "aarch64-unknown-linux-gnu": "linux-arm64",
  "x86_64-unknown-linux-gnu": "linux-x64",
  "aarch64-pc-windows-msvc": "win32-arm64",
  "x86_64-pc-windows-msvc": "win32-x64"
}
export function stageRuntime(target, destination, licenses) {
  const platform = targets[target]
  if (!platform) throw new Error(`Unsupported Runtime target: ${target}`)
  const name = `@alwith/runtime-${platform}`
  let root
  try {
    root = dirname(require.resolve(`${name}/package.json`))
  } catch {
    throw new Error(
      `Missing ${name}. Install optional dependencies for the build target (Bun: bun install --os='*' --cpu='*').`
    )
  }
  const pin = JSON.parse(readFileSync(join(root, "artifact.json"), "utf8"))
  const version = JSON.parse(readFileSync(new URL("./package.json", import.meta.url), "utf8")).version
  const expectedFile = platform.startsWith("win32") ? "alwith-runtime.exe" : "alwith-runtime"
  if (pin.version !== version || pin.target !== target || pin.binary !== expectedFile)
    throw new Error("Runtime artifact identity mismatch")
  const binary = join(root, expectedFile)
  if (createHash("sha256").update(readFileSync(binary)).digest("hex") !== pin.sha256)
    throw new Error("Runtime binary checksum mismatch")
  mkdirSync(dirname(destination), { recursive: true })
  cpSync(binary, destination)
  if (!platform.startsWith("win32")) chmodSync(destination, 0o755)
  const legal = join(licenses, "alwith-runtime")
  mkdirSync(legal, { recursive: true })
  for (const file of ["LICENSE", "THIRD_PARTY_NOTICES.md", "licenses"])
    cpSync(join(root, file), join(legal, file), { recursive: true })
  return destination
}
