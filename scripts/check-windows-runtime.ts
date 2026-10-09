import { execFileSync } from "node:child_process"

export function assertStaticWindowsCrt(report: string): void {
  if (!/^Format: COFF-(x86-64|ARM64)\r?$/m.test(report)) {
    throw new Error("Expected an x64 or ARM64 Windows binary from llvm-readobj")
  }
  // --coff-imports includes both Import and DelayImport entries.
  const dependencies = Array.from(report.matchAll(/^\s*Name: (\S+\.dll)\s*$/gim), match => match[1]!)
  if (dependencies.length === 0) throw new Error("No Windows DLL imports found; refusing an unchecked binary")
  const dynamicCrt = dependencies.filter(name =>
    /^(vcruntime|msvcp|msvcr|concrt|vccorlib|ucrtbase|api-ms-win-crt-|ext-ms-win-crt-).*\.dll$/i.test(name)
  )
  if (dynamicCrt.length > 0) {
    throw new Error(
      `Windows Runtime requires dynamic CRT DLLs: ${dynamicCrt.join(", ")}. Rebuild with -C target-feature=+crt-static.`
    )
  }
}

export function checkWindowsRuntime(binary: string, readobj: string = "llvm-readobj"): void {
  const report = execFileSync(readobj, ["--coff-imports", binary], { encoding: "utf8", stdio: "pipe" })
  assertStaticWindowsCrt(report)
}

if (import.meta.main) {
  const [binary, readobj = "llvm-readobj"] = process.argv.slice(2)
  if (!binary) throw new Error("Usage: bun scripts/check-windows-runtime.ts <binary.exe> [llvm-readobj]")
  checkWindowsRuntime(binary, readobj)
  console.log(`${binary}: no dynamic CRT imports`)
}
