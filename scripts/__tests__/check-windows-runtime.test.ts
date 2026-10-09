import { expect, test } from "bun:test"
import { assertStaticWindowsCrt, checkWindowsRuntime } from "../check-windows-runtime"

function importsReport(format: string, imports: string[], delayed: string[] = []): string {
  return [
    "File: alwith-runtime.exe",
    `Format: ${format}`,
    "AddressSize: 64bit",
    ...imports.map(
      name => `Import {\n  Name: ${name}\n  ImportLookupTableRVA: 0x1000\n  ImportAddressTableRVA: 0x2000\n}`
    ),
    ...delayed.map(name => `DelayImport {\n  Name: ${name}\n  Attributes: 0x1\n  ModuleHandle: 0x3000\n}`)
  ].join("\n")
}

test.each(["COFF-x86-64", "COFF-ARM64"])("accepts Windows system imports for %s", format => {
  expect(() =>
    assertStaticWindowsCrt(importsReport(format, ["KERNEL32.dll", "ws2_32.dll", "bcryptprimitives.dll"]))
  ).not.toThrow()
})

test.each([
  "VCRUNTIME140.dll",
  "vcruntime140_1.dll",
  "VCRUNTIME140D.dll",
  "MSVCP140.dll",
  "msvcp140_atomic_wait.dll",
  "MSVCR120.dll",
  "msvcrt.dll",
  "CONCRT140.dll",
  "vccorlib140.dll",
  "ucrtbase.dll",
  "ucrtbased.dll",
  "api-ms-win-crt-runtime-l1-1-0.dll",
  "ext-ms-win-crt-runtime-l1-1-0.dll"
])("rejects dynamic CRT dependency %s", dependency => {
  expect(() => assertStaticWindowsCrt(importsReport("COFF-x86-64", ["KERNEL32.dll", dependency]))).toThrow(dependency)
})

test("rejects delayed CRT imports too", () => {
  expect(() => assertStaticWindowsCrt(importsReport("COFF-ARM64", ["KERNEL32.dll"], ["MSVCP140.dll"]))).toThrow(
    "MSVCP140.dll"
  )
})

test("accepts Windows line endings", () => {
  expect(() =>
    assertStaticWindowsCrt(importsReport("COFF-ARM64", ["KERNEL32.dll"]).replaceAll("\n", "\r\n"))
  ).not.toThrow()
})

test("fails if the inspection tool is missing", () => {
  expect(() => checkWindowsRuntime("alwith-runtime.exe", `${import.meta.dir}/missing-llvm-readobj`)).toThrow("ENOENT")
})

test("propagates inspection tool failure", () => {
  expect(() => checkWindowsRuntime("alwith-runtime.exe", process.execPath)).toThrow("Command failed")
})

test.each(["", "tool failed", "Format: ELF64-x86-64\n  Name: KERNEL32.dll", "Format: COFF-x86-64"])(
  "rejects unreadable or empty import reports: %s",
  report => {
    expect(() => assertStaticWindowsCrt(report)).toThrow()
  }
)
