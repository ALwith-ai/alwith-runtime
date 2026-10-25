import { expect, test } from "bun:test"

import { stageRuntime } from "../../packages/runtime/stage.js"

test("rejects Windows and Linux ARM64 before resolving any Runtime package", () => {
  for (const target of ["aarch64-pc-windows-msvc", "aarch64-unknown-linux-gnu"]) {
    expect(() => stageRuntime(target, "/unused", "/unused")).toThrow("Unsupported Runtime target")
  }
})
