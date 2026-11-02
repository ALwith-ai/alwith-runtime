# @alwith/runtime

The closed ALwith Runtime process, distributed as npm platform packages: `bun install` (or npm) pulls the
one binary for your platform via `optionalDependencies`; nothing else is downloaded, no credentials.

At build time:

```js
import { stageRuntime } from "@alwith/runtime/stage"
stageRuntime("aarch64-apple-darwin", "src-tauri/binaries/alwith-runtime-aarch64-apple-darwin", "src-tauri/resources/licenses")
```

The binary is verified against the sha256 pinned in the platform package before it is copied; the
Runtime's LICENSE and third-party notices land next to your app's other licence files.

Cross builds must install the optional package for the target too: `bun install --os='*' --cpu='*'`.
Platforms: macOS arm64 and Windows x64. The protocol the binary speaks is
documented in the `alwith-api` repository (PROTOCOL.md); talk to it with `@alwith/api`.
