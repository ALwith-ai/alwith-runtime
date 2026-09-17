/**
 * Build-time only. Copies the installed, checksum-verified Runtime binary for `target` (a Rust
 * target triple) to `destination` (the exact file path your bundler expects, e.g. a Tauri sidecar
 * `binaries/alwith-runtime-<triple>`), and its LICENSE, THIRD_PARTY_NOTICES.md and licenses/ under
 * `licenses`/alwith-runtime. Returns `destination`.
 */
export declare function stageRuntime(target: string, destination: string, licenses: string): string;
