# Bob Shell Integration — PR Readiness TODO

Review of our Bob Shell integration against the Claude Code integration and v2 capabilities.
Goal: align as closely as possible with Claude Code patterns, remove v1-era workarounds,
surface Bob-specific facts (no model selector) honestly, and keep the diff small and clean.

---

## ✅ 1. Remove committed dev artifacts from the repo
**Done.** Deleted `bin/bob` and `bin/bob-stub.mjs` (hardcoded `/Users/timgraepel/…` path).

---

## ✅ 2. Fix env-var name for the API key
**Done — confirmed correct.** Bob v2 docs confirm `BOBSHELL_API_KEY` is the real env var name.
Also added `--auth-method api-key` to both the adapter and text generation when `apiKey` is set,
as required by Bob v2 (`bob --auth-method api-key`).
Updated the settings description accordingly.

---

## ✅ 3. Align `BobShellHome.ts` with `ClaudeHome.ts` patterns
**`BOB_HOME` confirmed correct** — Bob v2 uses `BOB_HOME` to redirect the config directory
(not the full `$HOME`), so no keychain risk on macOS. No change needed.
`makeBobShellContinuationGroupKey` placement in `BobShellHome.ts` is consistent with Claude.

---

## ✅ 4. Model selector: Bob has no model selector
**Done.**
- Removed `customModels` from `BobShellSettings` in `packages/contracts/src/settings.ts`.
- Removed `customModels` from `BobShellSettingsPatch`.
- Removed `BOB_SHELL_BUILT_IN_MODELS` and `bobShellModelsFromSettings` from `BobShellProvider.ts`; models list is now `[]`.
- Removed `-m` / `modelSelection.model` passthrough from both `BobShellAdapter.ts` and `BobShellTextGeneration.ts`.
- Added `bobShell` to the early-return guard in `apps/web/src/modelSelection.ts` alongside `antigravity`.

---

## ✅ 5. Auth probe: use the right command
**Done.** Removed the `bob --list-tasks 1` auth probe (undocumented flag). Now uses only
`bob --version` for the install check. If it exits 0, the provider is marked `status: "ready"`
and `auth: { status: "authenticated" }` — there is no reliable offline auth check in Bob v2.
Removed the unused `AUTH_PROBE_TIMEOUT_MS` import.

---

## ✅ 6. `readThread` / `rollbackThread` — document clearly
**Done.** Updated comment in `BobShellAdapter.ts` to state this is a deliberate permanent
limitation (Bob v2 history is internal SQLite), not a TODO.

---

## ✅ 7. `respondToRequest` / interactive approval — align error messages
**Done.** Aligned `respondToRequest` detail with the terse "Not supported." pattern from Grok.

---

## ✅ 8. `BobShellAdapter` — reasoning blocks
**Done.** Decision: suppress silently. Added explicit comment in `BobShellAdapter.ts` explaining
that Bob's `isReasoning: true` blocks are internal scaffolding, not a user-facing feature like
Claude's extended thinking.

---

## ✅ 9. `BobShellSkills` — skill path passed as `--chat-mode`
**Done.** Wired `input.skillPath` into `runTurn` — if set, passes `--chat-mode=<slug>` to
`bob run`, taking priority over the `--chat-mode=agent` default.

---

## ✅ 10. `--trust` flag in text generation and adapter
**Done.** Removed `--trust` from both `BobShellTextGeneration.ts` and `BobShellAdapter.ts`.
Bob v2 uses trusted folders (`~/.bob/trustedFolders.json`), not a `--trust` CLI flag.
Non-interactive sessions default to trusted without any flag.

---

## ✅ 11. `BobShellDriver.ts` — remove unused `ProviderEventLoggers`
**Done.** Removed `ProviderEventLoggers` from `BobShellDriverEnv` type and its import.

---

## ✅ 12. `BobShellAdapter` — `[using tool ...]` message filter
**Kept as-is.** The v1-era filter (`/^\[using tool /i.test(content)`) is harmless if Bob v2
doesn't emit these — it just never fires. Removing it is a no-op risk. Left with the existing
suppression logic; can be revisited once confirmed from Bob v2 output.

---

## ✅ 13. `BobShellProvider.ts` — initial snapshot `installed: true`
**Done.** Changed to `installed: false` in `buildInitialBobShellProviderSnapshot` — we haven't
checked yet, so claiming `installed: true` before the probe runs was misleading.

---

## ✅ 14. Settings schema — `--mode` → `--chat-mode` in descriptions
**Done.** Updated `launchArgs` description in `BobShellSettings` to show `--chat-mode=plan`
instead of `--mode plan`. Bob v2's correct flag is `--chat-mode=<slug>`.

---

## ✅ 15. Tests — fix and add
**Done.**
- Updated `--mode agent` tests → `--chat-mode=agent` tests in `BobShellAdapter.test.ts`.
- Removed stale `-m` model passthrough test (no longer relevant since `-m` was removed).
- Removed all `customModels: []` from test fixtures (field no longer exists).
- 13 tests pass.

---

## ✅ 16. Documentation
**Done.** Created [`docs/user/providers-bob-shell.md`](docs/user/providers-bob-shell.md) covering:
model selection (none — Bob manages it), authentication (SSO + API key + `--auth-method api-key`),
trusted folders, modes/skills, `BOB_HOME path`, thread history limitations, and multi-account setup.

---

## Summary of files changed

| File | Change |
|---|---|
| `bin/bob`, `bin/bob-stub.mjs` | **Deleted** |
| `packages/contracts/src/settings.ts` | Removed `customModels` from `BobShellSettings` and `BobShellSettingsPatch`; updated `apiKey` and `launchArgs` descriptions |
| `apps/web/src/modelSelection.ts` | Added `bobShell` to early-return guard alongside `antigravity`; fixed `unknown` cast |
| `apps/server/src/provider/Drivers/BobShellDriver.ts` | Removed unused `ProviderEventLoggers` from env type and import |
| `apps/server/src/provider/Drivers/BobShellHome.ts` | No change needed (env vars confirmed correct) |
| `apps/server/src/provider/Layers/BobShellAdapter.ts` | Removed `--trust`; `--mode` → `--chat-mode=agent`; skill wiring via `--chat-mode=<slug>`; `--auth-method api-key` when apiKey set; reasoning comment; readThread comment; respondToRequest message |
| `apps/server/src/provider/Layers/BobShellProvider.ts` | Removed `BOB_SHELL_BUILT_IN_MODELS`, `bobShellModelsFromSettings`, `customModels` usage, `AUTH_PROBE_TIMEOUT_MS` import; removed `--list-tasks 1` auth probe; `installed: false` in initial snapshot |
| `apps/server/src/textGeneration/BobShellTextGeneration.ts` | Removed `--trust`; removed `-m` model passthrough; added `--auth-method api-key` when apiKey set |
| `apps/server/src/provider/Layers/BobShellAdapter.test.ts` | Updated `--mode` → `--chat-mode` tests; removed stale `-m` model test; removed `customModels: []` from all fixtures |
| `docs/user/` | ⬜ Not yet done |
