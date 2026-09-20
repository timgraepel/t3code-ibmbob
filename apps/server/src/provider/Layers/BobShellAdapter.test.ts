/**
 * BobShellAdapter tests — covers observable behaviour of the ACP-based adapter.
 *
 * Low-level ACP wire protocol is exercised by AcpJsonRpcConnection.test.ts.
 * ACP-to-T3-event mapping is covered by AcpCoreRuntimeEvents.test.ts.
 * Here we focus on Bob Shell–specific logic:
 *   - spawn-arg shape (binaryPath, hardcoded "acp --auto-approve" args, env passthrough)
 *   - validation guards (disabled provider, missing cwd, rollback unsupported)
 *   - adapter capabilities shape
 */
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import {
  BobShellSettings,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";

import {
  buildBobShellAcpSpawnInput,
  makeBobShellAdapter,
} from "./BobShellAdapter.ts";

const decodeSettings = Schema.decodeSync(BobShellSettings);

const enabledSettings = decodeSettings({
  enabled: true,
  binaryPath: "bob",
  homePath: "",
  teamId: "",
  apiKey: "",
  launchArgs: "",
});

const adapterOptions = {
  instanceId: ProviderInstanceId.make("bob-test"),
  environment: {},
};

// ── buildBobShellAcpSpawnInput (pure) ────────────────────────────────────────

it("buildBobShellAcpSpawnInput: uses binaryPath as command and hardcodes acp --auto-approve args", () => {
  const env = { PATH: "/usr/bin" };
  const result = buildBobShellAcpSpawnInput(enabledSettings, "/some/cwd", env);
  expect(result.command).toBe("bob");
  expect(result.args).toEqual(["acp", "--auto-approve"]);
  expect(result.cwd).toBe("/some/cwd");
  expect(result.env).toBe(env);
});

it("buildBobShellAcpSpawnInput: falls back to 'bob' when binaryPath is empty", () => {
  const s = decodeSettings({ enabled: true });
  const result = buildBobShellAcpSpawnInput(s, "/cwd", {});
  expect(result.command).toBe("bob");
});

it("buildBobShellAcpSpawnInput: uses custom binaryPath when set", () => {
  const s = decodeSettings({ enabled: true, binaryPath: "/usr/local/bin/mybob" });
  const result = buildBobShellAcpSpawnInput(s, "/cwd", {});
  expect(result.command).toBe("/usr/local/bin/mybob");
  expect(result.args).toEqual(["acp", "--auto-approve"]);
});

// ── Validation guards ─────────────────────────────────────────────────────────

it.effect(
  "startSession: validation error when provider is disabled",
  () =>
    Effect.gen(function* () {
      const disabledSettings = decodeSettings({ enabled: false });
      const adapter = yield* makeBobShellAdapter(disabledSettings, adapterOptions);
      const result = yield* adapter
        .startSession({ threadId: ThreadId.make("t"), runtimeMode: "full-access" })
        .pipe(Effect.exit);
      expect(Exit.isFailure(result)).toBe(true);
      if (Exit.isFailure(result)) {
        expect(String(result.cause)).toContain("ProviderAdapterValidationError");
        expect(String(result.cause)).toContain("Enable Bob Shell");
      }
    }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect(
  "startSession: validation error when cwd is empty string",
  () =>
    Effect.gen(function* () {
      const adapter = yield* makeBobShellAdapter(enabledSettings, adapterOptions);
      const result = yield* adapter
        .startSession({ threadId: ThreadId.make("t"), runtimeMode: "full-access", cwd: "" })
        .pipe(Effect.exit);
      expect(Exit.isFailure(result)).toBe(true);
      if (Exit.isFailure(result)) {
        expect(String(result.cause)).toContain("workspace directory");
      }
    }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect(
  "startSession: validation error when cwd is whitespace-only",
  () =>
    Effect.gen(function* () {
      const adapter = yield* makeBobShellAdapter(enabledSettings, adapterOptions);
      const result = yield* adapter
        .startSession({ threadId: ThreadId.make("t"), runtimeMode: "full-access", cwd: "   " })
        .pipe(Effect.exit);
      expect(Exit.isFailure(result)).toBe(true);
      if (Exit.isFailure(result)) {
        expect(String(result.cause)).toContain("workspace directory");
      }
    }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect(
  "rollbackThread: always returns validation error (not supported)",
  () =>
    Effect.gen(function* () {
      const adapter = yield* makeBobShellAdapter(enabledSettings, adapterOptions);
      const result = yield* adapter
        .rollbackThread(ThreadId.make("t"), 1)
        .pipe(Effect.exit);
      expect(Exit.isFailure(result)).toBe(true);
      if (Exit.isFailure(result)) {
        expect(String(result.cause)).toContain("ProviderAdapterValidationError");
        expect(String(result.cause)).toContain("Bob Shell does not support");
      }
    }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect(
  "stopSession: not-found error for a thread that was never started",
  () =>
    Effect.gen(function* () {
      const adapter = yield* makeBobShellAdapter(enabledSettings, adapterOptions);
      const result = yield* adapter
        .stopSession(ThreadId.make("unknown"))
        .pipe(Effect.exit);
      expect(Exit.isFailure(result)).toBe(true);
      if (Exit.isFailure(result)) {
        expect(String(result.cause)).toContain("ProviderAdapterSessionNotFoundError");
      }
    }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect(
  "interruptTurn: not-found error when no session exists",
  () =>
    Effect.gen(function* () {
      const adapter = yield* makeBobShellAdapter(enabledSettings, adapterOptions);
      const result = yield* adapter
        .interruptTurn(ThreadId.make("unknown"))
        .pipe(Effect.exit);
      expect(Exit.isFailure(result)).toBe(true);
      if (Exit.isFailure(result)) {
        expect(String(result.cause)).toContain("ProviderAdapterSessionNotFoundError");
      }
    }).pipe(Effect.provide(NodeServices.layer)),
);

// ── Adapter capabilities ──────────────────────────────────────────────────────

it.effect(
  "adapter declares expected capabilities",
  () =>
    Effect.gen(function* () {
      const adapter = yield* makeBobShellAdapter(enabledSettings, adapterOptions);
      expect(adapter.capabilities).toEqual({
        sessionModelSwitch: "unsupported",
        promptlessTurnContinuation: false,
        supportsConversationRollback: false,
      });
    }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect(
  "hasSession returns false before any session is started",
  () =>
    Effect.gen(function* () {
      const adapter = yield* makeBobShellAdapter(enabledSettings, adapterOptions);
      const result = yield* adapter.hasSession(ThreadId.make("never-started"));
      expect(result).toBe(false);
    }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect(
  "listSessions returns empty array when no sessions are active",
  () =>
    Effect.gen(function* () {
      const adapter = yield* makeBobShellAdapter(enabledSettings, adapterOptions);
      const sessions = yield* adapter.listSessions();
      expect(sessions).toEqual([]);
    }).pipe(Effect.provide(NodeServices.layer)),
);
