/**
 * BobShellProvider — provider status probe for Bob Shell v2.
 *
 * Runs `bob --version` to detect installation. If it succeeds (exit 0) the
 * provider is marked ready and auth is assumed valid — there is no reliable
 * offline auth check in Bob v2.
 *
 * @module provider/Layers/BobShellProvider
 */
import { type BobShellSettings } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Result from "effect/Result";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import { resolveSpawnCommand } from "@t3tools/shared/shell";

import {
  buildServerProvider,
  isCommandMissingCause,
  parseGenericCliVersion,
  spawnAndCollect,
  type ServerProviderDraft,
} from "../providerSnapshot.ts";
import { makeBobShellEnvironment } from "../Drivers/BobShellHome.ts";

const VERSION_PROBE_TIMEOUT_MS = 4_000;

const BOB_SHELL_PRESENTATION = {
  displayName: "Bob Shell",
  showInteractionModeToggle: false,
} as const;

const runBobShellCommand = (
  bobShellSettings: BobShellSettings,
  args: ReadonlyArray<string>,
  environment: NodeJS.ProcessEnv,
) =>
  Effect.gen(function* () {
    const command = bobShellSettings.binaryPath || "bob";
    const env = yield* makeBobShellEnvironment(bobShellSettings, environment);
    const spawnCommand = yield* resolveSpawnCommand(command, args, { env });
    return yield* spawnAndCollect(
      command,
      ChildProcess.make(spawnCommand.command, spawnCommand.args, {
        env,
        shell: spawnCommand.shell,
      }),
    );
  });

export function buildInitialBobShellProviderSnapshot(
  settings: BobShellSettings,
): Effect.Effect<ServerProviderDraft> {
  return Effect.gen(function* () {
    const checkedAt = DateTime.formatIso(yield* DateTime.now);
    const models: ReadonlyArray<never> = [];

    if (!settings.enabled) {
      return buildServerProvider({
        presentation: BOB_SHELL_PRESENTATION,
        enabled: false,
        checkedAt,
        models,
        probe: {
          installed: false,
          version: null,
          status: "warning",
          auth: { status: "unknown" },
          message: "Bob Shell is disabled in T3 Code settings.",
        },
      });
    }

    return buildServerProvider({
      presentation: BOB_SHELL_PRESENTATION,
      enabled: true,
      checkedAt,
      models,
      probe: {
        installed: false,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "Checking Bob Shell availability...",
      },
    });
  });
}

export const checkBobShellProviderStatus = Effect.fn("checkBobShellProviderStatus")(function* (
  settings: BobShellSettings,
  environment: NodeJS.ProcessEnv = process.env,
): Effect.fn.Return<
  ServerProviderDraft,
  never,
  ChildProcessSpawner.ChildProcessSpawner | Path.Path
> {
  const checkedAt = DateTime.formatIso(yield* DateTime.now);
  const models: ReadonlyArray<never> = [];

  if (!settings.enabled) {
    return buildServerProvider({
      presentation: BOB_SHELL_PRESENTATION,
      enabled: false,
      checkedAt,
      models,
      probe: {
        installed: false,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "Bob Shell is disabled in T3 Code settings.",
      },
    });
  }

  // Step 1: check the binary exists and get version.
  const versionResult = yield* runBobShellCommand(settings, ["--version"], environment).pipe(
    Effect.timeoutOption(VERSION_PROBE_TIMEOUT_MS),
    Effect.result,
  );

  if (Result.isFailure(versionResult)) {
    const error = versionResult.failure;
    yield* Effect.logWarning("Bob Shell health check failed.", {
      errorTag: (error as { _tag?: string })._tag ?? "UnknownError",
    });
    return buildServerProvider({
      presentation: BOB_SHELL_PRESENTATION,
      enabled: settings.enabled,
      checkedAt,
      models,
      probe: {
        installed: !isCommandMissingCause(error),
        version: null,
        status: "error",
        auth: { status: "unknown" },
        message: isCommandMissingCause(error)
          ? "Bob Shell (`bob`) is not installed or not on PATH."
          : "Failed to execute Bob Shell health check.",
      },
    });
  }

  if (Option.isNone(versionResult.success)) {
    return buildServerProvider({
      presentation: BOB_SHELL_PRESENTATION,
      enabled: settings.enabled,
      checkedAt,
      models,
      probe: {
        installed: false,
        version: null,
        status: "error",
        auth: { status: "unknown" },
        message: "Bob Shell CLI check timed out.",
      },
    });
  }

  const versionOutput = versionResult.success.value;
  const version = parseGenericCliVersion(
    versionOutput.stdout.trim() || versionOutput.stderr.trim(),
  );

  if (versionOutput.code !== 0) {
    return buildServerProvider({
      presentation: BOB_SHELL_PRESENTATION,
      enabled: settings.enabled,
      checkedAt,
      models,
      probe: {
        installed: false,
        version: null,
        status: "error",
        auth: { status: "unknown" },
        message: "Bob Shell CLI exited with an error during version check.",
      },
    });
  }

  return buildServerProvider({
    presentation: BOB_SHELL_PRESENTATION,
    enabled: settings.enabled,
    checkedAt,
    models,
    probe: {
      installed: true,
      version,
      status: "ready",
      auth: { status: "authenticated" },
    },
  });
});
