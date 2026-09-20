/**
 * BobShellProvider tests — covers provider snapshot logic.
 *
 * Exercises:
 *   - disabled: immediate warning snapshot without probing
 *   - missing binary: error snapshot with installed=false
 *   - binary exits non-zero: error snapshot with installed=true
 *   - happy path: ready snapshot with version parsed
 */
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Schema from "effect/Schema";
import { BobShellSettings } from "@t3tools/contracts";

import {
  buildInitialBobShellProviderSnapshot,
  checkBobShellProviderStatus,
} from "./BobShellProvider.ts";
import { writeFakeCli } from "../../testUtils/fakeCli.ts";

const decodeSettings = Schema.decodeSync(BobShellSettings);

// ── buildInitialBobShellProviderSnapshot (pure) ───────────────────────────────

it.effect(
  "buildInitialBobShellProviderSnapshot: disabled → disabled status, models populated",
  () =>
    Effect.gen(function* () {
      const snapshot = yield* buildInitialBobShellProviderSnapshot(
        decodeSettings({ enabled: false }),
      );
      expect(snapshot.enabled).toBe(false);
      expect(snapshot.status).toBe("disabled");
      expect(snapshot.models).toHaveLength(1);
      expect(snapshot.models[0]?.slug).toBe("premium");
      expect(snapshot.models[0]?.isDefault).toBe(true);
    }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect(
  "buildInitialBobShellProviderSnapshot: enabled → warning/checking, models populated",
  () =>
    Effect.gen(function* () {
      const snapshot = yield* buildInitialBobShellProviderSnapshot(
        decodeSettings({ enabled: true }),
      );
      expect(snapshot.enabled).toBe(true);
      expect(snapshot.status).toBe("warning");
      expect(snapshot.models).toHaveLength(1);
      expect(snapshot.models[0]?.slug).toBe("premium");
    }).pipe(Effect.provide(NodeServices.layer)),
);

// ── checkBobShellProviderStatus (live process spawn) ──────────────────────────

it.layer(NodeServices.layer)("checkBobShellProviderStatus", (it) => {
  it.effect("disabled → disabled status without spawning any process", () =>
    Effect.gen(function* () {
      const snapshot = yield* checkBobShellProviderStatus(decodeSettings({ enabled: false }));
      expect(snapshot.enabled).toBe(false);
      expect(snapshot.status).toBe("disabled");
      expect(snapshot.installed).toBe(false);
    }),
  );

  it.effect("missing binary → error with installed=false", () =>
    Effect.gen(function* () {
      const snapshot = yield* checkBobShellProviderStatus(
        decodeSettings({ enabled: true, binaryPath: "/definitely/not/installed/bob-shell-binary" }),
      );
      expect(snapshot.enabled).toBe(true);
      expect(snapshot.installed).toBe(false);
      expect(snapshot.status).toBe("error");
      expect(snapshot.message).toMatch(/not installed|not on PATH|Failed to execute/);
    }),
  );

  it.effect("binary exits non-zero → error with installed=true", () =>
    Effect.gen(function* () {
      const snapshot = yield* Effect.scoped(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem;
          const dir = yield* fs.makeTempDirectoryScoped({ prefix: "t3code-bob-version-" });
          const bobPath = writeFakeCli({
            directory: dir,
            name: "bob",
            source: [
              "process.stderr.write('internal error\\n');",
              "process.exit(1);",
            ].join("\n"),
          });
          return yield* checkBobShellProviderStatus(
            decodeSettings({ enabled: true, binaryPath: bobPath }),
          );
        }),
      );
      expect(snapshot.enabled).toBe(true);
      expect(snapshot.status).toBe("error");
    }),
  );

  it.effect("binary exits 0 with version string → ready snapshot", () =>
    Effect.gen(function* () {
      const snapshot = yield* Effect.scoped(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem;
          const dir = yield* fs.makeTempDirectoryScoped({ prefix: "t3code-bob-version-ok-" });
          const bobPath = writeFakeCli({
            directory: dir,
            name: "bob",
            source: [
              "process.stdout.write('bob 2.1.0\\n');",
              "process.exit(0);",
            ].join("\n"),
          });
          return yield* checkBobShellProviderStatus(
            decodeSettings({ enabled: true, binaryPath: bobPath }),
          );
        }),
      );
      expect(snapshot.enabled).toBe(true);
      expect(snapshot.installed).toBe(true);
      expect(snapshot.status).toBe("ready");
      expect(snapshot.auth?.status).toBe("authenticated");
    }),
  );
});
