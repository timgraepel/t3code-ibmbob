/**
 * BobShellHome tests — covers environment building helpers.
 *
 * Exercises:
 *   - makeBobShellEnvironment: no extra keys when homePath and apiKey are empty
 *   - makeBobShellEnvironment: BOB_HOME injected when homePath is set
 *   - makeBobShellEnvironment: BOBSHELL_API_KEY injected when apiKey is set
 *   - makeBobShellEnvironment: both BOB_HOME and BOBSHELL_API_KEY injected together
 *   - makeBobShellEnvironment: does not mutate the input base env
 *   - resolveBobShellHomePath: empty homePath resolves to os.homedir()
 *   - resolveBobShellHomePath: explicit homePath is resolved via path.resolve
 *   - makeBobShellContinuationGroupKey: key includes resolved bob home
 */
import * as NodeOS from "node:os";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { BobShellSettings } from "@t3tools/contracts";

import {
  makeBobShellContinuationGroupKey,
  makeBobShellEnvironment,
  resolveBobShellHomePath,
} from "./BobShellHome.ts";

const decodeSettings = Schema.decodeSync(BobShellSettings);

it.layer(NodeServices.layer)("makeBobShellEnvironment", (it) => {
  it.effect("no extra keys when homePath and apiKey are empty", () =>
    Effect.gen(function* () {
      const base = { PATH: "/usr/bin" };
      const env = yield* makeBobShellEnvironment(decodeSettings({ enabled: true }), base);

      expect(env.PATH).toBe("/usr/bin");
      expect(env.BOB_HOME).toBeUndefined();
      expect(env.BOBSHELL_API_KEY).toBeUndefined();
    }),
  );

  it.effect("BOB_HOME is injected when homePath is set", () =>
    Effect.gen(function* () {
      const env = yield* makeBobShellEnvironment(
        decodeSettings({ enabled: true, homePath: "/custom/bob/home" }),
        {},
      );
      expect(env.BOB_HOME).toBe("/custom/bob/home");
    }),
  );

  it.effect("BOBSHELL_API_KEY is injected when apiKey is set", () =>
    Effect.gen(function* () {
      const env = yield* makeBobShellEnvironment(
        decodeSettings({ enabled: true, apiKey: "my-secret-key" }),
        {},
      );
      expect(env.BOBSHELL_API_KEY).toBe("my-secret-key");
    }),
  );

  it.effect("both BOB_HOME and BOBSHELL_API_KEY are injected when both are set", () =>
    Effect.gen(function* () {
      const env = yield* makeBobShellEnvironment(
        decodeSettings({ enabled: true, homePath: "/my/bob", apiKey: "key123" }),
        {},
      );
      expect(env.BOB_HOME).toBe("/my/bob");
      expect(env.BOBSHELL_API_KEY).toBe("key123");
    }),
  );

  it.effect("does not mutate the base env object", () =>
    Effect.gen(function* () {
      const base = { PATH: "/usr/bin" };
      yield* makeBobShellEnvironment(
        decodeSettings({ enabled: true, homePath: "/some/home", apiKey: "key" }),
        base,
      );
      // Original base should be untouched
      expect(Object.keys(base)).toEqual(["PATH"]);
    }),
  );

  it.effect("whitespace-only apiKey is not injected", () =>
    Effect.gen(function* () {
      const env = yield* makeBobShellEnvironment(
        decodeSettings({ enabled: true, apiKey: "   " }),
        {},
      );
      // BobShellSettings trims apiKey via TrimmedString, so "   " → ""
      expect(env.BOBSHELL_API_KEY).toBeUndefined();
    }),
  );
});

it.layer(NodeServices.layer)("resolveBobShellHomePath", (it) => {
  it.effect("empty homePath resolves to os.homedir()", () =>
    Effect.gen(function* () {
      const resolved = yield* resolveBobShellHomePath(decodeSettings({ enabled: true }));
      expect(resolved).toBe(NodeOS.homedir());
    }),
  );

  it.effect("absolute homePath is used as-is", () =>
    Effect.gen(function* () {
      const resolved = yield* resolveBobShellHomePath(
        decodeSettings({ enabled: true, homePath: "/custom/bob" }),
      );
      expect(resolved).toBe("/custom/bob");
    }),
  );
});

it.layer(NodeServices.layer)("makeBobShellContinuationGroupKey", (it) => {
  it.effect("key includes 'bobShell:home:' prefix and the resolved home path", () =>
    Effect.gen(function* () {
      const key = yield* makeBobShellContinuationGroupKey(
        decodeSettings({ enabled: true, homePath: "/custom/bob" }),
      );
      expect(key).toBe("bobShell:home:/custom/bob");
    }),
  );

  it.effect("key uses os.homedir() when homePath is empty", () =>
    Effect.gen(function* () {
      const key = yield* makeBobShellContinuationGroupKey(decodeSettings({ enabled: true }));
      expect(key).toBe(`bobShell:home:${NodeOS.homedir()}`);
    }),
  );
});
