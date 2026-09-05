/**
 * BobShellHome — home-path helpers for Bob Shell instances.
 *
 * Bob Shell stores its config in `~/.bob` by default. When an explicit
 * `homePath` is configured we pass it via the `BOB_HOME` environment
 * variable, which Bob Shell reads to locate its config directory.
 *
 * `apiKey` is the Bob Shell API key. When set it is injected as
 * `BOBSHELL_API_KEY` so that `bob run` can authenticate without a browser
 * session. Leave empty to rely on browser-based authentication.
 *
 * @module provider/Drivers/BobShellHome
 */
import * as NodeOS from "node:os";

import type { BobShellSettings } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Path from "effect/Path";

import { expandHomePath } from "../../pathExpansion.ts";

export const resolveBobShellHomePath = Effect.fn("resolveBobShellHomePath")(function* (
  config: Pick<BobShellSettings, "homePath">,
): Effect.fn.Return<string, never, Path.Path> {
  const path = yield* Path.Path;
  const homePath = config.homePath.trim();
  return path.resolve(homePath.length > 0 ? expandHomePath(homePath) : NodeOS.homedir());
});

export const makeBobShellEnvironment = Effect.fn("makeBobShellEnvironment")(function* (
  config: Pick<BobShellSettings, "homePath" | "apiKey">,
  baseEnv?: NodeJS.ProcessEnv,
): Effect.fn.Return<NodeJS.ProcessEnv, never, Path.Path> {
  const resolvedBaseEnv = baseEnv ?? process.env;
  const env: NodeJS.ProcessEnv = { ...resolvedBaseEnv };

  const homePath = config.homePath.trim();
  if (homePath.length > 0) {
    const resolvedHomePath = yield* resolveBobShellHomePath(config);
    env.BOB_HOME = resolvedHomePath;
  }

  const apiKey = (config.apiKey ?? "").trim();
  if (apiKey) {
    env.BOBSHELL_API_KEY = apiKey;
  }

  return env;
});

export const makeBobShellContinuationGroupKey = Effect.fn("makeBobShellContinuationGroupKey")(
  function* (config: Pick<BobShellSettings, "homePath">): Effect.fn.Return<string, never, Path.Path> {
    const resolvedHomePath = yield* resolveBobShellHomePath(config);
    return `bobShell:home:${resolvedHomePath}`;
  },
);
