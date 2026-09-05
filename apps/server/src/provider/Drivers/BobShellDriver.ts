/**
 * BobShellDriver — `ProviderDriver` for Bob Shell.
 *
 * Mirrors the ClaudeDriver / GrokDriver pattern: a plain value whose
 * `create()` returns one `ProviderInstance` bundling:
 *   - `snapshot`       — live status/model list via `BobShellProvider`
 *   - `adapter`        — session/turn runtime via `BobShellAdapter`
 *   - `textGeneration` — commit/PR/branch/title gen via `BobShellTextGeneration`
 *
 * @module provider/Drivers/BobShellDriver
 */
import { BobShellSettings, ProviderDriverKind } from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import { HttpClient } from "effect/unstable/http";
import { ChildProcessSpawner } from "effect/unstable/process";

import * as BackgroundPolicy from "../../background/BackgroundPolicy.ts";
import { ServerConfig } from "../../config.ts";
import { ServerSettingsService } from "../../serverSettings.ts";
import { makeBobShellTextGeneration } from "../../textGeneration/BobShellTextGeneration.ts";
import { ProviderDriverError } from "../Errors.ts";
import { makeBobShellAdapter } from "../Layers/BobShellAdapter.ts";
import {
  buildInitialBobShellProviderSnapshot,
  checkBobShellProviderStatus,
} from "../Layers/BobShellProvider.ts";
import { enrichProviderSnapshotWithVersionAdvisory } from "../providerMaintenance.ts";
import { makeManagedServerProvider } from "../makeManagedServerProvider.ts";
import {
  defaultProviderContinuationIdentity,
  type ProviderDriver,
  type ProviderInstance,
} from "../ProviderDriver.ts";
import { withInstanceIdentity } from "./instanceIdentity.ts";
import { mergeProviderInstanceEnvironment } from "../ProviderInstanceEnvironment.ts";
import {
  makeManualOnlyProviderMaintenanceCapabilities,
  makeStaticProviderMaintenanceResolver,
  resolveProviderMaintenanceCapabilitiesEffect,
} from "../providerMaintenance.ts";
import {
  haveProviderSnapshotSettingsChanged,
  makeProviderSnapshotSettingsSource,
  type ProviderSnapshotSettings,
} from "../providerUpdateSettings.ts";
import { makeBobShellContinuationGroupKey } from "./BobShellHome.ts";
import { discoverBobShellSkills } from "./BobShellSkills.ts";

const decodeSettings = Schema.decodeSync(BobShellSettings);

const DRIVER_KIND = ProviderDriverKind.make("bobShell");

/** No NPM package or Homebrew formula to check — manual-only updates. */
const UPDATE = makeStaticProviderMaintenanceResolver(
  makeManualOnlyProviderMaintenanceCapabilities({
    provider: DRIVER_KIND,
    packageName: null,
  }),
);

export type BobShellDriverEnv =
  | BackgroundPolicy.BackgroundPolicy
  | ChildProcessSpawner.ChildProcessSpawner
  | Crypto.Crypto
  | FileSystem.FileSystem
  | HttpClient.HttpClient
  | Path.Path
  | ServerConfig
  | ServerSettingsService;

export const BobShellDriver: ProviderDriver<BobShellSettings, BobShellDriverEnv> = {
  driverKind: DRIVER_KIND,
  metadata: {
    displayName: "Bob Shell",
    supportsMultipleInstances: true,
  },
  configSchema: BobShellSettings,
  defaultConfig: () => decodeSettings({}),
  create: ({ instanceId, displayName, accentColor, environment, enabled, config }) =>
    Effect.gen(function* () {
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const httpClient = yield* HttpClient.HttpClient;
      const serverSettings = yield* ServerSettingsService;
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const processEnv = mergeProviderInstanceEnvironment(environment);

      const effectiveConfig = { ...config, enabled } satisfies BobShellSettings;
      const continuationGroupKey = yield* makeBobShellContinuationGroupKey(effectiveConfig);
      const continuationIdentity = defaultProviderContinuationIdentity({ driverKind: DRIVER_KIND, instanceId });
      const stampIdentity = withInstanceIdentity({
        instanceId,
        driverKind: DRIVER_KIND,
        displayName,
        accentColor,
        continuationGroupKey,
      });

      const maintenanceCapabilities = yield* resolveProviderMaintenanceCapabilitiesEffect(UPDATE, {
        binaryPath: effectiveConfig.binaryPath,
        env: processEnv,
      });

      const adapter = yield* makeBobShellAdapter(effectiveConfig, {
        instanceId,
        environment: processEnv,
      });

      const textGeneration = yield* makeBobShellTextGeneration(effectiveConfig, processEnv);

      const checkProvider = checkBobShellProviderStatus(effectiveConfig, processEnv).pipe(
        Effect.map(stampIdentity),
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
        Effect.provideService(Path.Path, path),
      );

      const snapshotSettings = makeProviderSnapshotSettingsSource(effectiveConfig, serverSettings);
      const snapshot = yield* makeManagedServerProvider<ProviderSnapshotSettings<BobShellSettings>>({
        maintenanceCapabilities,
        getSettings: snapshotSettings.getSettings,
        streamSettings: snapshotSettings.streamSettings,
        haveSettingsChanged: haveProviderSnapshotSettingsChanged,
        initialSnapshot: (settings) =>
          buildInitialBobShellProviderSnapshot(settings.provider).pipe(Effect.map(stampIdentity)),
        checkProvider,
        enrichSnapshot: ({ settings, snapshot: currentSnapshot, publishSnapshot }) =>
          enrichProviderSnapshotWithVersionAdvisory(currentSnapshot, maintenanceCapabilities, {
            enableProviderUpdateChecks: settings.enableProviderUpdateChecks,
          }).pipe(
            Effect.provideService(HttpClient.HttpClient, httpClient),
            Effect.flatMap((enrichedSnapshot) => publishSnapshot(enrichedSnapshot)),
          ),
      }).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderDriverError({
              driver: DRIVER_KIND,
              instanceId,
              detail: `Failed to build Bob Shell snapshot: ${cause.message ?? String(cause)}`,
              cause,
            }),
        ),
      );

      const snapshotForCwd = (cwd: string) =>
        !effectiveConfig.enabled
          ? snapshot.getSnapshot
          : Effect.all([
              snapshot.getSnapshot,
              discoverBobShellSkills(effectiveConfig, cwd, processEnv).pipe(
                Effect.provideService(FileSystem.FileSystem, fileSystem),
                Effect.provideService(Path.Path, path),
              ),
            ]).pipe(
              Effect.map(([machineSnapshot, skills]) => ({ ...machineSnapshot, skills })),
            );

      return {
        instanceId,
        driverKind: DRIVER_KIND,
        continuationIdentity: {
          ...continuationIdentity,
          continuationKey: continuationGroupKey,
        },
        displayName,
        accentColor,
        enabled,
        snapshot,
        snapshotForCwd,
        adapter,
        textGeneration,
      } satisfies ProviderInstance;
    }),
};
