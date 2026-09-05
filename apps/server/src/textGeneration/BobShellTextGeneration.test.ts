import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import { BobShellSettings, ProviderInstanceId } from "@t3tools/contracts";
import { isHostWindows } from "@t3tools/shared/hostProcess";
import { createModelSelection } from "@t3tools/shared/model";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import { expect } from "vite-plus/test";

import * as ServerConfig from "../config.ts";
import { makeBobShellTextGeneration } from "./BobShellTextGeneration.ts";

const decodeBobShellSettings = Schema.decodeSync(BobShellSettings);

const BobShellTextGenerationTestLayer = ServerConfig.ServerConfig.layerTest(process.cwd(), {
  prefix: "t3code-bob-text-generation-test-",
}).pipe(Layer.provideMerge(NodeServices.layer));

function makeFakeBobBinary(dir: string) {
  return Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const isWindows = yield* isHostWindows;
    const binDir = path.join(dir, "bin");
    const stubPath = path.join(binDir, "bob-stub.mjs");
    yield* fs.makeDirectory(binDir, { recursive: true });

    yield* fs.writeFileString(
      stubPath,
      [
        'const args = process.argv.slice(2).join(" ");',
        "",
        "function fail(message, code) {",
        '  process.stderr.write(message + "\\n");',
        "  process.exit(code);",
        "}",
        "",
        'const responseJson = process.env.T3_FAKE_BOB_RESPONSE_JSON || \'{"title": "Generated Title"}\';',
        "",
        "// Output stream-json tool_use event with attempt_completion",
        'console.log(JSON.stringify({ type: "tool_use", tool_name: "attempt_completion", parameters: { result: responseJson } }));',
        'console.log(JSON.stringify({ type: "result", status: "success" }));',
        "process.exit(0);",
      ].join("\n"),
    );

    if (isWindows) {
      const cmdPath = path.join(binDir, "bob.cmd");
      yield* fs.writeFileString(cmdPath, `@node "${stubPath}" %*\r\n`);
      return cmdPath;
    }

    const shPath = path.join(binDir, "bob");
    yield* fs.writeFileString(shPath, `#!/bin/sh\nexec node "${stubPath}" "$@"\n`);
    yield* fs.chmod(shPath, 0o755);
    return shPath;
  });
}

it.layer(BobShellTextGenerationTestLayer)("BobShellTextGeneration", (it) => {
  it.effect("generates thread title using bob text generation service", () =>
    Effect.gen(function* () {
      const config = yield* ServerConfig.ServerConfig;
      const fakeBinary = yield* makeFakeBobBinary(config.cwd);

      const bobSettings = decodeBobShellSettings({
        binaryPath: fakeBinary,
      });

      const textGen = yield* makeBobShellTextGeneration(bobSettings, {
        ...process.env,
        T3_FAKE_BOB_RESPONSE_JSON: '{"title":"My Cool Thread"}',
      });

      const result = yield* textGen.generateThreadTitle({
        cwd: config.cwd,
        message: "Please help me write a function",
        modelSelection: createModelSelection(ProviderInstanceId.make("bobShell"), "premium"),
      });

      expect(result.title).toBe("My Cool Thread");
    }),
  );

  it.effect("generates commit message using bob text generation service", () =>
    Effect.gen(function* () {
      const config = yield* ServerConfig.ServerConfig;
      const fakeBinary = yield* makeFakeBobBinary(config.cwd);

      const bobSettings = decodeBobShellSettings({
        binaryPath: fakeBinary,
      });

      const textGen = yield* makeBobShellTextGeneration(bobSettings, {
        ...process.env,
        T3_FAKE_BOB_RESPONSE_JSON:
          '{"subject":"feat: add amazing feature","body":"Added the feature details."}',
      });

      const result = yield* textGen.generateCommitMessage({
        cwd: config.cwd,
        branch: "main",
        stagedSummary: "modified file.ts",
        stagedPatch: "diff --git a/file b/file",
        modelSelection: createModelSelection(ProviderInstanceId.make("bobShell"), "premium"),
      });

      expect(result.subject).toBe("feat: add amazing feature");
      expect(result.body).toBe("Added the feature details.");
    }),
  );
});
