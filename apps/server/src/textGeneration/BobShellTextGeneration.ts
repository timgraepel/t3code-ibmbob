/**
 * BobShellTextGeneration — text generation via the Bob Shell CLI.
 *
 * Spawns `bob run "<prompt>" --format stream-json --auto-approve` in the
 * project cwd and collects the `attempt_completion` tool result (or direct assistant message)
 * as the generated text. The prompt instructs Bob Shell to output valid JSON
 * matching the requested schema so the result can be decoded exactly like
 * the Claude/Codex text generation layers.
 *
 * @module textGeneration/BobShellTextGeneration
 */
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import { type BobShellSettings, type ModelSelection } from "@t3tools/contracts";
import { sanitizeBranchFragment, sanitizeFeatureBranchName } from "@t3tools/shared/git";
import { resolveSpawnCommand } from "@t3tools/shared/shell";

import { TextGenerationError } from "@t3tools/contracts";
import * as TextGeneration from "./TextGeneration.ts";
import {
  buildBranchNamePrompt,
  buildCommitMessagePrompt,
  buildPrContentPrompt,
  buildThreadTitlePrompt,
} from "./TextGenerationPrompts.ts";
import {
  normalizeCliError,
  sanitizeCommitSubject,
  sanitizePrTitle,
  sanitizeThreadTitle,
  toJsonSchemaObject,
} from "./TextGenerationUtils.ts";
import { makeBobShellEnvironment } from "../provider/Drivers/BobShellHome.ts";
import { extractJsonObject } from "@t3tools/shared/schemaJson";

const isTextGenerationError = Schema.is(TextGenerationError);
const encodeJsonString = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));

const BOB_SHELL_TIMEOUT_MS = 180_000;

/** Shape of `stream-json` events from `bob run --format stream-json`. */
const BobToolUseEventSchema = Schema.Struct({
  type: Schema.Literal("tool_use"),
  tool_name: Schema.String,
  parameters: Schema.Unknown,
});

const BobMessageEventSchema = Schema.Struct({
  type: Schema.Literal("message"),
  role: Schema.String,
  content: Schema.String,
  isReasoning: Schema.optional(Schema.Boolean),
});

const BobStreamJsonEvent = Schema.Union([
  BobToolUseEventSchema,
  BobMessageEventSchema,
  Schema.Struct({ type: Schema.Literal("result"), status: Schema.String }),
  Schema.Struct({ type: Schema.String }),
]);

const decodeBobTextStreamEvent = Schema.decodeUnknownOption(
  Schema.fromJsonString(BobStreamJsonEvent),
);

const decodeOutput = <S extends Schema.Top>(outputSchemaJson: S) =>
  Schema.decodeEffect(Schema.fromJsonString(outputSchemaJson));

export const makeBobShellTextGeneration = Effect.fn("makeBobShellTextGeneration")(function* (
  bobShellSettings: BobShellSettings,
  environment?: NodeJS.ProcessEnv,
) {
  const commandSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const bobEnvironment = yield* makeBobShellEnvironment(bobShellSettings, environment);

  const readStreamAsString = <E>(
    operation: string,
    stream: Stream.Stream<Uint8Array, E>,
  ): Effect.Effect<string, TextGenerationError> =>
    stream.pipe(
      Stream.decodeText(),
      Stream.runFold(
        () => "",
        (acc, chunk) => acc + chunk,
      ),
      Effect.mapError((cause) =>
        normalizeCliError("bob", operation, cause, "Failed to collect process output"),
      ),
    );

  /**
   * Extract the text completion result from stream-json output.
   * Inspects `attempt_completion` parameters first, then accumulates `message` events.
   */
  function extractCompletionResult(stdout: string): string | undefined {
    let accumulatedMessage = "";
    for (const line of stdout.split(/\r?\n/)) {
      const trimmed = line.trim();
      if (!trimmed) continue;
      const decoded = decodeBobTextStreamEvent(trimmed);
      if (decoded._tag !== "Some") continue;
      const event = decoded.value;
      if (
        event.type === "tool_use" &&
        (event as { tool_name?: string }).tool_name === "attempt_completion"
      ) {
        const params = (event as { parameters?: unknown }).parameters;
        if (params && typeof params === "object" && params !== null) {
          const result = (params as { result?: unknown }).result;
          if (typeof result === "string") return result;
        }
      } else if (event.type === "message") {
        const msg = event as typeof BobMessageEventSchema.Type;
        if (msg.role === "assistant" && msg.content && !msg.isReasoning) {
          accumulatedMessage += msg.content;
        }
      }
    }
    if (accumulatedMessage.trim()) {
      return accumulatedMessage;
    }
    return undefined;
  }

  const runBobJson = <S extends Schema.Top>({
    operation,
    cwd,
    prompt,
    outputSchemaJson,
    modelSelection,
  }: {
    operation:
      | "generateCommitMessage"
      | "generatePrContent"
      | "generateBranchName"
      | "generateThreadTitle";
    cwd: string;
    prompt: string;
    outputSchemaJson: S;
    modelSelection: ModelSelection;
  }): Effect.Effect<S["Type"], TextGenerationError, S["DecodingServices"]> =>
    Effect.gen(function* () {
      const binary = bobShellSettings.binaryPath || "bob";
      const extraArgs: string[] = [];
      if (bobShellSettings.teamId?.trim()) {
        extraArgs.push("--team-id", bobShellSettings.teamId.trim());
      }
      const apiKey = (bobShellSettings.apiKey ?? "").trim();
      if (apiKey) {
        extraArgs.push("--auth-method", "api-key");
      }

      // Tell Bob Shell to emit structured JSON matching the requested schema.
      const jsonSchemaStr = yield* encodeJsonString(toJsonSchemaObject(outputSchemaJson)).pipe(
        Effect.mapError(
          (cause) =>
            new TextGenerationError({
              operation,
              detail: "Failed to encode structured output schema.",
              cause,
            }),
        ),
      );
      const fullPrompt = `${prompt}\n\nRespond ONLY with a JSON object that satisfies this JSON Schema:\n${jsonSchemaStr}`;

      const spawnCommand = yield* resolveSpawnCommand(
        binary,
        [
          "run",
          "--auto-approve",
          "--format",
          "stream-json",
          "--disable-mcp",
          ...extraArgs,
          fullPrompt,
        ],
        { env: bobEnvironment },
      );

      const command = ChildProcess.make(spawnCommand.command, spawnCommand.args, {
        env: bobEnvironment,
        cwd,
        shell: spawnCommand.shell,
      });

      const child = yield* commandSpawner
        .spawn(command)
        .pipe(
          Effect.mapError((cause) =>
            normalizeCliError("bob", operation, cause, "Failed to spawn Bob Shell process"),
          ),
        );

      const [stdout, , exitCode] = yield* Effect.all(
        [
          readStreamAsString(operation, child.stdout),
          readStreamAsString(operation, child.stderr),
          child.exitCode.pipe(
            Effect.mapError((cause) =>
              normalizeCliError("bob", operation, cause, "Failed to read Bob Shell exit code"),
            ),
          ),
        ],
        { concurrency: "unbounded" },
      );

      if (exitCode !== 0) {
        return yield* new TextGenerationError({
          operation,
          detail: `Bob Shell exited with code ${exitCode}.`,
        });
      }

      const completionText = extractCompletionResult(stdout);
      if (!completionText) {
        return yield* new TextGenerationError({
          operation,
          detail: "Bob Shell returned no completion result.",
        });
      }

      const trimmed = completionText.trim();
      return yield* decodeOutput(outputSchemaJson)(extractJsonObject(trimmed)).pipe(
        Effect.catchTags({
          SchemaError: (cause) =>
            Effect.fail(
              new TextGenerationError({
                operation,
                detail: "Bob Shell returned invalid structured output.",
                cause,
              }),
            ),
        }),
      );
    }).pipe(
      Effect.mapError((cause) =>
        isTextGenerationError(cause)
          ? cause
          : new TextGenerationError({
              operation,
              detail: "Bob Shell text generation failed.",
              cause,
            }),
      ),
      Effect.scoped,
    );

  const runBobJsonWithTimeout = <S extends Schema.Top>(
    args: Parameters<typeof runBobJson<S>>[0],
  ): Effect.Effect<S["Type"], TextGenerationError, S["DecodingServices"]> =>
    runBobJson(args).pipe(
      Effect.timeoutOption(BOB_SHELL_TIMEOUT_MS),
      Effect.flatMap(
        Option.match({
          onNone: () =>
            Effect.fail(
              new TextGenerationError({
                operation: args.operation,
                detail: "Bob Shell request timed out.",
              }),
            ),
          onSome: (value) => Effect.succeed(value),
        }),
      ),
    );

  const generateCommitMessage: TextGeneration.TextGeneration["Service"]["generateCommitMessage"] =
    Effect.fn("BobShellTextGeneration.generateCommitMessage")(function* (input) {
      const { prompt, outputSchema } = buildCommitMessagePrompt({
        branch: input.branch,
        stagedSummary: input.stagedSummary,
        stagedPatch: input.stagedPatch,
        includeBranch: input.includeBranch === true,
        policy: input.policy,
      });

      const generated = yield* runBobJsonWithTimeout({
        operation: "generateCommitMessage",
        cwd: input.cwd,
        prompt,
        outputSchemaJson: outputSchema,
        modelSelection: input.modelSelection,
      });

      return {
        subject: sanitizeCommitSubject(generated.subject),
        body: generated.body.trim(),
        ...("branch" in generated && typeof generated.branch === "string"
          ? { branch: sanitizeFeatureBranchName(generated.branch) }
          : {}),
      };
    });

  const generatePrContent: TextGeneration.TextGeneration["Service"]["generatePrContent"] =
    Effect.fn("BobShellTextGeneration.generatePrContent")(function* (input) {
      const { prompt, outputSchema } = buildPrContentPrompt({
        baseBranch: input.baseBranch,
        headBranch: input.headBranch,
        commitSummary: input.commitSummary,
        diffSummary: input.diffSummary,
        diffPatch: input.diffPatch,
        policy: input.policy,
        changeRequestTemplate: input.changeRequestTemplate,
      });

      const generated = yield* runBobJsonWithTimeout({
        operation: "generatePrContent",
        cwd: input.cwd,
        prompt,
        outputSchemaJson: outputSchema,
        modelSelection: input.modelSelection,
      });

      return {
        title: sanitizePrTitle(generated.title),
        body: generated.body.trim(),
      };
    });

  const generateBranchName: TextGeneration.TextGeneration["Service"]["generateBranchName"] =
    Effect.fn("BobShellTextGeneration.generateBranchName")(function* (input) {
      const { prompt, outputSchema } = buildBranchNamePrompt({
        message: input.message,
        attachments: input.attachments,
      });

      const generated = yield* runBobJsonWithTimeout({
        operation: "generateBranchName",
        cwd: input.cwd,
        prompt,
        outputSchemaJson: outputSchema,
        modelSelection: input.modelSelection,
      });

      return { branch: sanitizeBranchFragment(generated.branch) };
    });

  const generateThreadTitle: TextGeneration.TextGeneration["Service"]["generateThreadTitle"] =
    Effect.fn("BobShellTextGeneration.generateThreadTitle")(function* (input) {
      const { prompt, outputSchema } = buildThreadTitlePrompt({
        message: input.message,
        previousTitle: input.previousTitle,
        attachments: input.attachments,
      });

      const generated = yield* runBobJsonWithTimeout({
        operation: "generateThreadTitle",
        cwd: input.cwd,
        prompt,
        outputSchemaJson: outputSchema,
        modelSelection: input.modelSelection,
      });

      return {
        title: sanitizeThreadTitle(generated.title),
      } satisfies TextGeneration.ThreadTitleGenerationResult;
    });

  return {
    generateCommitMessage,
    generatePrContent,
    generateBranchName,
    generateThreadTitle,
  } satisfies TextGeneration.TextGeneration["Service"];
});
