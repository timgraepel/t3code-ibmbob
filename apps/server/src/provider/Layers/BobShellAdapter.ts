/**
 * BobShellAdapter — provider adapter for Bob Shell v2.
 *
 * Each t3code thread maps to a Bob Shell CLI task. Turns are run by
 * spawning `bob run "<prompt>" --format stream-json` in the project
 * working directory. Session continuity across turns uses `--resume <task_id>`,
 * where the task_id comes from the `result` event's `stats.task_id`.
 *
 * Protocol (stream-json events emitted by bob run):
 *   message     → assistant streaming text; `isReasoning: true` marks
 *                 thinking blocks which are suppressed from visible output.
 *                 Role "user" echos are ignored.
 *   tool_use    → item.started; attempt_completion is special-cased to
 *                 publish its parameters.result as the assistant response
 *   tool_result → item.completed
 *   result      → turn.completed (with cost stats, captures task_id for resume)
 *   error       → turn.completed with error state
 *
 * Mode: Bob Shell v2 defaults to `agent` mode with subagents enabled.
 * `--chat-mode` can be overridden via launchArgs. No default override needed.
 *
 * Approval: `--auto-approve` maps to full-access / never-approval-policy.
 * Without it, Bob v2 runs non-interactively but does not prompt (subprocess
 * model is incompatible with interactive approval).
 *
 * @module provider/Layers/BobShellAdapter
 */
import * as nodePath from "node:path";
import {
  type BobShellSettings,
  type ProviderDriverKind,
  type ProviderInstanceId,
  EventId,
  type ProviderRuntimeEvent,
  type ProviderSendTurnInput,
  type ProviderSession,
  type ProviderSessionStartInput,
  type ProviderUserInputAnswers,
  type CanonicalItemType,
  RuntimeItemId,
  type ThreadId,
  TurnId,
} from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as PubSub from "effect/PubSub";
import * as Stream from "effect/Stream";
import * as Schema from "effect/Schema";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import { resolveSpawnCommand } from "@t3tools/shared/shell";
import { tokenizeCliArgs } from "@t3tools/shared/cliArgs";

import {
  ProviderAdapterProcessError,
  ProviderAdapterRequestError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
  type ProviderAdapterError,
} from "../Errors.ts";
import type { ProviderAdapterShape, ProviderThreadSnapshot } from "../Services/ProviderAdapter.ts";
import type { BobShellAdapterShape } from "../Services/BobShellAdapter.ts";
import { makeBobShellEnvironment } from "../Drivers/BobShellHome.ts";

const PROVIDER = "bobShell" as ProviderDriverKind;

// ── Bob Shell v2 stream-json event schemas ────────────────────────────────────

const BobMessageEvent = Schema.Struct({
  type: Schema.Literal("message"),
  role: Schema.String,
  content: Schema.String,
  isReasoning: Schema.optional(Schema.Boolean),
  timestamp: Schema.optional(Schema.String),
});
type BobMessageEvent = typeof BobMessageEvent.Type;

const BobToolUseEvent = Schema.Struct({
  type: Schema.Literal("tool_use"),
  tool_name: Schema.String,
  tool_id: Schema.String,
  parameters: Schema.Unknown,
  timestamp: Schema.optional(Schema.String),
});
type BobToolUseEvent = typeof BobToolUseEvent.Type;

const BobToolResultEvent = Schema.Struct({
  type: Schema.Literal("tool_result"),
  tool_id: Schema.String,
  status: Schema.String,
  output: Schema.optional(Schema.String),
  error: Schema.optional(Schema.Unknown),
  timestamp: Schema.optional(Schema.String),
});
type BobToolResultEvent = typeof BobToolResultEvent.Type;

const BobResultStats = Schema.Struct({
  task_id: Schema.optional(Schema.String),
  duration_ms: Schema.optional(Schema.Number),
  session_costs: Schema.optional(Schema.Number),
  tool_calls: Schema.optional(Schema.Number),
  // Token fields are only present in dev mode
  total_tokens: Schema.optional(Schema.Number),
  input_tokens: Schema.optional(Schema.Number),
  output_tokens: Schema.optional(Schema.Number),
});

const BobResultEvent = Schema.Struct({
  type: Schema.Literal("result"),
  status: Schema.String,
  stats: Schema.optional(BobResultStats),
  timestamp: Schema.optional(Schema.String),
});
type BobResultEvent = typeof BobResultEvent.Type;

const BobErrorEvent = Schema.Struct({
  type: Schema.Literal("error"),
  severity: Schema.optional(Schema.String),
  message: Schema.String,
  timestamp: Schema.optional(Schema.String),
});
type BobErrorEvent = typeof BobErrorEvent.Type;

const BobUnknownEvent = Schema.Struct({ type: Schema.String });

const BobStreamEvent = Schema.Union([
  BobMessageEvent,
  BobToolUseEvent,
  BobToolResultEvent,
  BobResultEvent,
  BobErrorEvent,
  BobUnknownEvent,
]);

const decodeBobStreamEvent = Schema.decodeUnknownOption(Schema.fromJsonString(BobStreamEvent));

// ── Per-thread session state ──────────────────────────────────────────────────

interface BobShellSessionState {
  readonly threadId: ThreadId;
  readonly cwd: string;
  /** Populated from result.stats.task_id — used for --resume on next turn. */
  bobTaskId: string | undefined;
  readonly toolItemTypes: Map<string, CanonicalItemType>;
  activeTurnId: TurnId | undefined;
  activeFiber: Fiber.Fiber<void, unknown> | undefined;
  activeChildProcess: ChildProcessSpawner.ChildProcessHandle | undefined;
  interruptRef: Deferred.Deferred<void, void> | undefined;
  readonly runtimeMode: ProviderSessionStartInput["runtimeMode"];
  readonly createdAt: string;
}

function classifyToolItemType(toolName: string): CanonicalItemType {
  const normalized = toolName.toLowerCase();
  if (normalized.includes("subagent") || normalized.includes("agent")) {
    return "collab_agent_tool_call";
  }
  if (normalized.includes("mcp")) {
    return "mcp_tool_call";
  }
  if (
    normalized.includes("bash") ||
    normalized.includes("command") ||
    normalized.includes("shell")
  ) {
    return "command_execution";
  }
  if (
    normalized.includes("edit") ||
    normalized.includes("write") ||
    normalized.includes("patch") ||
    normalized.includes("replace") ||
    normalized.includes("create") ||
    normalized.includes("delete")
  ) {
    return "file_change";
  }
  if (normalized.includes("image")) {
    return "image_view";
  }
  if (normalized.includes("websearch") || normalized.includes("web_search")) {
    return "web_search";
  }
  return "dynamic_tool_call";
}

// ── Adapter factory ───────────────────────────────────────────────────────────

export const makeBobShellAdapter = Effect.fn("makeBobShellAdapter")(function* (
  bobShellSettings: BobShellSettings,
  options: {
    readonly instanceId: ProviderInstanceId;
    readonly environment: NodeJS.ProcessEnv;
  },
) {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const crypto = yield* Crypto.Crypto;
  const bobEnvironment = yield* makeBobShellEnvironment(bobShellSettings, options.environment);
  // Capture the adapter's own scope so turn fibers outlive the sendTurn call.
  const adapterScope = yield* Effect.scope;

  const sessions = new Map<string, BobShellSessionState>();
  const eventPubSub = yield* PubSub.unbounded<ProviderRuntimeEvent>();
  const publish = (event: ProviderRuntimeEvent) => PubSub.publish(eventPubSub, event);

  // ── Event helpers ──────────────────────────────────────────────────────────

  const makeBase = (threadId: ThreadId, turnId?: TurnId) =>
    Effect.gen(function* () {
      const rawId = yield* crypto.randomUUIDv4.pipe(Effect.orDie);
      const now = yield* DateTime.now.pipe(Effect.orDie);
      return {
        eventId: EventId.make(rawId),
        provider: PROVIDER,
        providerInstanceId: options.instanceId,
        threadId,
        createdAt: DateTime.formatIso(now),
        ...(turnId ? { turnId } : {}),
      } as const;
    });

  // ── Turn runner ────────────────────────────────────────────────────────────

  const runTurn = (
    session: BobShellSessionState,
    turnId: TurnId,
    prompt: string,
    skillPath: string | undefined,
  ): Effect.Effect<void, ProviderAdapterError> =>
    Effect.gen(function* () {
      const binary = bobShellSettings.binaryPath || "bob";
      const extraArgs: string[] = [];

      if (bobShellSettings.teamId.trim()) {
        extraArgs.push("--team-id", bobShellSettings.teamId.trim());
      }
      const apiKey = (bobShellSettings.apiKey ?? "").trim();
      if (apiKey) {
        extraArgs.push("--auth-method", "api-key");
      }
      if (session.bobTaskId) {
        extraArgs.push("--resume", session.bobTaskId);
      }

      // Extra launch args from settings (e.g. --max-cost 2)
      const launchArgTokens = tokenizeCliArgs(bobShellSettings.launchArgs);

      // If a skill is selected via skillPath, use it as the --chat-mode value.
      // Bob built-in modes use their slug as path; custom skills use directory name.
      const selectedSkillArg: string[] = [];
      if (skillPath?.trim()) {
        const skillSlug = nodePath.basename(nodePath.dirname(skillPath)) || skillPath;
        selectedSkillArg.push(`--chat-mode=${skillSlug}`);
      }
      // Default to --chat-mode=agent only if no skill selected and user hasn't overridden via launchArgs
      const modeArgs =
        selectedSkillArg.length > 0 ||
        launchArgTokens.includes("--chat-mode") ||
        launchArgTokens.some((t) => t.startsWith("--chat-mode="))
          ? selectedSkillArg
          : ["--chat-mode=agent"];

      const spawnCommand = yield* resolveSpawnCommand(
        binary,
        [
          "run",
          "--auto-approve",
          "--format",
          "stream-json",
          ...modeArgs,
          ...extraArgs,
          ...launchArgTokens,
          prompt,
        ],
        { env: bobEnvironment },
      ).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderAdapterProcessError({
              provider: PROVIDER,
              threadId: session.threadId,
              detail: "Failed to resolve Bob Shell spawn command.",
              cause,
            }),
        ),
      );

      const command = ChildProcess.make(spawnCommand.command, spawnCommand.args, {
        env: bobEnvironment,
        cwd: session.cwd,
        shell: spawnCommand.shell,
      });

      const child = yield* spawner.spawn(command).pipe(
        Effect.tapError((cause) => Effect.logWarning("Bob Shell process spawn failed", { cause })),
        Effect.mapError(
          (cause) =>
            new ProviderAdapterProcessError({
              provider: PROVIDER,
              threadId: session.threadId,
              detail: "Failed to spawn Bob Shell process.",
              cause,
            }),
        ),
      );

      session.activeChildProcess = child;

      // Emit turn.started
      const turnBase = yield* makeBase(session.threadId, turnId);
      yield* publish({
        ...turnBase,
        type: "turn.started",
        payload: {},
      } satisfies ProviderRuntimeEvent);

      const textItemId = RuntimeItemId.make(`bob-text-${turnId}`);
      let textItemStarted = false;
      let turnEnded = false;

      // Capture stderr lines for error reporting
      const stderrChunks: string[] = [];
      const processStderr = child.stderr.pipe(
        Stream.runForEach((chunk) =>
          Effect.sync(() => {
            const text = typeof chunk === "string" ? chunk : new TextDecoder().decode(chunk);
            for (const line of text.split("\n")) {
              const trimmed = line.trim();
              if (trimmed) {
                stderrChunks.push(trimmed);
              }
            }
          }),
        ),
        Effect.ignore,
      );

      // Process stdout line by line
      const processStdout = child.stdout.pipe(
        Stream.runForEach((chunk) =>
          Effect.gen(function* () {
            const text = typeof chunk === "string" ? chunk : new TextDecoder().decode(chunk);
            for (const line of text.split("\n")) {
              const trimmed = line.trim();
              if (!trimmed || turnEnded) continue;
              const maybeEvent = decodeBobStreamEvent(trimmed);
              if (maybeEvent._tag !== "Some") {
                yield* Effect.logDebug("BobShell: unrecognized stdout line", { line: trimmed });
                continue;
              }
              const event = maybeEvent.value as typeof BobStreamEvent.Type;

              const base = yield* makeBase(session.threadId, turnId);

              if (event.type === "message") {
                const msgEv = event as BobMessageEvent;
                // Skip user-echo messages and thinking/reasoning blocks
                if (msgEv.role !== "assistant" || !msgEv.content) continue;
                // Bob v2 internal reasoning/thinking blocks are not surfaced to the user.
                // Unlike Claude's extended thinking (which is a deliberate user feature),
                // Bob's reasoning is internal scaffolding. Suppress silently.
                if (msgEv.isReasoning) continue;
                const content = msgEv.content;
                if (/^\[using tool /i.test(content)) continue;
                if (!textItemStarted) {
                  textItemStarted = true;
                  yield* publish({
                    ...base,
                    itemId: textItemId,
                    type: "item.started",
                    payload: { itemType: "assistant_message", title: "Response" },
                  } satisfies ProviderRuntimeEvent);
                }
                yield* publish({
                  ...base,
                  itemId: textItemId,
                  type: "content.delta",
                  payload: { streamKind: "assistant_text", delta: content },
                } satisfies ProviderRuntimeEvent);
              } else if (event.type === "tool_use") {
                const toolEv = event as BobToolUseEvent;
                if (toolEv.tool_name === "attempt_completion") {
                  const params = toolEv.parameters as Record<string, unknown> | null | undefined;
                  const result = typeof params?.result === "string" ? params.result : undefined;
                  if (result) {
                    if (!textItemStarted) {
                      textItemStarted = true;
                      yield* publish({
                        ...base,
                        itemId: textItemId,
                        type: "item.started",
                        payload: { itemType: "assistant_message", title: "Response" },
                      } satisfies ProviderRuntimeEvent);
                    }
                    yield* publish({
                      ...base,
                      itemId: textItemId,
                      type: "content.delta",
                      payload: { streamKind: "assistant_text", delta: result },
                    } satisfies ProviderRuntimeEvent);
                  }
                  continue;
                }
                const toolItemId = RuntimeItemId.make(toolEv.tool_id);
                const itemType = classifyToolItemType(toolEv.tool_name);
                session.toolItemTypes.set(toolEv.tool_id, itemType);
                const params = toolEv.parameters as Record<string, unknown> | null | undefined;
                const isSubagent = toolEv.tool_name === "spawn_subagent";
                const subagentName = typeof params?.name === "string" ? params.name : undefined;
                const subagentDesc =
                  typeof params?.description === "string" ? params.description : undefined;
                const toolTitle = isSubagent
                  ? `Subagent${subagentName ? ` (${subagentName})` : ""}`
                  : toolEv.tool_name;
                yield* publish({
                  ...base,
                  itemId: toolItemId,
                  type: "item.started",
                  payload: {
                    itemType,
                    title: toolTitle,
                    status: "inProgress",
                    ...(isSubagent && subagentDesc ? { detail: subagentDesc } : {}),
                    data: {
                      toolName: toolEv.tool_name,
                      parameters: toolEv.parameters,
                    },
                  },
                } satisfies ProviderRuntimeEvent);
              } else if (event.type === "tool_result") {
                const resEv = event as BobToolResultEvent;
                const toolItemId = RuntimeItemId.make(resEv.tool_id);
                yield* publish({
                  ...base,
                  itemId: toolItemId,
                  type: "item.completed",
                  payload: {
                    itemType: session.toolItemTypes.get(resEv.tool_id) ?? "dynamic_tool_call",
                    status: resEv.status === "success" ? "completed" : "failed",
                    ...(resEv.output ? { detail: resEv.output } : {}),
                    data: {
                      status: resEv.status,
                      ...(resEv.output !== undefined ? { output: resEv.output } : {}),
                    },
                  },
                } satisfies ProviderRuntimeEvent);
              } else if (event.type === "result") {
                const resEv = event as BobResultEvent;
                turnEnded = true;
                // Capture task_id for --resume on the next turn
                if (resEv.stats?.task_id) {
                  session.bobTaskId = resEv.stats.task_id;
                }
                session.activeTurnId = undefined;
                session.activeChildProcess = undefined;

                if (resEv.stats) {
                  const usedTokens = resEv.stats.total_tokens ?? 0;
                  if (usedTokens > 0 || resEv.stats.input_tokens !== undefined) {
                    yield* publish({
                      ...base,
                      type: "thread.token-usage.updated",
                      payload: {
                        usage: {
                          usedTokens,
                          ...(resEv.stats.total_tokens !== undefined
                            ? { totalProcessedTokens: resEv.stats.total_tokens }
                            : {}),
                          ...(resEv.stats.input_tokens !== undefined
                            ? { inputTokens: resEv.stats.input_tokens }
                            : {}),
                          ...(resEv.stats.output_tokens !== undefined
                            ? { outputTokens: resEv.stats.output_tokens }
                            : {}),
                          ...(resEv.stats.duration_ms !== undefined
                            ? { durationMs: resEv.stats.duration_ms }
                            : {}),
                        },
                      },
                    } satisfies ProviderRuntimeEvent);
                  }
                }

                if (textItemStarted) {
                  yield* publish({
                    ...base,
                    itemId: textItemId,
                    type: "item.completed",
                    payload: { itemType: "assistant_message", status: "completed" },
                  } satisfies ProviderRuntimeEvent);
                }
                yield* publish({
                  ...base,
                  type: "turn.completed",
                  payload: {
                    state: resEv.status === "success" ? "completed" : "failed",
                    stopReason: resEv.status,
                    ...(resEv.stats?.session_costs !== undefined
                      ? { totalCostUsd: resEv.stats.session_costs }
                      : {}),
                  },
                } satisfies ProviderRuntimeEvent);
              } else if (event.type === "error") {
                const errEv = event as BobErrorEvent;
                turnEnded = true;
                session.activeTurnId = undefined;
                session.activeChildProcess = undefined;

                yield* publish({
                  ...base,
                  itemId: RuntimeItemId.make(`bob-err-${turnId}`),
                  type: "item.completed",
                  payload: {
                    itemType: "error",
                    status: "failed",
                    title: "Bob Shell error",
                    detail: errEv.message,
                  },
                } satisfies ProviderRuntimeEvent);
                yield* publish({
                  ...base,
                  type: "turn.completed",
                  payload: { state: "failed", stopReason: "error" },
                } satisfies ProviderRuntimeEvent);
              } else {
                yield* Effect.logDebug("BobShell: unhandled stream event type", {
                  type: (event as { type: string }).type,
                });
              }
            }
          }),
        ),
        Effect.mapError(
          (cause) =>
            new ProviderAdapterProcessError({
              provider: PROVIDER,
              threadId: session.threadId,
              detail: "Bob Shell stdout processing failed.",
              cause,
            }),
        ),
      );

      const [, , exitCode] = yield* Effect.all(
        [
          processStdout,
          processStderr,
          child.exitCode.pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapterProcessError({
                  provider: PROVIDER,
                  threadId: session.threadId,
                  detail: "Failed to read Bob Shell exit code.",
                  cause,
                }),
            ),
          ),
        ] as const,
        { concurrency: "unbounded" },
      );

      session.activeChildProcess = undefined;

      if (!turnEnded) {
        const base = yield* makeBase(session.threadId, turnId);
        const stderrDetail = stderrChunks.length > 0 ? stderrChunks.join("\n") : undefined;

        if (exitCode !== 0 && stderrDetail) {
          yield* publish({
            ...base,
            itemId: RuntimeItemId.make(`bob-err-${turnId}`),
            type: "item.completed",
            payload: {
              itemType: "error",
              status: "failed",
              title: "Bob Shell process error",
              detail: stderrDetail,
            },
          } satisfies ProviderRuntimeEvent);
        }

        yield* publish({
          ...base,
          type: "turn.completed",
          payload: {
            state: exitCode === 0 ? "completed" : "failed",
            stopReason: exitCode !== 0 ? `exit_code_${exitCode}` : "end_turn",
          },
        } satisfies ProviderRuntimeEvent);
      }
    }).pipe(Effect.scoped);

  // ── Adapter operations ─────────────────────────────────────────────────────

  const startSession: ProviderAdapterShape<ProviderAdapterError>["startSession"] = (input) =>
    Effect.gen(function* () {
      const base = yield* makeBase(input.threadId);
      const now = base.createdAt;

      if (sessions.has(input.threadId)) {
        const existing = sessions.get(input.threadId)!;
        yield* publish({
          ...base,
          type: "session.started",
          payload: { ...(existing.bobTaskId ? { resume: existing.bobTaskId } : {}) },
        } satisfies ProviderRuntimeEvent);
        return {
          provider: PROVIDER,
          providerInstanceId: options.instanceId,
          status: "ready",
          runtimeMode: input.runtimeMode,
          cwd: existing.cwd,
          threadId: input.threadId,
          createdAt: existing.createdAt,
          updatedAt: now,
        } satisfies ProviderSession;
      }

      const cwd = input.cwd ?? process.cwd();
      const state: BobShellSessionState = {
        threadId: input.threadId,
        cwd,
        bobTaskId: undefined,
        toolItemTypes: new Map(),
        activeTurnId: undefined,
        activeFiber: undefined,
        activeChildProcess: undefined,
        interruptRef: undefined,
        runtimeMode: input.runtimeMode,
        createdAt: now,
      };
      sessions.set(input.threadId, state);

      yield* publish({
        ...base,
        type: "thread.started",
        payload: {},
      } satisfies ProviderRuntimeEvent);
      yield* publish({
        ...base,
        type: "session.started",
        payload: {},
      } satisfies ProviderRuntimeEvent);

      return {
        provider: PROVIDER,
        providerInstanceId: options.instanceId,
        status: "ready",
        runtimeMode: input.runtimeMode,
        cwd,
        threadId: input.threadId,
        createdAt: now,
        updatedAt: now,
      } satisfies ProviderSession;
    }).pipe(Effect.mapError((e) => e as ProviderAdapterError));

  const sendTurn: ProviderAdapterShape<ProviderAdapterError>["sendTurn"] = (input) =>
    Effect.gen(function* () {
      const session = sessions.get(input.threadId);
      if (!session) {
        return yield* new ProviderAdapterSessionNotFoundError({
          provider: PROVIDER,
          threadId: input.threadId,
        });
      }
      if (!input.input && !input.continuation) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "sendTurn",
          issue: "Bob Shell requires a non-empty prompt.",
        });
      }

      const uuid = yield* crypto.randomUUIDv4.pipe(Effect.orDie);
      const turnId = TurnId.make(`bob-turn-${uuid}`);
      const prompt = input.input ?? "";

      session.activeTurnId = turnId;
      const interrupt = yield* Deferred.make<void, void>();
      session.interruptRef = interrupt;

      const turnFiber = yield* runTurn(session, turnId, prompt, input.skillPath).pipe(
        Effect.race(
          Deferred.await(interrupt).pipe(
            Effect.flatMap(() =>
              Effect.gen(function* () {
                const base = yield* makeBase(session.threadId, turnId);
                yield* publish({
                  ...base,
                  type: "turn.completed",
                  payload: { state: "interrupted", stopReason: "interrupted" },
                } satisfies ProviderRuntimeEvent);
              }),
            ),
          ),
        ),
        Effect.forkIn(adapterScope),
      );
      session.activeFiber = turnFiber;

      return {
        threadId: input.threadId,
        turnId,
      } as const;
    }).pipe(Effect.mapError((e) => e as ProviderAdapterError));

  const interruptTurn: ProviderAdapterShape<ProviderAdapterError>["interruptTurn"] = (
    threadId,
    _turnId,
  ) =>
    Effect.gen(function* () {
      const session = sessions.get(threadId);
      if (!session) return;
      if (session.activeChildProcess) {
        yield* session.activeChildProcess.kill({ forceKillAfter: "1 second" }).pipe(Effect.ignore);
        session.activeChildProcess = undefined;
      }
      if (session.interruptRef) {
        yield* Deferred.succeed(session.interruptRef, undefined);
      }
      if (session.activeFiber) {
        yield* Fiber.interrupt(session.activeFiber).pipe(Effect.ignore);
      }
    }).pipe(Effect.mapError((e) => e as ProviderAdapterError));

  const stopSession: ProviderAdapterShape<ProviderAdapterError>["stopSession"] = (threadId) =>
    Effect.gen(function* () {
      const session = sessions.get(threadId);
      if (!session) return;
      yield* interruptTurn(threadId);
      sessions.delete(threadId);
      const base = yield* makeBase(threadId);
      yield* publish({
        ...base,
        type: "session.exited",
        payload: {},
      } satisfies ProviderRuntimeEvent);
    }).pipe(Effect.mapError((e) => e as ProviderAdapterError));

  const respondToRequest: ProviderAdapterShape<ProviderAdapterError>["respondToRequest"] = () =>
    Effect.fail(
      new ProviderAdapterRequestError({
        provider: PROVIDER,
        method: "respondToRequest",
        detail: "Not supported.",
      }),
    );

  const respondToUserInput: ProviderAdapterShape<ProviderAdapterError>["respondToUserInput"] = () =>
    Effect.fail(
      new ProviderAdapterRequestError({
        provider: PROVIDER,
        method: "respondToUserInput",
        detail: "Bob Shell does not support interactive user-input requests.",
      }),
    );

  const listSessions: ProviderAdapterShape<ProviderAdapterError>["listSessions"] = () =>
    DateTime.now.pipe(
      Effect.orDie,
      Effect.map((now) => {
        const nowStr = DateTime.formatIso(now);
        return Array.from(sessions.values()).map((s): ProviderSession => ({
          provider: PROVIDER,
          providerInstanceId: options.instanceId,
          status: s.activeTurnId ? "running" : "ready",
          runtimeMode: s.runtimeMode,
          cwd: s.cwd,
          threadId: s.threadId,
          activeTurnId: s.activeTurnId,
          createdAt: s.createdAt,
          updatedAt: nowStr,
        }));
      }),
    );

  const hasSession: ProviderAdapterShape<ProviderAdapterError>["hasSession"] = (threadId) =>
    Effect.succeed(sessions.has(threadId));

  const readThread: ProviderAdapterShape<ProviderAdapterError>["readThread"] = (threadId) =>
    Effect.gen(function* () {
      // Bob v2 stores conversation history in its internal SQLite database.
      // This is not accessible via the CLI, so we always return an empty snapshot.
      // T3 Code checkpoints serve as the rollback mechanism instead.
      return { threadId, turns: [] } satisfies ProviderThreadSnapshot;
    });

  const rollbackThread: ProviderAdapterShape<ProviderAdapterError>["rollbackThread"] = (threadId) =>
    Effect.succeed({ threadId, turns: [] } satisfies ProviderThreadSnapshot);

  const stopAll: ProviderAdapterShape<ProviderAdapterError>["stopAll"] = () =>
    Effect.forEach(Array.from(sessions.keys()), (threadId) => stopSession(threadId as ThreadId), {
      concurrency: "unbounded",
    }).pipe(Effect.asVoid);

  const streamEvents: ProviderAdapterShape<ProviderAdapterError>["streamEvents"] =
    Stream.fromPubSub(eventPubSub);

  return {
    provider: PROVIDER,
    capabilities: {
      sessionModelSwitch: "unsupported" as const,
      promptlessTurnContinuation: false,
      supportsConversationRollback: false,
    },
    startSession,
    sendTurn,
    interruptTurn,
    respondToRequest,
    respondToUserInput,
    stopSession,
    listSessions,
    hasSession,
    readThread,
    rollbackThread,
    stopAll,
    streamEvents,
  } satisfies BobShellAdapterShape;
});
