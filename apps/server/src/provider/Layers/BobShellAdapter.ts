/**
 * BobShellAdapter — provider adapter for Bob Shell v2 using ACP mode.
 *
 * Spawns `bob acp --auto-approve` as a persistent ACP server per thread.
 * Sessions survive across turns via the ACP session resume cursor.
 *
 * @module provider/Layers/BobShellAdapter
 */
import {
  ApprovalRequestId,
  EventId,
  type BobShellSettings,
  type ProviderApprovalDecision,
  type ProviderDriverKind,
  type ProviderInstanceId,
  type ProviderRuntimeEvent,
  type ProviderSession,
  type ProviderUserInputAnswers,
  RuntimeRequestId,
  type ThreadId,
  TurnId,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as PubSub from "effect/PubSub";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as Option from "effect/Option";
import * as SynchronizedRef from "effect/SynchronizedRef";
import * as Layer from "effect/Layer";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";
import * as EffectAcpErrors from "effect-acp/errors";
import type * as EffectAcpSchema from "effect-acp/schema";

import { buildRuntimeInstructions } from "../RuntimeInstructions.ts";
import * as McpProviderSession from "../../mcp/McpProviderSession.ts";
import {
  ProviderAdapterProcessError,
  ProviderAdapterRequestError,
  ProviderAdapterSessionClosedError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
  type ProviderAdapterError,
} from "../Errors.ts";
import { mapAcpToAdapterError } from "../acp/AcpAdapterSupport.ts";
import * as AcpSessionRuntime from "../acp/AcpSessionRuntime.ts";
import {
  makeAcpAssistantItemEvent,
  makeAcpContentDeltaEvent,
  makeAcpRequestOpenedEvent,
  makeAcpRequestResolvedEvent,
  makeAcpToolCallEvent,
} from "../acp/AcpCoreRuntimeEvents.ts";
import { parsePermissionRequest } from "../acp/AcpRuntimeModel.ts";
import type { ProviderAdapterShape, ProviderThreadSnapshot } from "../Services/ProviderAdapter.ts";
import type { BobShellAdapterShape } from "../Services/BobShellAdapter.ts";
import { makeBobShellEnvironment } from "../Drivers/BobShellHome.ts";

const PROVIDER = "bobShell" as ProviderDriverKind;

// ── Spawn helpers ─────────────────────────────────────────────────────────────

export function buildBobShellAcpSpawnInput(
  settings: Pick<BobShellSettings, "binaryPath">,
  cwd: string,
  environment: NodeJS.ProcessEnv,
): AcpSessionRuntime.AcpSpawnInput {
  return {
    command: settings.binaryPath || "bob",
    args: ["acp", "--auto-approve"],
    cwd,
    env: environment,
  };
}

function resolveAuthMethodId(settings: Pick<BobShellSettings, "apiKey">): string {
  return (settings.apiKey ?? "").trim() ? "api_key" : "sso";
}

// ── Per-thread session state ──────────────────────────────────────────────────

interface PendingApproval {
  readonly request: EffectAcpSchema.RequestPermissionRequest;
  readonly response: Deferred.Deferred<{
    readonly decision: ProviderApprovalDecision;
    readonly result: EffectAcpSchema.RequestPermissionResponse;
  }>;
}

interface PendingQuestion {
  readonly request: EffectAcpSchema.RequestPermissionRequest;
  readonly response: Deferred.Deferred<{
    readonly answers: ProviderUserInputAnswers;
    readonly result: EffectAcpSchema.RequestPermissionResponse;
  }>;
}

interface SessionContext {
  readonly threadId: ThreadId;
  readonly cwd: string;
  readonly nativeSessionId: string;
  readonly scope: Scope.Closeable;
  readonly runtime: AcpSessionRuntime.AcpSessionRuntime["Service"];
  readonly promptLock: Semaphore.Semaphore;
  readonly stopLock: Semaphore.Semaphore;
  readonly approvals: Map<ApprovalRequestId, PendingApproval>;
  readonly questions: Map<ApprovalRequestId, PendingQuestion>;
  readonly turns: Array<{ id: TurnId; items: Array<unknown> }>;
  session: ProviderSession;
  activeTurnId: TurnId | undefined;
  promptFiber: Fiber.Fiber<EffectAcpSchema.PromptResponse, EffectAcpErrors.AcpError> | undefined;
  generation: number;
  stopped: boolean;
  closed: boolean;
  disconnected: boolean;
}

interface TurnIntent {
  readonly turnId: TurnId;
  readonly generation: number;
  settled: boolean;
}

const ResumeCursor = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  sessionId: Schema.NonEmptyString,
});
const decodeResumeCursor = Schema.decodeUnknownOption(ResumeCursor);
const isAcpError = Schema.is(EffectAcpErrors.AcpError);

// ── Adapter factory ───────────────────────────────────────────────────────────

export const makeBobShellAdapter = Effect.fn("makeBobShellAdapter")(function* (
  settings: BobShellSettings,
  options: {
    readonly instanceId: ProviderInstanceId;
    readonly environment: NodeJS.ProcessEnv;
  },
) {
  const crypto = yield* Crypto.Crypto;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const ownerScope = yield* Effect.scope;

  const bobEnvironment = yield* makeBobShellEnvironment(settings, options.environment);

  const sessions = new Map<ThreadId, SessionContext>();
  const locks = yield* SynchronizedRef.make(new Map<ThreadId, Semaphore.Semaphore>());
  const events = yield* PubSub.unbounded<ProviderRuntimeEvent>();

  const nowIso = Effect.map(DateTime.now, DateTime.formatIso);
  const randomId = crypto.randomUUIDv4.pipe(
    Effect.mapError(
      (cause) =>
        new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "crypto/randomUUIDv4",
          detail: "Could not create a Bob Shell event ID.",
          cause,
        }),
    ),
  );
  const stamp = Effect.all({
    eventId: Effect.map(randomId, EventId.make),
    createdAt: nowIso,
  });
  const emit = (event: ProviderRuntimeEvent) => PubSub.publish(events, event).pipe(Effect.asVoid);

  const withThreadLock = <A, E, R>(threadId: ThreadId, task: Effect.Effect<A, E, R>) =>
    SynchronizedRef.modifyEffect(locks, (current) => {
      const existing = current.get(threadId);
      if (existing) return Effect.succeed([existing, current] as const);
      return Semaphore.make(1).pipe(
        Effect.map((lock) => [lock, new Map(current).set(threadId, lock)] as const),
      );
    }).pipe(Effect.flatMap((lock) => lock.withPermit(task)));

  const requireSession = (threadId: ThreadId) => {
    const context = sessions.get(threadId);
    return context && !context.stopped
      ? Effect.succeed(context)
      : Effect.fail(new ProviderAdapterSessionNotFoundError({ provider: PROVIDER, threadId }));
  };

  const cancelRequests = (context: SessionContext) =>
    Effect.gen(function* () {
      for (const pending of context.approvals.values()) {
        yield* Deferred.succeed(pending.response, {
          decision: "cancel",
          result: { outcome: { outcome: "cancelled" } },
        });
      }
      for (const pending of context.questions.values()) {
        yield* Deferred.succeed(pending.response, {
          answers: {},
          result: { outcome: { outcome: "cancelled" } },
        });
      }
    });

  const stopContext = (context: SessionContext) =>
    context.stopLock
      .withPermit(
        Effect.gen(function* () {
          if (context.closed) return;
          context.stopped = true;
          yield* Effect.gen(function* () {
            yield* cancelRequests(context);
            if (context.promptFiber && !context.disconnected) {
              yield* Effect.ignore(context.runtime.cancel);
            }
          }).pipe(Effect.ensuring(Scope.close(context.scope, Exit.void)));
          context.closed = true;
          if (sessions.get(context.threadId) === context) sessions.delete(context.threadId);
          yield* emit({
            type: "session.exited",
            ...(yield* stamp),
            provider: PROVIDER,
            threadId: context.threadId,
            payload: {
              exitKind: context.disconnected ? "error" : "graceful",
              ...(context.disconnected ? { reason: "Bob Shell process stopped." } : {}),
            },
          });
        }),
      )
      .pipe(Effect.uninterruptible);

  const handlePermission = Effect.fn("BobShellAdapter.handlePermission")(function* (
    context: SessionContext,
    request: EffectAcpSchema.RequestPermissionRequest,
  ): Effect.fn.Return<EffectAcpSchema.RequestPermissionResponse, ProviderAdapterError> {
    if (context.stopped || request.sessionId !== context.nativeSessionId) {
      return { outcome: { outcome: "cancelled" } };
    }
    const requestId = ApprovalRequestId.make(yield* randomId);
    const runtimeRequestId = RuntimeRequestId.make(requestId);
    const turnId = context.activeTurnId;

    const response = yield* Deferred.make<{
      decision: ProviderApprovalDecision;
      result: EffectAcpSchema.RequestPermissionResponse;
    }>();
    context.approvals.set(requestId, { request, response });
    const parsed = parsePermissionRequest(request);
    return yield* Effect.gen(function* () {
      yield* emit(
        makeAcpRequestOpenedEvent({
          stamp: yield* stamp,
          provider: PROVIDER,
          threadId: context.threadId,
          turnId,
          requestId: runtimeRequestId,
          permissionRequest: parsed,
          detail: parsed.toolCall?.command ?? parsed.toolCall?.title ?? "Bob requests permission.",
          args: request,
          source: "acp.jsonrpc",
          method: "session/request_permission",
          rawPayload: request,
        }),
      );
      const answer = yield* Deferred.await(response);
      yield* emit(
        makeAcpRequestResolvedEvent({
          stamp: yield* stamp,
          provider: PROVIDER,
          threadId: context.threadId,
          turnId,
          requestId: runtimeRequestId,
          permissionRequest: parsed,
          decision: answer.decision,
        }),
      );
      return answer.result;
    }).pipe(Effect.ensuring(Effect.sync(() => context.approvals.delete(requestId))));
  });

  const handleEvent = Effect.fn("BobShellAdapter.handleEvent")(function* (
    context: SessionContext,
    event: AcpSessionRuntime.AcpSessionRuntimeEvent,
  ) {
    if (event._tag === "EventStreamBarrier") {
      yield* Deferred.succeed(event.acknowledge, undefined);
      return;
    }
    if (context.stopped) return;
    switch (event._tag) {
      case "ModeChanged":
      case "ConfigOptionsUpdated":
        return;
      case "AvailableCommandsUpdated":
        return;
      case "ConnectionTerminated":
        context.stopped = true;
        context.disconnected = true;
        yield* stopContext(context).pipe(Effect.forkIn(ownerScope));
        return;
      case "AssistantItemStarted":
      case "AssistantItemCompleted":
        yield* emit(
          makeAcpAssistantItemEvent({
            stamp: yield* stamp,
            provider: PROVIDER,
            threadId: context.threadId,
            turnId: context.activeTurnId,
            itemId: event.itemId,
            lifecycle: event._tag === "AssistantItemStarted" ? "item.started" : "item.completed",
          }),
        );
        return;
      case "ThoughtDelta":
      case "ContentDelta":
        yield* emit(
          makeAcpContentDeltaEvent({
            stamp: yield* stamp,
            provider: PROVIDER,
            threadId: context.threadId,
            turnId: context.activeTurnId,
            ...(event._tag === "ContentDelta" && event.itemId ? { itemId: event.itemId } : {}),
            ...(event._tag === "ThoughtDelta" ? { streamKind: "reasoning_text" } : {}),
            text: event.text,
            rawPayload: event.rawPayload,
          }),
        );
        return;
      case "PlanUpdated":
        return;
      case "ToolCallUpdated":
        yield* emit(
          makeAcpToolCallEvent({
            stamp: yield* stamp,
            provider: PROVIDER,
            threadId: context.threadId,
            turnId: context.activeTurnId,
            toolCall: event.toolCall,
            rawPayload: event.rawPayload,
          }),
        );
        return;
    }
  });

  const startSession: ProviderAdapterShape<ProviderAdapterError>["startSession"] = (input) =>
    withThreadLock(
      input.threadId,
      Effect.gen(function* () {
        if (!settings.enabled) {
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "startSession",
            issue: "Enable Bob Shell in provider settings before starting a thread.",
          });
        }
        if (!input.cwd?.trim()) {
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "startSession",
            issue: "The session requires a workspace directory.",
          });
        }
        const cursor = decodeResumeCursor(input.resumeCursor);
        if (input.resumeCursor !== undefined && Option.isNone(cursor)) {
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "startSession",
            issue: "The saved Bob Shell session is invalid. Start a new thread.",
          });
        }
        const previous = sessions.get(input.threadId);
        if (previous) yield* stopContext(previous);
        const cwd = input.cwd.trim();
        const sessionScope = yield* Scope.make("sequential");
        let transferred = false;
        let context: SessionContext | undefined;
        yield* Effect.addFinalizer(() => {
          if (transferred) return Effect.void;
          sessions.delete(input.threadId);
          return Scope.close(sessionScope, Exit.void);
        });

        const mcp = McpProviderSession.readMcpProviderSession(input.threadId);
        const spawnInput = buildBobShellAcpSpawnInput(settings, cwd, bobEnvironment);
        const resumeSessionId =
          Option.isSome(cursor) ? cursor.value.sessionId : undefined;
        const runtime = yield* Effect.gen(function* () {
          const acpContext = yield* Layer.build(
            AcpSessionRuntime.layer({
              spawn: spawnInput,
              cwd,
              clientInfo: { name: "t3-code", version: "0.0.0" },
              authMethodId: resolveAuthMethodId(settings),
              ...(resumeSessionId ? { resumeSessionId } : {}),
              ...(mcp
                ? {
                    mcpServers: [
                      {
                        type: "http" as const,
                        name: "t3-code",
                        url: mcp.endpoint,
                        headers: [{ name: "Authorization", value: mcp.authorizationHeader }],
                      },
                    ],
                  }
                : {}),
            }).pipe(
              Layer.provide(
                Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawner),
              ),
            ),
          );
          return yield* Effect.service(AcpSessionRuntime.AcpSessionRuntime).pipe(
            Effect.provide(acpContext),
          );
        }).pipe(
          Effect.provideService(Scope.Scope, sessionScope),
          Effect.provideService(Crypto.Crypto, crypto),
          Effect.mapError(
            (cause) =>
              new ProviderAdapterProcessError({
                provider: PROVIDER,
                threadId: input.threadId,
                detail: "Failed to build Bob Shell ACP runtime.",
                cause,
              }),
          ),
        );

        yield* runtime.handleRequestPermission((request) =>
          context
            ? handlePermission(context, request).pipe(
                Effect.mapError((cause) =>
                  EffectAcpErrors.AcpRequestError.internalError(
                    "Could not process a Bob Shell permission request.",
                    undefined,
                    { cause },
                  ),
                ),
              )
            : Effect.succeed({ outcome: { outcome: "cancelled" } } satisfies EffectAcpSchema.RequestPermissionResponse),
        );

        const started = yield* runtime.start().pipe(
          Effect.mapError((cause) =>
            isAcpError(cause)
              ? mapAcpToAdapterError(PROVIDER, input.threadId, "session/start", cause)
              : new ProviderAdapterProcessError({
                  provider: PROVIDER,
                  threadId: input.threadId,
                  detail: "Failed to start Bob Shell ACP session.",
                  cause,
                }),
          ),
        );

        const createdAt = yield* nowIso;
        const session: ProviderSession = {
          provider: PROVIDER,
          providerInstanceId: options.instanceId,
          threadId: input.threadId,
          cwd,
          status: "ready",
          runtimeMode: input.runtimeMode,
          resumeCursor: { schemaVersion: 1, sessionId: started.sessionId },
          createdAt,
          updatedAt: createdAt,
        };
        context = {
          threadId: input.threadId,
          cwd,
          nativeSessionId: started.sessionId,
          scope: sessionScope,
          runtime,
          promptLock: yield* Semaphore.make(1),
          stopLock: yield* Semaphore.make(1),
          approvals: new Map(),
          questions: new Map(),
          turns: [],
          session,
          activeTurnId: undefined,
          promptFiber: undefined,
          generation: 0,
          stopped: false,
          closed: false,
          disconnected: false,
        };
        const running = context;
        sessions.set(input.threadId, running);
        yield* Stream.runForEach(runtime.getEvents(), (event) =>
          handleEvent(running, event),
        ).pipe(
          Effect.catchCause(() => Effect.logError("Could not process a Bob Shell runtime event.")),
          Effect.forkIn(sessionScope),
        );
        yield* emit({
          type: "thread.started",
          ...(yield* stamp),
          provider: PROVIDER,
          threadId: input.threadId,
          payload: { providerThreadId: started.sessionId },
        });
        yield* emit({
          type: "session.started",
          ...(yield* stamp),
          provider: PROVIDER,
          threadId: input.threadId,
          payload: { resume: started.initializeResult },
        });
        yield* runtime.drainEvents;
        if (running.stopped) {
          return yield* new ProviderAdapterSessionClosedError({
            provider: PROVIDER,
            threadId: input.threadId,
          });
        }
        transferred = true;
        return session;
      }).pipe(Effect.scoped),
    );

  const sendTurn: ProviderAdapterShape<ProviderAdapterError>["sendTurn"] = Effect.fn(
    "BobShellAdapter.sendTurn",
  )(function* (input) {
    const context = yield* requireSession(input.threadId);
    const promptText = input.input ?? "";
    let intent: TurnIntent | undefined;

    const finishTurn = (
      turn: TurnIntent,
      payload: {
        state: "completed" | "failed" | "cancelled";
        stopReason?: string | null;
        errorMessage?: string;
      },
    ) =>
      Effect.gen(function* () {
        if (turn.settled || context.stopped || context.generation !== turn.generation) return;
        turn.settled = true;
        context.activeTurnId = undefined;
        context.promptFiber = undefined;
        context.session = {
          ...context.session,
          status: payload.state === "failed" ? "error" : "ready",
          activeTurnId: undefined,
          updatedAt: yield* nowIso,
          ...(payload.errorMessage ? { lastError: payload.errorMessage } : { lastError: undefined }),
        };
        yield* emit({
          type: "turn.completed",
          ...(yield* stamp),
          provider: PROVIDER,
          threadId: input.threadId,
          turnId: turn.turnId,
          payload,
        });
      }).pipe(Effect.uninterruptible);

    return yield* Effect.gen(function* () {
      const launch = yield* context.promptLock.withPermit(
        Effect.gen(function* () {
          yield* requireSession(input.threadId);
          const turnId = context.activeTurnId ?? TurnId.make(yield* randomId);
          const steering = context.activeTurnId !== undefined;
          const turn: TurnIntent = { turnId, generation: ++context.generation, settled: false };
          intent = turn;
          context.activeTurnId = turnId;
          if (!steering) {
            yield* emit({
              type: "turn.started",
              ...(yield* stamp),
              provider: PROVIDER,
              threadId: input.threadId,
              turnId,
              payload: {},
            });
          }
          if (context.promptFiber) {
            yield* cancelRequests(context);
            yield* context.runtime.cancel;
            yield* Fiber.await(context.promptFiber);
          }
          context.session = {
            ...context.session,
            status: "running",
            activeTurnId: turnId,
            updatedAt: yield* nowIso,
          };
          const dispatched = yield* Deferred.make<void>();
          const fiber = yield* context.runtime
            .prompt(
              {
                prompt: [
                  { type: "text", text: promptText },
                  { type: "text", text: buildRuntimeInstructions({ harness: "Bob Shell" }) },
                ],
              },
              { dispatched },
            )
            .pipe(Effect.forkIn(context.scope));
          context.promptFiber = fiber;
          yield* Effect.raceFirst(
            Deferred.await(dispatched),
            Fiber.await(fiber).pipe(
              Effect.flatMap((exit) => exit),
              Effect.asVoid,
            ),
          );
          return { turn, fiber };
        }),
      );
      const result = yield* Fiber.await(launch.fiber).pipe(Effect.flatMap((exit) => exit));
      yield* context.runtime.drainEvents;
      if (context.stopped) {
        return yield* new ProviderAdapterSessionClosedError({
          provider: PROVIDER,
          threadId: input.threadId,
        });
      }
      const record = context.turns.find((t) => t.id === launch.turn.turnId);
      if (record) record.items.push(result);
      else context.turns.push({ id: launch.turn.turnId, items: [result] });
      yield* context.promptLock.withPermit(
        finishTurn(launch.turn, {
          state: result.stopReason === "cancelled" ? "cancelled" : "completed",
          stopReason: result.stopReason,
        }),
      );
      return {
        threadId: input.threadId,
        turnId: launch.turn.turnId,
        resumeCursor: context.session.resumeCursor,
      };
    }).pipe(
      Effect.mapError((cause) =>
        isAcpError(cause) ? mapAcpToAdapterError(PROVIDER, input.threadId, "session/prompt", cause) : cause,
      ),
      Effect.tapError((cause) =>
        Effect.suspend(() =>
          intent
            ? context.promptLock.withPermit(
                finishTurn(intent, {
                  state: "failed",
                  errorMessage: (cause as { message?: string }).message ?? String(cause),
                }),
              )
            : Effect.void,
        ),
      ),
      Effect.onInterrupt(() =>
        context.promptLock.withPermit(
          Effect.gen(function* () {
            const turn = intent;
            if (!turn || turn.settled || context.stopped || context.generation !== turn.generation) return;
            const promptFiber = context.promptFiber;
            yield* cancelRequests(context);
            yield* Effect.ignore(context.runtime.cancel);
            if (promptFiber) yield* Fiber.interrupt(promptFiber);
            yield* finishTurn(turn, { state: "cancelled", stopReason: "cancelled" });
          }),
        ),
      ),
    );
  });

  const interruptTurn: ProviderAdapterShape<ProviderAdapterError>["interruptTurn"] = (threadId) =>
    Effect.gen(function* () {
      const context = yield* requireSession(threadId);
      yield* context.promptLock
        .withPermit(
          Effect.gen(function* () {
            yield* cancelRequests(context);
            yield* context.runtime.cancel;
          }),
        )
        .pipe(
          Effect.mapError((cause) => mapAcpToAdapterError(PROVIDER, threadId, "session/cancel", cause)),
        );
    });

  const respondToRequest: ProviderAdapterShape<ProviderAdapterError>["respondToRequest"] = (
    threadId,
    requestId,
    decision,
  ) =>
    Effect.gen(function* () {
      const context = yield* requireSession(threadId);
      const pending = context.approvals.get(requestId);
      if (!pending) {
        return yield* new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "session/request_permission",
          detail: "This approval request is no longer pending.",
        });
      }
      const optionId =
        decision === "cancel"
          ? undefined
          : pending.request.options.find(
              (o) =>
                o.kind ===
                (decision === "acceptForSession"
                  ? "allow_always"
                  : decision === "accept"
                    ? "allow_once"
                    : "reject_once"),
            )?.optionId ?? pending.request.options[0]?.optionId;
      yield* Deferred.succeed(pending.response, {
        decision,
        result: {
          outcome:
            optionId === undefined ? { outcome: "cancelled" } : { outcome: "selected", optionId },
        },
      });
    });

  const respondToUserInput: ProviderAdapterShape<ProviderAdapterError>["respondToUserInput"] = (
    threadId,
    requestId,
    answers,
  ) =>
    Effect.gen(function* () {
      const context = yield* requireSession(threadId);
      const pending = context.questions.get(requestId);
      if (!pending) {
        return yield* new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "session/request_permission",
          detail: "This question is no longer pending.",
        });
      }
      yield* Deferred.succeed(pending.response, {
        answers,
        result: { outcome: { outcome: "cancelled" } },
      });
    });

  const stopSession: ProviderAdapterShape<ProviderAdapterError>["stopSession"] = (threadId) =>
    withThreadLock(threadId, Effect.flatMap(requireSession(threadId), stopContext));

  const stopAll: ProviderAdapterShape<ProviderAdapterError>["stopAll"] = () =>
    Effect.forEach([...sessions.values()], stopContext, { discard: true });

  yield* Effect.addFinalizer(() =>
    stopAll().pipe(
      Effect.catchCause((cause) =>
        Cause.hasInterrupts(cause)
          ? Effect.void
          : Effect.logError("Could not stop a Bob Shell session."),
      ),
      Effect.ensuring(PubSub.shutdown(events)),
    ),
  );

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
    stopAll,
    listSessions: () =>
      Effect.sync(() =>
        [...sessions.values()]
          .filter((ctx) => !ctx.stopped)
          .map((ctx) => ({ ...ctx.session })),
      ),
    hasSession: (threadId) =>
      Effect.sync(() => sessions.has(threadId) && !sessions.get(threadId)?.stopped),
    readThread: (threadId) =>
      Effect.map(requireSession(threadId), (ctx) => ({
        threadId,
        turns: ctx.turns,
      } satisfies ProviderThreadSnapshot)),
    rollbackThread: (_threadId: ThreadId, _numTurns: number) =>
      Effect.fail(
        new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "rollbackThread",
          issue: "Bob Shell does not support conversation rewind. Start a new thread instead.",
        }),
      ),
    streamEvents: Stream.fromPubSub(events),
  } satisfies BobShellAdapterShape;
});
