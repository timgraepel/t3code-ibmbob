import * as NodeAssert from "node:assert/strict";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import { ChildProcessSpawner } from "effect/unstable/process";
import {
  BobShellSettings,
  ProviderInstanceId,
  ThreadId,
  type ProviderRuntimeEvent,
} from "@t3tools/contracts";
import { makeBobShellAdapter } from "./BobShellAdapter.ts";

describe("BobShellAdapter", () => {
  const testLayer = NodeServices.layer;

  it.effect("parses v2 stream-json lines, records token usage, tools, and completes turn", () =>
    Effect.gen(function* () {
      const spawnerLayer = Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, {
        spawn: (_command: unknown) =>
          Effect.succeed({
            pid: 12345 as any,
            stdout: Stream.fromIterable([
              // v2: no init event; message has no delta flag
              JSON.stringify({ type: "message", role: "assistant", content: "Hello from Bob!" }) +
                "\n",
              JSON.stringify({
                type: "tool_use",
                tool_name: "readFile",
                tool_id: "call-1",
                parameters: { path: "foo.ts" },
              }) + "\n",
              JSON.stringify({
                type: "tool_result",
                tool_id: "call-1",
                status: "success",
                output: "file contents",
              }) + "\n",
              JSON.stringify({
                type: "result",
                status: "success",
                stats: {
                  task_id: "task-abc-123",
                  total_tokens: 150,
                  input_tokens: 100,
                  output_tokens: 50,
                  duration_ms: 1200,
                  session_costs: 0.005,
                },
              }) + "\n",
            ]),
            stderr: Stream.empty,
            exitCode: Effect.succeed(0),
            kill: () => Effect.void,
          } as any),
      } as any);

      const adapter = yield* makeBobShellAdapter(
        BobShellSettings.make({
          enabled: true,
          binaryPath: "bob",
          homePath: "",
          teamId: "",
          apiKey: "",
          launchArgs: "--mode code --verbose",
        }),
        {
          instanceId: ProviderInstanceId.make("bob-test"),
          environment: {},
        },
      ).pipe(Effect.provide(spawnerLayer));

      const threadId = ThreadId.make("thread-test-1");
      yield* adapter.startSession({
        threadId,
        runtimeMode: "full-access",
        approvalPolicy: "never",
      });

      const eventsFiber = yield* adapter.streamEvents.pipe(
        Stream.filter((e) => e.threadId === threadId),
        Stream.take(8),
        Stream.runCollect,
        Effect.forkChild,
      );

      yield* adapter.sendTurn({
        threadId,
        input: "say hello",
      });

      const events = (yield* Fiber.join(eventsFiber)) as ProviderRuntimeEvent[];

      const startedEvent = events.find((e) => e.type === "turn.started");
      NodeAssert.ok(startedEvent, "turn.started should be emitted");

      const messageEvent = events.find((e) => e.type === "content.delta");
      NodeAssert.ok(messageEvent, "content.delta should be emitted");
      NodeAssert.equal((messageEvent.payload as any).delta, "Hello from Bob!");

      const toolStart = events.find(
        (e) => e.type === "item.started" && (e.payload as any).itemType === "dynamic_tool_call",
      );
      NodeAssert.ok(toolStart, "item.started for tool should be emitted");
      NodeAssert.deepEqual((toolStart.payload as any).data, {
        toolName: "readFile",
        parameters: { path: "foo.ts" },
      });

      const toolComplete = events.find(
        (e) => e.type === "item.completed" && (e.payload as any).itemType === "dynamic_tool_call",
      );
      NodeAssert.ok(toolComplete, "item.completed for tool should be emitted");
      NodeAssert.equal((toolComplete.payload as any).status, "completed");
      NodeAssert.equal((toolComplete.payload as any).detail, "file contents");

      const usageEvent = events.find((e) => e.type === "thread.token-usage.updated");
      NodeAssert.ok(usageEvent, "thread.token-usage.updated should be emitted");
      NodeAssert.deepEqual((usageEvent.payload as any).usage, {
        usedTokens: 150,
        totalProcessedTokens: 150,
        inputTokens: 100,
        outputTokens: 50,
        durationMs: 1200,
      });

      const turnComplete = events.find((e) => e.type === "turn.completed");
      NodeAssert.ok(turnComplete, "turn.completed should be emitted");
      NodeAssert.equal((turnComplete.payload as any).state, "completed");
      NodeAssert.equal((turnComplete.payload as any).totalCostUsd, 0.005);
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("captures stderr on non-zero exit code and emits error item", () =>
    Effect.gen(function* () {
      const spawnerLayer = Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, {
        spawn: (_command: unknown) =>
          Effect.succeed({
            pid: 12346 as any,
            stdout: Stream.empty,
            stderr: Stream.fromIterable([
              "Fatal error: authentication token invalid\n",
              "Exiting.\n",
            ]),
            exitCode: Effect.succeed(1),
            kill: () => Effect.void,
          } as any),
      } as any);

      const adapter = yield* makeBobShellAdapter(
        BobShellSettings.make({
          enabled: true,
          binaryPath: "bob",
          homePath: "",
          teamId: "",
          apiKey: "",
          launchArgs: "",
        }),
        {
          instanceId: ProviderInstanceId.make("bob-test"),
          environment: {},
        },
      ).pipe(Effect.provide(spawnerLayer));

      const threadId = ThreadId.make("thread-test-2");
      yield* adapter.startSession({
        threadId,
        runtimeMode: "full-access",
        approvalPolicy: "never",
      });

      const eventsFiber = yield* adapter.streamEvents.pipe(
        Stream.filter((e) => e.threadId === threadId),
        Stream.take(3),
        Stream.runCollect,
        Effect.forkChild,
      );

      yield* adapter.sendTurn({
        threadId,
        input: "run task",
      });

      const events = (yield* Fiber.join(eventsFiber)) as ProviderRuntimeEvent[];

      const errorItem = events.find(
        (e) => e.type === "item.completed" && (e.payload as any).itemType === "error",
      );
      NodeAssert.ok(errorItem, "error item should be emitted on non-zero exit");
      NodeAssert.ok(
        (errorItem.payload as any).detail.includes("Fatal error: authentication token invalid"),
      );

      const turnComplete = events.find((e) => e.type === "turn.completed");
      NodeAssert.ok(turnComplete, "turn.completed should be emitted");
      NodeAssert.equal((turnComplete.payload as any).state, "failed");
      NodeAssert.equal((turnComplete.payload as any).stopReason, "exit_code_1");
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("terminates child process on interruptTurn", () =>
    Effect.gen(function* () {
      let killed = false;
      const deferredHang = yield* Deferred.make<void>();

      const spawnerLayer = Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, {
        spawn: (_command: unknown) =>
          Effect.succeed({
            pid: 12347 as any,
            stdout: Stream.fromEffect(Deferred.await(deferredHang).pipe(Effect.as(""))),
            stderr: Stream.empty,
            exitCode: Deferred.await(deferredHang).pipe(Effect.as(0)),
            kill: () =>
              Effect.sync(() => {
                killed = true;
              }),
          } as any),
      } as any);

      const adapter = yield* makeBobShellAdapter(
        BobShellSettings.make({
          enabled: true,
          binaryPath: "bob",
          homePath: "",
          teamId: "",
          apiKey: "",
          launchArgs: "",
        }),
        {
          instanceId: ProviderInstanceId.make("bob-test"),
          environment: {},
        },
      ).pipe(Effect.provide(spawnerLayer));

      const threadId = ThreadId.make("thread-test-3");
      yield* adapter.startSession({
        threadId,
        runtimeMode: "full-access",
        approvalPolicy: "never",
      });

      yield* adapter.sendTurn({
        threadId,
        input: "run long task",
      });

      // Give fiber a moment to spawn the child and set session.activeChildProcess
      yield* Effect.yieldNow;
      yield* Effect.yieldNow;

      yield* adapter.interruptTurn(threadId);

      NodeAssert.equal(killed, true, "childProcess.kill should be called on interruptTurn");
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("readThread always returns empty snapshot in v2 (history in SQLite)", () =>
    Effect.gen(function* () {
      const spawnerLayer = Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, {
        spawn: () =>
          Effect.succeed({
            pid: 1 as any,
            stdout: Stream.empty,
            stderr: Stream.empty,
            exitCode: Effect.succeed(0),
            kill: () => Effect.void,
          } as any),
      } as any);

      const adapter = yield* makeBobShellAdapter(
        BobShellSettings.make({
          enabled: true,
          binaryPath: "bob",
          homePath: "",
          teamId: "",
          apiKey: "",
          launchArgs: "",
        }),
        { instanceId: ProviderInstanceId.make("bob-test"), environment: {} },
      ).pipe(Effect.provide(spawnerLayer));

      const threadId = ThreadId.make("thread-readthread-v2");
      yield* adapter.startSession({ threadId, runtimeMode: "full-access" });

      // v2 stores history in ~/.bob/db/bob.db; readThread always returns empty.
      const snapshot = yield* adapter.readThread(threadId);
      NodeAssert.equal(snapshot.threadId, threadId);
      NodeAssert.deepEqual(snapshot.turns, []);
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("spawn_subagent tool_use is rendered with human-readable title and description", () =>
    Effect.gen(function* () {
      const spawnerLayer = Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, {
        spawn: (_command: unknown) =>
          Effect.succeed({
            pid: 30001 as any,
            stdout: Stream.fromIterable([
              JSON.stringify({
                type: "tool_use",
                tool_name: "spawn_subagent",
                tool_id: "subagent-call-1",
                parameters: {
                  name: "explore",
                  description: "Explore the codebase and find all usages of readThread",
                  fork_context: false,
                },
              }) + "\n",
              JSON.stringify({
                type: "tool_result",
                tool_id: "subagent-call-1",
                status: "success",
                output: "Found 8 usages of readThread.",
              }) + "\n",
              JSON.stringify({ type: "result", status: "success" }) + "\n",
            ]),
            stderr: Stream.empty,
            exitCode: Effect.succeed(0),
            kill: () => Effect.void,
          } as any),
      } as any);

      const adapter = yield* makeBobShellAdapter(
        BobShellSettings.make({
          enabled: true,
          binaryPath: "bob",
          homePath: "",
          teamId: "",
          apiKey: "",
          launchArgs: "",
        }),
        { instanceId: ProviderInstanceId.make("bob-test"), environment: {} },
      ).pipe(Effect.provide(spawnerLayer));

      const threadId = ThreadId.make("thread-subagent-title");
      yield* adapter.startSession({ threadId, runtimeMode: "full-access" });

      const eventsFiber = yield* adapter.streamEvents.pipe(
        Stream.filter((e) => e.threadId === threadId),
        Stream.takeUntil((e) => e.type === "turn.completed"),
        Stream.runCollect,
        Effect.forkChild,
      );

      yield* adapter.sendTurn({ threadId, input: "explore the codebase" });
      const events = (yield* Fiber.join(eventsFiber)) as ProviderRuntimeEvent[];

      // item.started for spawn_subagent should have a human-readable title
      const subagentStart = events.find(
        (e) =>
          e.type === "item.started" && (e.payload as any).itemType === "collab_agent_tool_call",
      );
      NodeAssert.ok(subagentStart, "item.started for spawn_subagent should be emitted");
      NodeAssert.equal((subagentStart.payload as any).title, "Subagent (explore)");
      NodeAssert.equal(
        (subagentStart.payload as any).detail,
        "Explore the codebase and find all usages of readThread",
      );
      // Original tool name is preserved in data for downstream consumers
      NodeAssert.equal((subagentStart.payload as any).data.toolName, "spawn_subagent");
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("classifies command and file tools consistently across tool events", () =>
    Effect.gen(function* () {
      const spawnerLayer = Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, {
        spawn: (_command: unknown) =>
          Effect.succeed({
            pid: 35001 as any,
            stdout: Stream.fromIterable([
              JSON.stringify({
                type: "tool_use",
                tool_name: "run_command",
                tool_id: "command-1",
                parameters: { command: "git status" },
              }) + "\n",
              JSON.stringify({
                type: "tool_result",
                tool_id: "command-1",
                status: "success",
                output: "",
              }) + "\n",
              JSON.stringify({
                type: "tool_use",
                tool_name: "write_file",
                tool_id: "file-1",
                parameters: { path: "foo.ts" },
              }) + "\n",
              JSON.stringify({
                type: "tool_result",
                tool_id: "file-1",
                status: "success",
                output: "",
              }) + "\n",
              JSON.stringify({ type: "result", status: "success" }) + "\n",
            ]),
            stderr: Stream.empty,
            exitCode: Effect.succeed(0),
            kill: () => Effect.void,
          } as any),
      } as any);

      const adapter = yield* makeBobShellAdapter(
        BobShellSettings.make({
          enabled: true,
          binaryPath: "bob",
          homePath: "",
          teamId: "",
          apiKey: "",
          launchArgs: "",
        }),
        { instanceId: ProviderInstanceId.make("bob-test"), environment: {} },
      ).pipe(Effect.provide(spawnerLayer));

      const threadId = ThreadId.make("thread-tool-classification");
      yield* adapter.startSession({ threadId, runtimeMode: "full-access" });

      const eventsFiber = yield* adapter.streamEvents.pipe(
        Stream.filter((e) => e.threadId === threadId),
        Stream.takeUntil((e) => e.type === "turn.completed"),
        Stream.runCollect,
        Effect.forkChild,
      );

      yield* adapter.sendTurn({ threadId, input: "make a change" });
      const events = (yield* Fiber.join(eventsFiber)) as ProviderRuntimeEvent[];

      for (const [itemId, itemType] of [
        ["command-1", "command_execution"],
        ["file-1", "file_change"],
      ] as const) {
        const itemEvents = events.filter((event) => (event as any).itemId === itemId);
        NodeAssert.deepEqual(
          itemEvents.map((event) => (event.payload as any).itemType),
          [itemType, itemType],
        );
      }
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect(
    "v2 isReasoning messages are suppressed and do not appear in content.delta events",
    () =>
      Effect.gen(function* () {
        // v2: reasoning is indicated by isReasoning: true on the message event.
        const spawnerLayer = Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, {
          spawn: (_command: unknown) =>
            Effect.succeed({
              pid: 40001 as any,
              stdout: Stream.fromIterable([
                // Pre-reasoning visible message
                JSON.stringify({ type: "message", role: "assistant", content: "I'll help. " }) +
                  "\n",
                // Reasoning messages — must all be suppressed
                JSON.stringify({
                  type: "message",
                  role: "assistant",
                  content: "Deciding what to do.",
                  isReasoning: true,
                }) + "\n",
                JSON.stringify({
                  type: "message",
                  role: "assistant",
                  content: "More thinking...",
                  isReasoning: true,
                }) + "\n",
                // Post-reasoning visible message
                JSON.stringify({
                  type: "message",
                  role: "assistant",
                  content: "Here is the answer.",
                }) + "\n",
                JSON.stringify({ type: "result", status: "success" }) + "\n",
              ]),
              stderr: Stream.empty,
              exitCode: Effect.succeed(0),
              kill: () => Effect.void,
            } as any),
        } as any);

        const adapter = yield* makeBobShellAdapter(
          BobShellSettings.make({
            enabled: true,
            binaryPath: "bob",
            homePath: "",
            teamId: "",
            apiKey: "",
            launchArgs: "",
          }),
          { instanceId: ProviderInstanceId.make("bob-test"), environment: {} },
        ).pipe(Effect.provide(spawnerLayer));

        const threadId = ThreadId.make("thread-reasoning-suppression");
        yield* adapter.startSession({ threadId, runtimeMode: "full-access" });

        const eventsFiber = yield* adapter.streamEvents.pipe(
          Stream.filter((e) => e.threadId === threadId),
          Stream.takeUntil((e) => e.type === "turn.completed"),
          Stream.runCollect,
          Effect.forkChild,
        );

        yield* adapter.sendTurn({ threadId, input: "test reasoning suppression" });
        const events = (yield* Fiber.join(eventsFiber)) as ProviderRuntimeEvent[];

        const contentDeltas = events.filter((e) => e.type === "content.delta");
        const allDeltas = contentDeltas.map((e) => (e.payload as any).delta as string);

        // Reasoning content must not appear
        const reasoningLeak = allDeltas.some(
          (d) => d.includes("Deciding") || d.includes("More thinking"),
        );
        NodeAssert.equal(
          reasoningLeak,
          false,
          `reasoning content leaked into content.delta: ${allDeltas.join(", ")}`,
        );

        // Visible content before and after reasoning must be present
        NodeAssert.ok(
          allDeltas.some((d) => d.includes("I'll help.")),
          "pre-reasoning content should be visible",
        );
        NodeAssert.ok(
          allDeltas.some((d) => d.includes("Here is the answer.")),
          "post-reasoning content should be visible",
        );
      }).pipe(Effect.provide(testLayer)),
  );

  it.effect("v2 error event emits error item and completes turn with error state", () =>
    Effect.gen(function* () {
      const spawnerLayer = Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, {
        spawn: (_command: unknown) =>
          Effect.succeed({
            pid: 40002 as any,
            stdout: Stream.fromIterable([
              JSON.stringify({
                type: "error",
                severity: "error",
                message: "Maximum cost limit reached: 0.50 exceeds 0.10",
              }) + "\n",
            ]),
            stderr: Stream.empty,
            exitCode: Effect.succeed(0),
            kill: () => Effect.void,
          } as any),
      } as any);

      const adapter = yield* makeBobShellAdapter(
        BobShellSettings.make({
          enabled: true,
          binaryPath: "bob",
          homePath: "",
          teamId: "",
          apiKey: "",
          launchArgs: "",
        }),
        { instanceId: ProviderInstanceId.make("bob-test"), environment: {} },
      ).pipe(Effect.provide(spawnerLayer));

      const threadId = ThreadId.make("thread-error-event");
      yield* adapter.startSession({ threadId, runtimeMode: "full-access" });

      const eventsFiber = yield* adapter.streamEvents.pipe(
        Stream.filter((e) => e.threadId === threadId),
        Stream.takeUntil((e) => e.type === "turn.completed"),
        Stream.runCollect,
        Effect.forkChild,
      );

      yield* adapter.sendTurn({ threadId, input: "expensive task" });
      const events = (yield* Fiber.join(eventsFiber)) as ProviderRuntimeEvent[];

      const errorItem = events.find(
        (e) => e.type === "item.completed" && (e.payload as any).itemType === "error",
      );
      NodeAssert.ok(errorItem, "error item should be emitted for error event");
      NodeAssert.ok((errorItem.payload as any).detail.includes("Maximum cost limit reached"));

      const turnComplete = events.find((e) => e.type === "turn.completed");
      NodeAssert.ok(turnComplete, "turn.completed should be emitted");
      NodeAssert.equal((turnComplete.payload as any).state, "failed");
      NodeAssert.equal((turnComplete.payload as any).stopReason, "error");
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("result.stats.task_id is captured and used as --resume on next turn", () =>
    Effect.gen(function* () {
      const capturedArgs: string[][] = [];
      let callCount = 0;

      const spawnerLayer = Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, {
        spawn: (command: unknown) => {
          const cmd = command as { args?: string[] };
          if (cmd.args) capturedArgs.push([...cmd.args]);
          callCount++;
          return Effect.succeed({
            pid: 40003 as any,
            stdout: Stream.fromIterable([
              JSON.stringify({
                type: "result",
                status: "success",
                stats: { task_id: "task-xyz-789", session_costs: 0.01 },
              }) + "\n",
            ]),
            stderr: Stream.empty,
            exitCode: Effect.succeed(0),
            kill: () => Effect.void,
          } as any);
        },
      } as any);

      const adapter = yield* makeBobShellAdapter(
        BobShellSettings.make({
          enabled: true,
          binaryPath: "bob",
          homePath: "",
          teamId: "",
          apiKey: "",
          launchArgs: "",
        }),
        { instanceId: ProviderInstanceId.make("bob-test"), environment: {} },
      ).pipe(Effect.provide(spawnerLayer));

      const threadId = ThreadId.make("thread-task-id-resume");
      yield* adapter.startSession({ threadId, runtimeMode: "full-access" });

      // First turn — no --resume
      const evFiber1 = yield* adapter.streamEvents.pipe(
        Stream.filter((e) => e.threadId === threadId),
        Stream.takeUntil((e) => e.type === "turn.completed"),
        Stream.runDrain,
        Effect.forkChild,
      );
      yield* adapter.sendTurn({ threadId, input: "first turn" });
      yield* Fiber.join(evFiber1);

      // Second turn — should include --resume task-xyz-789
      const evFiber2 = yield* adapter.streamEvents.pipe(
        Stream.filter((e) => e.threadId === threadId),
        Stream.takeUntil((e) => e.type === "turn.completed"),
        Stream.runDrain,
        Effect.forkChild,
      );
      yield* adapter.sendTurn({ threadId, input: "second turn" });
      yield* Fiber.join(evFiber2);

      NodeAssert.equal(capturedArgs.length, 2, "spawn should be called twice");
      const firstArgs = capturedArgs[0]!;
      const secondArgs = capturedArgs[1]!;
      NodeAssert.ok(!firstArgs.includes("--resume"), "first turn should not have --resume");
      const resumeIdx = secondArgs.indexOf("--resume");
      NodeAssert.ok(resumeIdx !== -1, "second turn should have --resume");
      NodeAssert.equal(
        secondArgs[resumeIdx + 1],
        "task-xyz-789",
        "--resume should use task_id from result stats",
      );
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("passes --chat-mode=agent by default when launchArgs has no --chat-mode", () =>
    Effect.gen(function* () {
      const capturedArgs: string[][] = [];

      const spawnerLayer = Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, {
        spawn: (command: unknown) => {
          const cmd = command as { args?: string[] };
          if (cmd.args) capturedArgs.push([...cmd.args]);
          return Effect.succeed({
            pid: 50001 as any,
            stdout: Stream.fromIterable([
              JSON.stringify({ type: "result", status: "success" }) + "\n",
            ]),
            stderr: Stream.empty,
            exitCode: Effect.succeed(0),
            kill: () => Effect.void,
          } as any);
        },
      } as any);

      const adapter = yield* makeBobShellAdapter(
        BobShellSettings.make({
          enabled: true,
          binaryPath: "bob",
          homePath: "",
          teamId: "",
          apiKey: "",
          launchArgs: "",
        }),
        { instanceId: ProviderInstanceId.make("bob-test"), environment: {} },
      ).pipe(Effect.provide(spawnerLayer));

      const threadId = ThreadId.make("thread-chatmode-default");
      yield* adapter.startSession({ threadId, runtimeMode: "full-access" });

      const eventsFiber = yield* adapter.streamEvents.pipe(
        Stream.filter((e) => e.threadId === threadId),
        Stream.takeUntil((e) => e.type === "turn.completed"),
        Stream.runDrain,
        Effect.forkChild,
      );

      yield* adapter.sendTurn({ threadId, input: "hello" });
      yield* Fiber.join(eventsFiber);

      NodeAssert.ok(capturedArgs.length > 0, "spawn should have been called");
      const args = capturedArgs[0]!;
      NodeAssert.ok(args.includes("--chat-mode=agent"), "--chat-mode=agent should be present in spawn args");
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("does not inject --chat-mode=agent when launchArgs already specifies --chat-mode", () =>
    Effect.gen(function* () {
      const capturedArgs: string[][] = [];

      const spawnerLayer = Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, {
        spawn: (command: unknown) => {
          const cmd = command as { args?: string[] };
          if (cmd.args) capturedArgs.push([...cmd.args]);
          return Effect.succeed({
            pid: 50002 as any,
            stdout: Stream.fromIterable([
              JSON.stringify({ type: "result", status: "success" }) + "\n",
            ]),
            stderr: Stream.empty,
            exitCode: Effect.succeed(0),
            kill: () => Effect.void,
          } as any);
        },
      } as any);

      const adapter = yield* makeBobShellAdapter(
        BobShellSettings.make({
          enabled: true,
          binaryPath: "bob",
          homePath: "",
          teamId: "",
          apiKey: "",
          launchArgs: "--chat-mode=code",
        }),
        { instanceId: ProviderInstanceId.make("bob-test"), environment: {} },
      ).pipe(Effect.provide(spawnerLayer));

      const threadId = ThreadId.make("thread-chatmode-override");
      yield* adapter.startSession({ threadId, runtimeMode: "full-access" });

      const eventsFiber = yield* adapter.streamEvents.pipe(
        Stream.filter((e) => e.threadId === threadId),
        Stream.takeUntil((e) => e.type === "turn.completed"),
        Stream.runDrain,
        Effect.forkChild,
      );

      yield* adapter.sendTurn({ threadId, input: "hello" });
      yield* Fiber.join(eventsFiber);

      NodeAssert.ok(capturedArgs.length > 0, "spawn should have been called");
      const args = capturedArgs[0]!;
      // User's --chat-mode=code must be present and the default must not inject --chat-mode=agent
      NodeAssert.ok(args.includes("--chat-mode=code"), "user-supplied --chat-mode=code should be present");
      NodeAssert.equal(
        args.filter((a) => a.startsWith("--chat-mode=")).length,
        1,
        "--chat-mode= should appear exactly once",
      );
    }).pipe(Effect.provide(testLayer)),
  );
});
