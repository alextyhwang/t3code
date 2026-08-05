import type { CodexSettings, ProviderInstanceId } from "@t3tools/contracts";
import { resolveSpawnCommand } from "@t3tools/shared/shell";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as ChildProcess from "effect/unstable/process/ChildProcess";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";
import * as CodexClient from "effect-codex-app-server/client";
import * as CodexSchema from "effect-codex-app-server/schema";

import { expandHomePath } from "../../pathExpansion.ts";
import { ProviderAdapterRequestError } from "../Errors.ts";
import type {
  ProviderHistoryAdapterShape,
  ProviderHistoryItem,
  ProviderHistoryThreadSnapshot,
  ProviderHistoryThreadSummary,
} from "../Services/ProviderHistory.ts";
import { buildCodexInitializeParams } from "./CodexProvider.ts";

const FORCE_KILL_AFTER = "2 seconds" as const;

function toIso(timestampSeconds: number | null | undefined, fallback: string): string {
  if (timestampSeconds === null || timestampSeconds === undefined) return fallback;
  return DateTime.formatIso(DateTime.makeUnsafe(timestampSeconds * 1_000));
}

function sourceLabel(source: CodexSchema.V2ThreadListResponse__Thread["source"]): string {
  if (typeof source === "string") return source;
  if ("custom" in source) return source.custom;
  return "subAgent";
}

function summaryFromThread(
  thread: CodexSchema.V2ThreadListResponse__Thread | CodexSchema.V2ThreadReadResponse__Thread,
  archived: boolean,
): ProviderHistoryThreadSummary {
  const createdAt = toIso(thread.createdAt, DateTime.formatIso(DateTime.makeUnsafe(0)));
  return {
    providerThreadId: thread.id,
    cwd: thread.cwd,
    title: thread.name?.trim() || thread.preview.trim() || "Imported Codex task",
    preview: thread.preview,
    source: sourceLabel(thread.source),
    archived,
    ephemeral: thread.ephemeral,
    parentThreadId: thread.parentThreadId ?? null,
    createdAt,
    updatedAt: toIso(thread.updatedAt, createdAt),
    externallyActive: thread.status.type === "active",
  };
}

function userMessageText(
  content: ReadonlyArray<CodexSchema.V2ThreadReadResponse__UserInput>,
): string {
  return content
    .map((item) => {
      switch (item.type) {
        case "text":
          return item.text;
        case "image":
          return `[Image: ${item.url}]`;
        case "localImage":
          return `[Image: ${item.path}]`;
        case "skill":
          return `$${item.name}`;
        case "mention":
          return `@${item.name}`;
        default:
          return "";
      }
    })
    .filter((value) => value.length > 0)
    .join("\n");
}

function itemFromCodex(
  item: CodexSchema.V2ThreadReadResponse__ThreadItem,
): ProviderHistoryItem | undefined {
  switch (item.type) {
    case "userMessage": {
      const text = userMessageText(item.content);
      return text.length > 0
        ? { kind: "message", providerItemId: item.id, role: "user", text }
        : undefined;
    }
    case "agentMessage":
      return item.text.trim().length > 0
        ? { kind: "message", providerItemId: item.id, role: "assistant", text: item.text }
        : undefined;
    case "plan":
      return item.text.trim().length > 0
        ? { kind: "plan", providerItemId: item.id, text: item.text }
        : undefined;
    case "commandExecution":
      return {
        kind: "activity",
        providerItemId: item.id,
        activityKind: "command",
        summary: item.command,
        status: item.status,
        payload: {
          command: item.command,
          cwd: item.cwd,
          output: item.aggregatedOutput ?? null,
          exitCode: item.exitCode ?? null,
          durationMs: item.durationMs ?? null,
        },
      };
    case "fileChange":
      return {
        kind: "activity",
        providerItemId: item.id,
        activityKind: "file-change",
        summary: `${item.changes.length} file change${item.changes.length === 1 ? "" : "s"}`,
        status: item.status,
        payload: { changes: item.changes },
      };
    default:
      return undefined;
  }
}

function snapshotFromThread(
  thread: CodexSchema.V2ThreadReadResponse__Thread,
  archived: boolean,
): ProviderHistoryThreadSnapshot {
  const summary = summaryFromThread(thread, archived);
  return {
    ...summary,
    externallyActive:
      summary.externallyActive || thread.turns.some((turn) => turn.status === "inProgress"),
    turns: thread.turns.map((turn) => ({
      providerTurnId: turn.id,
      state: turn.status,
      startedAt: toIso(turn.startedAt, summary.createdAt),
      completedAt: turn.status === "inProgress" ? null : toIso(turn.completedAt, summary.updatedAt),
      items: turn.items.flatMap((item) => {
        const normalized = itemFromCodex(item);
        return normalized ? [normalized] : [];
      }),
    })),
  };
}

function requestError(method: string, cause: unknown): ProviderAdapterRequestError {
  return new ProviderAdapterRequestError({
    provider: "codex",
    method,
    detail: cause instanceof Error ? cause.message : String(cause),
    cause,
  });
}

export const makeCodexHistoryAdapter = Effect.fn("makeCodexHistoryAdapter")(function* (input: {
  readonly instanceId: ProviderInstanceId;
  readonly settings: CodexSettings;
  readonly cwd: string;
  readonly environment?: NodeJS.ProcessEnv;
}) {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const resolvedHomePath = input.settings.homePath
    ? expandHomePath(input.settings.homePath)
    : undefined;
  const environment = {
    ...input.environment,
    ...(resolvedHomePath ? { CODEX_HOME: resolvedHomePath } : {}),
  };
  const spawnCommand = yield* resolveSpawnCommand(input.settings.binaryPath, ["app-server"], {
    env: environment,
    extendEnv: true,
  });
  const child = yield* spawner.spawn(
    ChildProcess.make(spawnCommand.command, spawnCommand.args, {
      cwd: input.cwd,
      env: environment,
      extendEnv: true,
      forceKillAfter: FORCE_KILL_AFTER,
      shell: spawnCommand.shell,
    }),
  );
  const clientContext = yield* Layer.build(CodexClient.layerChildProcess(child));
  const client = yield* Effect.service(CodexClient.CodexAppServerClient).pipe(
    Effect.provide(clientContext),
  );
  yield* client
    .request("initialize", buildCodexInitializeParams())
    .pipe(Effect.mapError((cause) => requestError("initialize", cause)));
  yield* client
    .notify("initialized", undefined)
    .pipe(Effect.mapError((cause) => requestError("initialized", cause)));

  const listPage = Effect.fn("CodexHistoryAdapter.listPage")(function* (archived: boolean) {
    const threads: ProviderHistoryThreadSummary[] = [];
    let cursor: string | null = null;
    do {
      const response: CodexSchema.V2ThreadListResponse = yield* client
        .request("thread/list", {
          archived,
          cursor,
          limit: 100,
          modelProviders: [],
          sortKey: "updated_at",
          sortDirection: "desc",
        })
        .pipe(Effect.mapError((cause) => requestError("thread/list", cause)));
      for (const thread of response.data) {
        const summary = summaryFromThread(thread, archived);
        if (
          !summary.ephemeral &&
          summary.parentThreadId === null &&
          summary.source !== "subAgent"
        ) {
          threads.push(summary);
        }
      }
      cursor = response.nextCursor ?? null;
    } while (cursor);
    return threads;
  });

  return {
    providerInstanceId: input.instanceId,
    listThreads: () =>
      Effect.all([listPage(false), listPage(true)], { concurrency: 1 }).pipe(
        Effect.map(([active, archived]) => [...active, ...archived]),
      ),
    readThread: (providerThreadId, archived) =>
      client.request("thread/read", { threadId: providerThreadId, includeTurns: true }).pipe(
        Effect.map((response) => snapshotFromThread(response.thread, archived)),
        Effect.mapError((cause) => requestError("thread/read", cause)),
      ),
    setName: (providerThreadId, name) =>
      client.request("thread/name/set", { threadId: providerThreadId, name }).pipe(
        Effect.asVoid,
        Effect.mapError((cause) => requestError("thread/name/set", cause)),
      ),
    setArchived: (providerThreadId, archived) =>
      client
        .request(archived ? "thread/archive" : "thread/unarchive", {
          threadId: providerThreadId,
        })
        .pipe(
          Effect.asVoid,
          Effect.mapError((cause) =>
            requestError(archived ? "thread/archive" : "thread/unarchive", cause),
          ),
        ),
  } satisfies ProviderHistoryAdapterShape;
});
