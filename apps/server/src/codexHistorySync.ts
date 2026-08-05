import * as NodeCrypto from "node:crypto";

import {
  CodexHistorySyncError,
  CommandId,
  DEFAULT_MODEL,
  DEFAULT_PROVIDER_INTERACTION_MODE,
  EventId,
  MessageId,
  OrchestrationProposedPlanId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  TurnId,
  type CodexHistorySyncStatus,
  type ModelSelection,
  type OrchestrationEvent,
  type OrchestrationThread,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Ref from "effect/Ref";
import * as Semaphore from "effect/Semaphore";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";

import { ProviderSessionRuntimeRepository } from "./persistence/ProviderSessionRuntime.ts";
import { ProviderHistoryRepository } from "./persistence/Services/ProviderHistory.ts";
import { OrchestrationEngineService } from "./orchestration/Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "./orchestration/Services/ProjectionSnapshotQuery.ts";
import { ProviderInstanceRegistry } from "./provider/Services/ProviderInstanceRegistry.ts";
import type {
  ProviderHistoryItem,
  ProviderHistoryThreadSnapshot,
  ProviderHistoryThreadSummary,
  ProviderHistoryTurn,
} from "./provider/Services/ProviderHistory.ts";
import { ServerSettingsService } from "./serverSettings.ts";

type MutableSyncCounts = {
  discovered: number;
  imported: number;
  updated: number;
  skipped: number;
  conflicts: number;
};

const emptyCounts = (): MutableSyncCounts => ({
  discovered: 0,
  imported: 0,
  updated: 0,
  skipped: 0,
  conflicts: 0,
});

const initialStatus: CodexHistorySyncStatus = {
  state: "disabled",
  lastRunAt: null,
  nextRunAt: null,
  counts: emptyCounts(),
  error: null,
  threads: [],
};

const syncError = (operation: "status" | "run" | "handoff", cause: unknown) =>
  new CodexHistorySyncError({
    operation,
    message: cause instanceof Error ? cause.message : String(cause),
    cause,
  });

const hash = (value: unknown) =>
  NodeCrypto.createHash("sha256").update(JSON.stringify(value)).digest("hex");

const stableId = (prefix: string, ...parts: ReadonlyArray<string>) =>
  `${prefix}:${hash(parts).slice(0, 32)}`;

const titleFor = (summary: ProviderHistoryThreadSummary) =>
  summary.title.trim() || summary.preview.trim().slice(0, 80) || "Imported Codex task";

const normalizedPath = (path: Path.Path, value: string) =>
  path.resolve(value).replaceAll("\\", "/").replace(/\/$/, "").toLocaleLowerCase();

function findExistingProjectionId(
  thread: OrchestrationThread | undefined,
  item: ProviderHistoryItem,
  used: Set<string>,
): string | undefined {
  if (!thread) return undefined;
  if (item.kind === "message") {
    const match = thread.messages.find(
      (message) =>
        !used.has(message.id) && message.role === item.role && message.text === item.text,
    );
    return match?.id;
  }
  if (item.kind === "plan") {
    const match = thread.proposedPlans.find(
      (plan) => !used.has(plan.id) && plan.planMarkdown === item.text,
    );
    return match?.id;
  }
  const match = thread.activities.find(
    (activity) => !used.has(activity.id) && activity.summary === item.summary,
  );
  return match?.id;
}

export interface CodexHistorySyncShape {
  readonly getStatus: Effect.Effect<CodexHistorySyncStatus>;
  readonly run: Effect.Effect<CodexHistorySyncStatus, CodexHistorySyncError>;
  readonly assertCanSend: (threadId: ThreadId) => Effect.Effect<void, CodexHistorySyncError>;
  readonly start: Effect.Effect<void, never, Scope.Scope>;
}

export class CodexHistorySync extends Context.Service<CodexHistorySync, CodexHistorySyncShape>()(
  "t3/codexHistorySync",
) {}

const make = Effect.gen(function* () {
  const settings = yield* ServerSettingsService;
  const instances = yield* ProviderInstanceRegistry;
  const links = yield* ProviderHistoryRepository;
  const runtimes = yield* ProviderSessionRuntimeRepository;
  const engine = yield* OrchestrationEngineService;
  const projections = yield* ProjectionSnapshotQuery;
  const crypto = yield* Crypto.Crypto;
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const status = yield* Ref.make<CodexHistorySyncStatus>(initialStatus);
  const lock = yield* Semaphore.make(1);

  const randomId = crypto.randomUUIDv4;
  const commandId = (kind: string, ...parts: ReadonlyArray<string>) =>
    CommandId.make(stableId(`codex-history:${kind}`, ...parts));

  const dispatch = (command: Parameters<typeof engine.dispatch>[0]) =>
    engine.dispatch(command).pipe(Effect.mapError((cause) => syncError("run", cause)));

  const resolveProject = Effect.fn("CodexHistorySync.resolveProject")(function* (
    cwd: string,
    instanceId: ProviderInstanceId,
    modelSelection: ModelSelection,
    allowCreate: boolean,
  ) {
    const readModel = yield* projections.getCommandReadModel();
    const normalizedCwd = normalizedPath(path, cwd);
    const existing = readModel.projects.find(
      (project) =>
        project.deletedAt === null && normalizedPath(path, project.workspaceRoot) === normalizedCwd,
    );
    if (existing) return existing.id;
    if (!allowCreate || !(yield* fs.exists(cwd))) return undefined;

    const projectId = ProjectId.make(yield* randomId);
    const createdAt = DateTime.formatIso(yield* DateTime.now);
    yield* dispatch({
      type: "project.create",
      commandId: commandId("project-create", instanceId, normalizedCwd),
      projectId,
      title: `Imported Codex · ${path.basename(cwd) || "workspace"}`,
      workspaceRoot: cwd,
      defaultModelSelection: modelSelection,
      createdAt,
    });
    return projectId;
  });

  const ensureThread = Effect.fn("CodexHistorySync.ensureThread")(function* (
    summary: ProviderHistoryThreadSummary,
    instanceId: ProviderInstanceId,
    modelSelection: ModelSelection,
    allowCreateProject: boolean,
  ) {
    const existingLink = yield* links.getThreadLinkByProvider({
      providerInstanceId: instanceId,
      providerThreadId: summary.providerThreadId,
    });
    if (Option.isSome(existingLink)) {
      return { threadId: existingLink.value.threadId, created: false } as const;
    }

    const projectId = yield* resolveProject(
      summary.cwd,
      instanceId,
      modelSelection,
      allowCreateProject,
    );
    if (!projectId) return undefined;

    const threadId = ThreadId.make(yield* randomId);
    yield* dispatch({
      type: "thread.create",
      commandId: commandId("thread-create", instanceId, summary.providerThreadId),
      threadId,
      projectId,
      title: titleFor(summary),
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
      branch: null,
      worktreePath: null,
      createdAt: summary.createdAt,
    });
    if (summary.archived) {
      yield* dispatch({
        type: "thread.archive",
        commandId: commandId("archive", instanceId, summary.providerThreadId, summary.updatedAt),
        threadId,
      });
    }
    return { threadId, created: true } as const;
  });

  const reconcileItem = Effect.fn("CodexHistorySync.reconcileItem")(function* (input: {
    readonly snapshot: ProviderHistoryThreadSnapshot;
    readonly turn: ProviderHistoryTurn;
    readonly item: ProviderHistoryItem;
    readonly instanceId: ProviderInstanceId;
    readonly threadId: ThreadId;
    readonly existingThread: OrchestrationThread | undefined;
    readonly existingHashes: ReadonlyMap<string, string>;
    readonly existingProjectionIds: ReadonlyMap<string, string>;
    readonly usedProjectionIds: Set<string>;
    readonly syncedAt: string;
  }) {
    const itemHash = hash(input.item);
    if (input.existingHashes.get(input.item.providerItemId) === itemHash) return false;
    const recoveredProjectionId =
      input.existingProjectionIds.get(input.item.providerItemId) ??
      findExistingProjectionId(input.existingThread, input.item, input.usedProjectionIds);
    const projectionId =
      recoveredProjectionId ??
      stableId(
        `codex-${input.item.kind}`,
        input.instanceId,
        input.snapshot.providerThreadId,
        input.item.providerItemId,
      );
    input.usedProjectionIds.add(projectionId);
    const turnId = TurnId.make(
      stableId(
        "codex-turn",
        input.instanceId,
        input.snapshot.providerThreadId,
        input.turn.providerTurnId,
      ),
    );
    const updatedAt = input.turn.completedAt ?? input.snapshot.updatedAt;

    if (input.item.kind === "message") {
      yield* dispatch({
        type: "thread.message.import",
        commandId: commandId(
          "message",
          input.instanceId,
          input.snapshot.providerThreadId,
          input.item.providerItemId,
          itemHash,
        ),
        threadId: input.threadId,
        messageId: MessageId.make(projectionId),
        role: input.item.role,
        text: input.item.text,
        turnId,
        createdAt: input.turn.startedAt,
        updatedAt,
      });
    } else if (input.item.kind === "plan" && input.item.text.trim()) {
      yield* dispatch({
        type: "thread.proposed-plan.upsert",
        commandId: commandId(
          "plan",
          input.instanceId,
          input.snapshot.providerThreadId,
          input.item.providerItemId,
          itemHash,
        ),
        threadId: input.threadId,
        proposedPlan: {
          id: OrchestrationProposedPlanId.make(projectionId),
          turnId,
          planMarkdown: input.item.text,
          implementedAt: null,
          implementationThreadId: null,
          createdAt: input.turn.startedAt,
          updatedAt,
        },
        createdAt: updatedAt,
      });
    } else if (input.item.kind === "activity" && input.item.summary.trim()) {
      yield* dispatch({
        type: "thread.activity.append",
        commandId: commandId(
          "activity",
          input.instanceId,
          input.snapshot.providerThreadId,
          input.item.providerItemId,
          itemHash,
        ),
        threadId: input.threadId,
        activity: {
          id: EventId.make(projectionId),
          tone: input.item.status === "failed" ? "error" : "tool",
          kind: input.item.activityKind,
          summary: input.item.summary,
          payload: input.item.payload,
          turnId,
          createdAt: updatedAt,
        },
        createdAt: updatedAt,
      });
    }

    yield* links.upsertItemLink({
      providerInstanceId: input.instanceId,
      providerThreadId: input.snapshot.providerThreadId,
      providerTurnId: input.turn.providerTurnId,
      providerItemId: input.item.providerItemId,
      projectionId,
      itemKind: input.item.kind,
      contentHash: itemHash,
      lastSeenAt: input.syncedAt,
    });
    return true;
  });

  const reconcileThread = Effect.fn("CodexHistorySync.reconcileThread")(function* (
    instanceId: ProviderInstanceId,
    summary: ProviderHistoryThreadSummary,
    modelSelection: ModelSelection,
    allowCreateProject: boolean,
    force: boolean,
  ) {
    const ensuredThread = yield* ensureThread(
      summary,
      instanceId,
      modelSelection,
      allowCreateProject,
    );
    if (!ensuredThread) return "skipped" as const;
    const { threadId } = ensuredThread;
    const previousLink = yield* links.getThreadLink(threadId);
    if (Option.isSome(previousLink) && previousLink.value.suppressedAt !== null) {
      return "skipped" as const;
    }
    const shouldDeferSnapshot =
      summary.externallyActive ||
      (!force &&
        (Option.isNone(previousLink) ||
          previousLink.value.providerUpdatedAt !== summary.updatedAt));
    if (shouldDeferSnapshot) {
      const syncedAt = DateTime.formatIso(yield* DateTime.now);
      const cwdAvailable = yield* fs.exists(summary.cwd);
      yield* links.upsertThreadLink({
        threadId,
        providerInstanceId: instanceId,
        providerThreadId: summary.providerThreadId,
        source: summary.source,
        cwd: summary.cwd,
        providerUpdatedAt: summary.updatedAt,
        lastSyncedAt: syncedAt,
        syncState: cwdAvailable ? "external-active" : "unavailable",
        lastError: null,
        externallyActive: true,
        idleObservations: 0,
        unavailable: !cwdAvailable,
        suppressedAt: null,
      });
      const existingThread = Option.getOrUndefined(
        yield* projections.getThreadDetailById(threadId),
      );
      yield* runtimes.upsert({
        threadId,
        providerName: "codex",
        providerInstanceId: instanceId,
        adapterKey: "codex",
        runtimeMode: existingThread?.runtimeMode ?? "full-access",
        status: "stopped",
        lastSeenAt: syncedAt,
        resumeCursor: { threadId: summary.providerThreadId },
        runtimePayload: null,
      });
      return ensuredThread.created || Option.isNone(previousLink)
        ? ("imported" as const)
        : ("updated" as const);
    }
    const itemLinks = yield* links.listItemLinks({
      providerInstanceId: instanceId,
      providerThreadId: summary.providerThreadId,
    });
    if (
      !force &&
      Option.isSome(previousLink) &&
      previousLink.value.providerUpdatedAt === summary.updatedAt &&
      previousLink.value.idleObservations === 1 &&
      itemLinks.length > 0
    ) {
      const syncedAt = DateTime.formatIso(yield* DateTime.now);
      const cwdAvailable = yield* fs.exists(summary.cwd);
      yield* links.upsertThreadLink({
        ...previousLink.value,
        lastSyncedAt: syncedAt,
        syncState: cwdAvailable ? "idle" : "unavailable",
        externallyActive: false,
        idleObservations: 2,
        unavailable: !cwdAvailable,
      });
      return "updated" as const;
    }
    const changed =
      force ||
      Option.isNone(previousLink) ||
      previousLink.value.providerUpdatedAt !== summary.updatedAt ||
      itemLinks.length === 0 ||
      previousLink.value.idleObservations === 0;
    if (!changed) return "skipped" as const;

    const instance = yield* instances.getInstance(instanceId);
    if (!instance?.history) return "skipped" as const;
    const snapshot = yield* instance.history.readThread(summary.providerThreadId, summary.archived);
    const syncedAt = DateTime.formatIso(yield* DateTime.now);
    const existingThreadOption = yield* projections.getThreadDetailById(threadId);
    const existingThread = Option.getOrUndefined(existingThreadOption);
    const existingHashes = new Map(
      itemLinks.map((item) => [item.providerItemId, item.contentHash]),
    );
    const existingProjectionIds = new Map(
      itemLinks.map((item) => [item.providerItemId, item.projectionId]),
    );
    const usedProjectionIds = new Set(itemLinks.map((item) => item.projectionId));
    let itemChanged = false;
    const seenProviderItemIds = new Set<string>();
    for (const turn of snapshot.turns) {
      for (const item of turn.items) {
        seenProviderItemIds.add(item.providerItemId);
        itemChanged =
          (yield* reconcileItem({
            snapshot,
            turn,
            item,
            instanceId,
            threadId,
            existingThread,
            existingHashes,
            existingProjectionIds,
            usedProjectionIds,
            syncedAt,
          })) || itemChanged;
      }
    }
    const staleItems = itemLinks.filter((item) => !seenProviderItemIds.has(item.providerItemId));
    if (staleItems.length > 0) {
      yield* dispatch({
        type: "thread.provider-history.prune",
        commandId: commandId(
          "prune",
          instanceId,
          snapshot.providerThreadId,
          ...staleItems.map((item) => item.providerItemId).toSorted(),
        ),
        threadId,
        messageIds: staleItems
          .filter((item) => item.itemKind === "message")
          .map((item) => MessageId.make(item.projectionId)),
        proposedPlanIds: staleItems
          .filter((item) => item.itemKind === "plan")
          .map((item) => OrchestrationProposedPlanId.make(item.projectionId)),
        activityIds: staleItems
          .filter((item) => item.itemKind === "activity")
          .map((item) => EventId.make(item.projectionId)),
        createdAt: syncedAt,
      });
      yield* links.deleteItemLinks({
        providerInstanceId: instanceId,
        providerThreadId: snapshot.providerThreadId,
        providerItemIds: staleItems.map((item) => item.providerItemId),
      });
      itemChanged = true;
    }

    if (existingThread && existingThread.title !== titleFor(snapshot)) {
      yield* dispatch({
        type: "thread.meta.update",
        commandId: commandId("title", instanceId, snapshot.providerThreadId, snapshot.title),
        threadId,
        title: titleFor(snapshot),
      });
    }
    if (existingThread && snapshot.archived !== (existingThread.archivedAt !== null)) {
      yield* dispatch({
        type: snapshot.archived ? "thread.archive" : "thread.unarchive",
        commandId: commandId(
          snapshot.archived ? "archive" : "unarchive",
          instanceId,
          snapshot.providerThreadId,
          snapshot.updatedAt,
        ),
        threadId,
      });
    }

    const previousIdle = Option.isSome(previousLink) ? previousLink.value.idleObservations : 0;
    const idleObservations = snapshot.externallyActive ? 0 : Math.min(previousIdle + 1, 2);
    const cwdAvailable = yield* fs.exists(snapshot.cwd);
    yield* links.upsertThreadLink({
      threadId,
      providerInstanceId: instanceId,
      providerThreadId: snapshot.providerThreadId,
      source: snapshot.source,
      cwd: snapshot.cwd,
      providerUpdatedAt: snapshot.updatedAt,
      lastSyncedAt: syncedAt,
      syncState: !cwdAvailable
        ? "unavailable"
        : snapshot.externallyActive || idleObservations < 2
          ? "external-active"
          : "idle",
      lastError: null,
      externallyActive: snapshot.externallyActive || idleObservations < 2,
      idleObservations,
      unavailable: !cwdAvailable,
      suppressedAt: null,
    });
    yield* runtimes.upsert({
      threadId,
      providerName: "codex",
      providerInstanceId: instanceId,
      adapterKey: "codex",
      runtimeMode: existingThread?.runtimeMode ?? "full-access",
      status: "stopped",
      lastSeenAt: syncedAt,
      resumeCursor: { threadId: snapshot.providerThreadId },
      runtimePayload: null,
    });
    if (ensuredThread.created || Option.isNone(previousLink)) return "imported" as const;
    return itemChanged ? ("updated" as const) : ("skipped" as const);
  });

  const runOnce = Effect.fn("CodexHistorySync.runOnce")(function* (force: boolean) {
    const configured = yield* settings.getSettings;
    if (!configured.codexHistorySync.enabled) {
      const disabled: CodexHistorySyncStatus = { ...initialStatus, counts: emptyCounts() };
      yield* Ref.set(status, disabled);
      return disabled;
    }
    yield* Ref.update(
      status,
      (value): CodexHistorySyncStatus => ({ ...value, state: "syncing", error: null }),
    );
    const counts = emptyCounts();
    const availableInstances = yield* instances.listInstances;
    for (const instance of availableInstances) {
      if (instance.driverKind !== "codex" || !instance.history) continue;
      const snapshot = yield* instance.snapshot.getSnapshot;
      const modelSelection: ModelSelection = {
        instanceId: instance.instanceId,
        model: snapshot.models[0]?.slug ?? DEFAULT_MODEL,
      };
      const summaries = yield* instance.history.listThreads();
      const seenThreadIds = new Set(summaries.map((summary) => summary.providerThreadId));
      counts.discovered += summaries.length;
      for (const summary of summaries) {
        const result = yield* reconcileThread(
          instance.instanceId,
          summary,
          modelSelection,
          configured.codexHistorySync.discoveryScope === "all",
          force,
        );
        if (result === "imported") counts.imported += 1;
        else if (result === "updated") counts.updated += 1;
        else counts.skipped += 1;
      }
      const knownLinks = (yield* links.listThreadLinks()).filter(
        (link) => link.providerInstanceId === instance.instanceId,
      );
      for (const link of knownLinks) {
        if (link.suppressedAt !== null || seenThreadIds.has(link.providerThreadId)) continue;
        const now = DateTime.formatIso(yield* DateTime.now);
        yield* links.upsertThreadLink({
          ...link,
          lastSyncedAt: now,
          syncState: "unavailable",
          lastError: "The Codex task is no longer available from app-server.",
          externallyActive: false,
          idleObservations: 0,
          unavailable: true,
        });
        const existing = yield* projections.getThreadDetailById(link.threadId);
        if (Option.isSome(existing) && existing.value.archivedAt === null) {
          yield* dispatch({
            type: "thread.archive",
            commandId: commandId("external-delete", instance.instanceId, link.providerThreadId),
            threadId: link.threadId,
          });
        }
      }
    }
    const now = DateTime.formatIso(yield* DateTime.now);
    const next = DateTime.formatIso(
      DateTime.addDuration(
        yield* DateTime.now,
        Duration.seconds(configured.codexHistorySync.intervalSeconds),
      ),
    );
    const statusLinks = yield* links.listThreadLinks();
    counts.conflicts = statusLinks.filter((link) => link.syncState === "conflict").length;
    const nextStatus: CodexHistorySyncStatus = {
      state: "idle",
      lastRunAt: now,
      nextRunAt: next,
      counts,
      error: null,
      threads: statusLinks.map((link) => ({
        threadId: link.threadId,
        providerInstanceId: link.providerInstanceId,
        providerThreadId: link.providerThreadId,
        source: link.source,
        syncState: link.syncState,
        lastSyncedAt: link.lastSyncedAt,
        lastError: link.lastError,
        externallyActive: link.externallyActive,
        unavailable: link.unavailable,
      })),
    };
    yield* Ref.set(status, nextStatus);
    return nextStatus;
  });

  const run = lock
    .withPermits(1)(runOnce(true))
    .pipe(
      Effect.catch((cause) =>
        Effect.gen(function* () {
          const error = syncError("run", cause);
          const now = DateTime.formatIso(yield* DateTime.now);
          yield* Ref.update(
            status,
            (value): CodexHistorySyncStatus => ({
              ...value,
              state: "error",
              lastRunAt: now,
              nextRunAt: null,
              error: error.message,
            }),
          );
          return yield* error;
        }),
      ),
    );

  const assertCanSend = (threadId: ThreadId) =>
    Effect.gen(function* () {
      const link = yield* links.getThreadLink(threadId);
      if (Option.isNone(link)) return;
      if (link.value.suppressedAt !== null) return;
      if (link.value.unavailable) {
        return yield* syncError("handoff", "The linked Codex task or workspace is unavailable.");
      }
      const instance = yield* instances.getInstance(link.value.providerInstanceId);
      if (!instance?.history) {
        return yield* syncError("handoff", "The linked Codex provider is unavailable.");
      }
      const snapshot = yield* instance.history.readThread(
        link.value.providerThreadId,
        link.value.syncState === "unavailable",
      );
      const idleObservations = snapshot.externallyActive
        ? 0
        : Math.min(link.value.idleObservations + 1, 2);
      const changedSinceCheckpoint = snapshot.updatedAt !== link.value.providerUpdatedAt;
      const blocked = snapshot.externallyActive || changedSinceCheckpoint || idleObservations < 2;
      yield* links.upsertThreadLink({
        ...link.value,
        providerUpdatedAt: snapshot.updatedAt,
        lastSyncedAt: DateTime.formatIso(yield* DateTime.now),
        syncState: changedSinceCheckpoint ? "conflict" : blocked ? "external-active" : "idle",
        externallyActive: blocked,
        idleObservations,
      });
      if (blocked) {
        return yield* syncError(
          "handoff",
          "Codex changed this task or is still active. Refresh it and continue after two idle checks.",
        );
      }
    }).pipe(Effect.mapError((cause) => syncError("handoff", cause)));

  const background = Effect.forever(
    Effect.gen(function* () {
      const configured = yield* settings.getSettings;
      if (configured.codexHistorySync.enabled) {
        yield* lock
          .withPermits(1)(runOnce(false))
          .pipe(Effect.ignoreCause({ log: true }));
      } else {
        yield* Ref.set(status, initialStatus);
      }
      yield* Effect.sleep(Duration.seconds(configured.codexHistorySync.intervalSeconds));
    }),
  );

  const propagateMetadata = Effect.fn("CodexHistorySync.propagateMetadata")(function* (
    event: OrchestrationEvent,
  ) {
    if (
      event.type !== "thread.meta-updated" &&
      event.type !== "thread.archived" &&
      event.type !== "thread.unarchived" &&
      event.type !== "thread.deleted"
    ) {
      return;
    }
    if (event.commandId?.startsWith("codex-history:")) return;
    const threadId = event.payload.threadId;
    const link = yield* links.getThreadLink(threadId);
    if (Option.isNone(link) || link.value.suppressedAt !== null) return;

    if (event.type === "thread.deleted") {
      yield* links.upsertThreadLink({
        ...link.value,
        suppressedAt: event.payload.deletedAt,
        lastSyncedAt: event.payload.deletedAt,
      });
      return;
    }

    const instance = yield* instances.getInstance(link.value.providerInstanceId);
    if (!instance?.history) return;
    if (event.type === "thread.meta-updated" && event.payload.title !== undefined) {
      yield* instance.history.setName(link.value.providerThreadId, event.payload.title);
    } else {
      yield* instance.history.setArchived(
        link.value.providerThreadId,
        event.type === "thread.archived",
      );
    }
    yield* links.upsertThreadLink({
      ...link.value,
      lastSyncedAt: event.occurredAt,
      lastError: null,
    });
  });

  const metadataLoop = Stream.runForEach(engine.streamDomainEvents, (event) =>
    propagateMetadata(event).pipe(
      Effect.catch((cause) =>
        Effect.logWarning("Codex metadata synchronization failed", {
          eventType: event.type,
          threadId: "threadId" in event.payload ? event.payload.threadId : undefined,
          cause,
        }),
      ),
    ),
  );

  return {
    getStatus: Ref.get(status),
    run,
    assertCanSend,
    start: settings.ready.pipe(
      Effect.andThen(
        Effect.all([background, metadataLoop], { concurrency: "unbounded", discard: true }),
      ),
      Effect.ignoreCause({ log: true }),
      Effect.forkScoped,
      Effect.asVoid,
    ),
  } satisfies CodexHistorySyncShape;
});

export const layer = Layer.effect(
  CodexHistorySync,
  make.pipe(Effect.tap((service) => service.start)),
);
