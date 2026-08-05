import * as Schema from "effect/Schema";

import { IsoDateTime, NonNegativeInt } from "./baseSchemas.ts";
import { ProviderInstanceId } from "./providerInstance.ts";
import { ThreadId } from "./baseSchemas.ts";

export const CodexHistorySyncState = Schema.Literals(["disabled", "idle", "syncing", "error"]);
export type CodexHistorySyncState = typeof CodexHistorySyncState.Type;

export const CodexHistoryThreadSyncState = Schema.Literals([
  "idle",
  "external-active",
  "conflict",
  "unavailable",
  "error",
]);
export type CodexHistoryThreadSyncState = typeof CodexHistoryThreadSyncState.Type;

export const CodexHistorySyncCounts = Schema.Struct({
  discovered: NonNegativeInt,
  imported: NonNegativeInt,
  updated: NonNegativeInt,
  skipped: NonNegativeInt,
  conflicts: NonNegativeInt,
});
export type CodexHistorySyncCounts = typeof CodexHistorySyncCounts.Type;

export const CodexExternalThreadLink = Schema.Struct({
  threadId: ThreadId,
  providerInstanceId: ProviderInstanceId,
  providerThreadId: Schema.String,
  source: Schema.String,
  syncState: CodexHistoryThreadSyncState,
  lastSyncedAt: Schema.NullOr(IsoDateTime),
  lastError: Schema.NullOr(Schema.String),
  externallyActive: Schema.Boolean,
  unavailable: Schema.Boolean,
});
export type CodexExternalThreadLink = typeof CodexExternalThreadLink.Type;

export const CodexHistorySyncStatus = Schema.Struct({
  state: CodexHistorySyncState,
  lastRunAt: Schema.NullOr(IsoDateTime),
  nextRunAt: Schema.NullOr(IsoDateTime),
  counts: CodexHistorySyncCounts,
  error: Schema.NullOr(Schema.String),
  threads: Schema.Array(CodexExternalThreadLink),
});
export type CodexHistorySyncStatus = typeof CodexHistorySyncStatus.Type;

export class CodexHistorySyncError extends Schema.TaggedErrorClass<CodexHistorySyncError>()(
  "CodexHistorySyncError",
  {
    operation: Schema.Literals(["status", "run", "handoff"]),
    message: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {}
