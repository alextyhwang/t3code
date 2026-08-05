import {
  CodexHistoryThreadSyncState,
  IsoDateTime,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import type * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import type { ProjectionRepositoryError } from "../Errors.ts";

export const ProviderThreadLink = Schema.Struct({
  threadId: ThreadId,
  providerInstanceId: ProviderInstanceId,
  providerThreadId: Schema.String,
  source: Schema.String,
  cwd: Schema.String,
  providerUpdatedAt: IsoDateTime,
  lastSyncedAt: Schema.NullOr(IsoDateTime),
  syncState: CodexHistoryThreadSyncState,
  lastError: Schema.NullOr(Schema.String),
  externallyActive: Schema.Boolean,
  idleObservations: Schema.Number,
  unavailable: Schema.Boolean,
  suppressedAt: Schema.NullOr(IsoDateTime),
});
export type ProviderThreadLink = typeof ProviderThreadLink.Type;

export const ProviderHistoryItemLink = Schema.Struct({
  providerInstanceId: ProviderInstanceId,
  providerThreadId: Schema.String,
  providerTurnId: Schema.String,
  providerItemId: Schema.String,
  projectionId: Schema.String,
  itemKind: Schema.String,
  contentHash: Schema.String,
  lastSeenAt: IsoDateTime,
});
export type ProviderHistoryItemLink = typeof ProviderHistoryItemLink.Type;

export interface ProviderHistoryRepositoryShape {
  readonly upsertThreadLink: (
    link: ProviderThreadLink,
  ) => Effect.Effect<void, ProjectionRepositoryError>;
  readonly getThreadLink: (
    threadId: ThreadId,
  ) => Effect.Effect<Option.Option<ProviderThreadLink>, ProjectionRepositoryError>;
  readonly getThreadLinkByProvider: (input: {
    readonly providerInstanceId: ProviderInstanceId;
    readonly providerThreadId: string;
  }) => Effect.Effect<Option.Option<ProviderThreadLink>, ProjectionRepositoryError>;
  readonly listThreadLinks: () => Effect.Effect<
    ReadonlyArray<ProviderThreadLink>,
    ProjectionRepositoryError
  >;
  readonly upsertItemLink: (
    link: ProviderHistoryItemLink,
  ) => Effect.Effect<void, ProjectionRepositoryError>;
  readonly listItemLinks: (input: {
    readonly providerInstanceId: ProviderInstanceId;
    readonly providerThreadId: string;
  }) => Effect.Effect<ReadonlyArray<ProviderHistoryItemLink>, ProjectionRepositoryError>;
  readonly deleteItemLinks: (input: {
    readonly providerInstanceId: ProviderInstanceId;
    readonly providerThreadId: string;
    readonly providerItemIds: ReadonlyArray<string>;
  }) => Effect.Effect<void, ProjectionRepositoryError>;
}

export class ProviderHistoryRepository extends Context.Service<
  ProviderHistoryRepository,
  ProviderHistoryRepositoryShape
>()("t3/persistence/Services/ProviderHistory/ProviderHistoryRepository") {}
