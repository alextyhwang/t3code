import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Struct from "effect/Struct";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { toPersistenceDecodeError, toPersistenceSqlError } from "../Errors.ts";
import {
  ProviderHistoryItemLink,
  ProviderHistoryRepository,
  type ProviderHistoryRepositoryShape,
  ProviderThreadLink,
} from "../Services/ProviderHistory.ts";

const ProviderThreadLinkDbRow = ProviderThreadLink.mapFields(
  Struct.assign({
    externallyActive: Schema.Number,
    idleObservations: Schema.Number,
    unavailable: Schema.Number,
  }),
);
const decodeProviderThreadLink = Schema.decodeUnknownEffect(ProviderThreadLink);
const decodeProviderThreadLinkDbRow = Schema.decodeUnknownEffect(ProviderThreadLinkDbRow);
const decodeProviderHistoryItemLink = Schema.decodeUnknownEffect(ProviderHistoryItemLink);

function decodeLink(row: typeof ProviderThreadLinkDbRow.Type) {
  return decodeProviderThreadLink({
    ...row,
    externallyActive: row.externallyActive === 1,
    idleObservations: row.idleObservations,
    unavailable: row.unavailable === 1,
  });
}

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  const decodeLinks = Effect.fn("ProviderHistoryRepository.decodeLinks")(function* (
    rows: ReadonlyArray<unknown>,
  ) {
    return yield* Effect.forEach(rows, (row) =>
      decodeProviderThreadLinkDbRow(row).pipe(Effect.flatMap(decodeLink)),
    );
  });

  const upsertThreadLink: ProviderHistoryRepositoryShape["upsertThreadLink"] = (link) =>
    sql`
      INSERT INTO provider_thread_links (
        thread_id, provider_instance_id, provider_thread_id, source, cwd,
        provider_updated_at, last_synced_at, sync_state, last_error,
        externally_active, idle_observations, unavailable, suppressed_at
      ) VALUES (
        ${link.threadId}, ${link.providerInstanceId}, ${link.providerThreadId}, ${link.source},
        ${link.cwd}, ${link.providerUpdatedAt}, ${link.lastSyncedAt}, ${link.syncState},
        ${link.lastError}, ${link.externallyActive ? 1 : 0}, ${link.idleObservations}, ${link.unavailable ? 1 : 0},
        ${link.suppressedAt}
      )
      ON CONFLICT (thread_id) DO UPDATE SET
        provider_instance_id = excluded.provider_instance_id,
        provider_thread_id = excluded.provider_thread_id,
        source = excluded.source,
        cwd = excluded.cwd,
        provider_updated_at = excluded.provider_updated_at,
        last_synced_at = excluded.last_synced_at,
        sync_state = excluded.sync_state,
        last_error = excluded.last_error,
        externally_active = excluded.externally_active,
        idle_observations = excluded.idle_observations,
        unavailable = excluded.unavailable,
        suppressed_at = excluded.suppressed_at
    `.pipe(
      Effect.asVoid,
      Effect.mapError(toPersistenceSqlError("ProviderHistoryRepository.upsertThreadLink")),
    );

  const getThreadLink: ProviderHistoryRepositoryShape["getThreadLink"] = (threadId) =>
    sql<Record<string, unknown>>`
      SELECT
        thread_id AS "threadId", provider_instance_id AS "providerInstanceId",
        provider_thread_id AS "providerThreadId", source, cwd,
        provider_updated_at AS "providerUpdatedAt", last_synced_at AS "lastSyncedAt",
        sync_state AS "syncState", last_error AS "lastError",
        externally_active AS "externallyActive", idle_observations AS "idleObservations", unavailable,
        suppressed_at AS "suppressedAt"
      FROM provider_thread_links
      WHERE thread_id = ${threadId}
      LIMIT 1
    `.pipe(
      Effect.mapError(toPersistenceSqlError("ProviderHistoryRepository.getThreadLink")),
      Effect.flatMap(decodeLinks),
      Effect.map((rows) => Option.fromNullishOr(rows[0])),
      Effect.mapError((cause) =>
        Schema.isSchemaError(cause)
          ? toPersistenceDecodeError("ProviderHistoryRepository.getThreadLink")(cause)
          : cause,
      ),
    );

  const getThreadLinkByProvider: ProviderHistoryRepositoryShape["getThreadLinkByProvider"] = ({
    providerInstanceId,
    providerThreadId,
  }) =>
    sql<Record<string, unknown>>`
      SELECT
        thread_id AS "threadId", provider_instance_id AS "providerInstanceId",
        provider_thread_id AS "providerThreadId", source, cwd,
        provider_updated_at AS "providerUpdatedAt", last_synced_at AS "lastSyncedAt",
        sync_state AS "syncState", last_error AS "lastError",
        externally_active AS "externallyActive", idle_observations AS "idleObservations", unavailable,
        suppressed_at AS "suppressedAt"
      FROM provider_thread_links
      WHERE provider_instance_id = ${providerInstanceId}
        AND provider_thread_id = ${providerThreadId}
      LIMIT 1`.pipe(
      Effect.mapError(toPersistenceSqlError("ProviderHistoryRepository.getThreadLinkByProvider")),
      Effect.flatMap(decodeLinks),
      Effect.map((rows) => Option.fromNullishOr(rows[0])),
      Effect.mapError((cause) =>
        Schema.isSchemaError(cause)
          ? toPersistenceDecodeError("ProviderHistoryRepository.getThreadLinkByProvider")(cause)
          : cause,
      ),
    );

  const listThreadLinks: ProviderHistoryRepositoryShape["listThreadLinks"] = () =>
    sql<Record<string, unknown>>`
      SELECT
        thread_id AS "threadId", provider_instance_id AS "providerInstanceId",
        provider_thread_id AS "providerThreadId", source, cwd,
        provider_updated_at AS "providerUpdatedAt", last_synced_at AS "lastSyncedAt",
        sync_state AS "syncState", last_error AS "lastError",
        externally_active AS "externallyActive", idle_observations AS "idleObservations", unavailable,
        suppressed_at AS "suppressedAt"
      FROM provider_thread_links
      ORDER BY provider_updated_at DESC
    `.pipe(
      Effect.mapError(toPersistenceSqlError("ProviderHistoryRepository.listThreadLinks")),
      Effect.flatMap(decodeLinks),
      Effect.mapError((cause) =>
        Schema.isSchemaError(cause)
          ? toPersistenceDecodeError("ProviderHistoryRepository.listThreadLinks")(cause)
          : cause,
      ),
    );

  const upsertItemLink: ProviderHistoryRepositoryShape["upsertItemLink"] = (link) =>
    sql`
      INSERT INTO provider_history_items (
        provider_instance_id, provider_thread_id, provider_turn_id, provider_item_id,
        projection_id, item_kind, content_hash, last_seen_at
      ) VALUES (
        ${link.providerInstanceId}, ${link.providerThreadId}, ${link.providerTurnId},
        ${link.providerItemId}, ${link.projectionId}, ${link.itemKind}, ${link.contentHash},
        ${link.lastSeenAt}
      )
      ON CONFLICT (provider_instance_id, provider_thread_id, provider_item_id) DO UPDATE SET
        provider_turn_id = excluded.provider_turn_id,
        projection_id = excluded.projection_id,
        item_kind = excluded.item_kind,
        content_hash = excluded.content_hash,
        last_seen_at = excluded.last_seen_at
    `.pipe(
      Effect.asVoid,
      Effect.mapError(toPersistenceSqlError("ProviderHistoryRepository.upsertItemLink")),
    );

  const listItemLinks: ProviderHistoryRepositoryShape["listItemLinks"] = ({
    providerInstanceId,
    providerThreadId,
  }) =>
    sql<Record<string, unknown>>`
      SELECT
        provider_instance_id AS "providerInstanceId",
        provider_thread_id AS "providerThreadId",
        provider_turn_id AS "providerTurnId",
        provider_item_id AS "providerItemId",
        projection_id AS "projectionId",
        item_kind AS "itemKind",
        content_hash AS "contentHash",
        last_seen_at AS "lastSeenAt"
      FROM provider_history_items
      WHERE provider_instance_id = ${providerInstanceId}
        AND provider_thread_id = ${providerThreadId}
    `.pipe(
      Effect.mapError(toPersistenceSqlError("ProviderHistoryRepository.listItemLinks")),
      Effect.flatMap((rows) => Effect.forEach(rows, (row) => decodeProviderHistoryItemLink(row))),
      Effect.mapError((cause) =>
        Schema.isSchemaError(cause)
          ? toPersistenceDecodeError("ProviderHistoryRepository.listItemLinks")(cause)
          : cause,
      ),
    );

  const deleteItemLinks: ProviderHistoryRepositoryShape["deleteItemLinks"] = ({
    providerInstanceId,
    providerThreadId,
    providerItemIds,
  }) =>
    providerItemIds.length === 0
      ? Effect.void
      : Effect.forEach(
          providerItemIds,
          (providerItemId) =>
            sql`
              DELETE FROM provider_history_items
              WHERE provider_instance_id = ${providerInstanceId}
                AND provider_thread_id = ${providerThreadId}
                AND provider_item_id = ${providerItemId}
            `,
          { discard: true },
        ).pipe(Effect.mapError(toPersistenceSqlError("ProviderHistoryRepository.deleteItemLinks")));

  return {
    upsertThreadLink,
    getThreadLink,
    getThreadLinkByProvider,
    listThreadLinks,
    upsertItemLink,
    listItemLinks,
    deleteItemLinks,
  } satisfies ProviderHistoryRepositoryShape;
});

export const ProviderHistoryRepositoryLive = Layer.effect(ProviderHistoryRepository, make);
