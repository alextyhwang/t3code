import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";

const layer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));

layer("035_CodexHistorySync", (it) => {
  it.effect("backfills one-time imports from Codex resume cursors", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 32 });
      yield* sql`
        INSERT INTO provider_session_runtime (
          thread_id, provider_name, provider_instance_id, adapter_key,
          runtime_mode, status, last_seen_at, resume_cursor_json, runtime_payload_json
        ) VALUES (
          't3-imported-thread', 'codex', 'codex', 'codex',
          'full-access', 'stopped', '2026-07-15T18:00:00.000Z',
          '{"threadId":"codex-provider-thread"}', NULL
        )
      `;

      yield* runMigrations({ toMigrationInclusive: 33 });

      const rows = yield* sql<{
        readonly threadId: string;
        readonly providerInstanceId: string;
        readonly providerThreadId: string;
        readonly syncState: string;
        readonly idleObservations: number;
      }>`
        SELECT
          thread_id AS "threadId",
          provider_instance_id AS "providerInstanceId",
          provider_thread_id AS "providerThreadId",
          sync_state AS "syncState",
          idle_observations AS "idleObservations"
        FROM provider_thread_links
      `;
      assert.deepStrictEqual(rows, [
        {
          threadId: "t3-imported-thread",
          providerInstanceId: "codex",
          providerThreadId: "codex-provider-thread",
          syncState: "idle",
          idleObservations: 2,
        },
      ]);
    }),
  );
});
