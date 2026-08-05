import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`
    CREATE TABLE IF NOT EXISTS provider_thread_links (
      thread_id TEXT PRIMARY KEY,
      provider_instance_id TEXT NOT NULL,
      provider_thread_id TEXT NOT NULL,
      source TEXT NOT NULL,
      cwd TEXT NOT NULL,
      provider_updated_at TEXT NOT NULL,
      last_synced_at TEXT,
      sync_state TEXT NOT NULL,
      last_error TEXT,
      externally_active INTEGER NOT NULL DEFAULT 0,
      idle_observations INTEGER NOT NULL DEFAULT 0,
      unavailable INTEGER NOT NULL DEFAULT 0,
      suppressed_at TEXT,
      UNIQUE (provider_instance_id, provider_thread_id)
    )
  `;

  yield* sql`
    CREATE TABLE IF NOT EXISTS provider_history_items (
      provider_instance_id TEXT NOT NULL,
      provider_thread_id TEXT NOT NULL,
      provider_turn_id TEXT NOT NULL,
      provider_item_id TEXT NOT NULL,
      projection_id TEXT NOT NULL,
      item_kind TEXT NOT NULL,
      content_hash TEXT NOT NULL,
      last_seen_at TEXT NOT NULL,
      PRIMARY KEY (provider_instance_id, provider_thread_id, provider_item_id)
    )
  `;

  yield* sql`
    CREATE INDEX IF NOT EXISTS idx_provider_thread_links_provider
    ON provider_thread_links(provider_instance_id, provider_thread_id)
  `;

  yield* sql`
    CREATE INDEX IF NOT EXISTS idx_provider_history_items_thread
    ON provider_history_items(provider_instance_id, provider_thread_id)
  `;

  // Preserve linkage created by older one-time importers. Transcript details
  // are reconciled by the first app-server scan; this migration only prevents
  // duplicate T3 threads before that scan runs.
  yield* sql`
    INSERT OR IGNORE INTO provider_thread_links (
      thread_id,
      provider_instance_id,
      provider_thread_id,
      source,
      cwd,
      provider_updated_at,
      last_synced_at,
      sync_state,
      last_error,
      externally_active,
      idle_observations,
      unavailable,
      suppressed_at
    )
    SELECT
      runtime.thread_id,
      COALESCE(runtime.provider_instance_id, 'codex'),
      json_extract(runtime.resume_cursor_json, '$.threadId'),
      't3',
      COALESCE(threads.worktree_path, projects.workspace_root, ''),
      COALESCE(threads.updated_at, runtime.last_seen_at),
      NULL,
      'idle',
      NULL,
      0,
      2,
      0,
      NULL
    FROM provider_session_runtime AS runtime
    LEFT JOIN projection_threads AS threads
      ON threads.thread_id = runtime.thread_id
    LEFT JOIN projection_projects AS projects
      ON projects.project_id = threads.project_id
    WHERE runtime.provider_name = 'codex'
      AND runtime.resume_cursor_json IS NOT NULL
      AND json_valid(runtime.resume_cursor_json)
      AND json_extract(runtime.resume_cursor_json, '$.threadId') IS NOT NULL
  `;
});
