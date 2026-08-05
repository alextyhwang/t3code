import { ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";

import { ProviderHistoryRepository } from "../Services/ProviderHistory.ts";
import { SqlitePersistenceMemory } from "./Sqlite.ts";
import { ProviderHistoryRepositoryLive } from "./ProviderHistory.ts";

const layer = it.layer(
  ProviderHistoryRepositoryLive.pipe(Layer.provideMerge(SqlitePersistenceMemory)),
);

layer("ProviderHistoryRepository", (it) => {
  it.effect("upserts links and item checkpoints idempotently", () =>
    Effect.gen(function* () {
      const repository = yield* ProviderHistoryRepository;
      const providerInstanceId = ProviderInstanceId.make("codex");
      const threadId = ThreadId.make("t3-thread");
      const base = {
        threadId,
        providerInstanceId,
        providerThreadId: "provider-thread",
        source: "vscode",
        cwd: "C:/repo",
        providerUpdatedAt: "2026-07-15T18:00:00.000Z",
        lastSyncedAt: null,
        syncState: "idle" as const,
        lastError: null,
        externallyActive: false,
        idleObservations: 2,
        unavailable: false,
        suppressedAt: null,
      };
      yield* repository.upsertThreadLink(base);
      yield* repository.upsertThreadLink({
        ...base,
        providerUpdatedAt: "2026-07-15T18:00:15.000Z",
      });

      const linked = yield* repository.getThreadLinkByProvider({
        providerInstanceId,
        providerThreadId: "provider-thread",
      });
      assert.equal(Option.isSome(linked), true);
      assert.equal(Option.getOrThrow(linked).providerUpdatedAt, "2026-07-15T18:00:15.000Z");

      yield* repository.upsertItemLink({
        providerInstanceId,
        providerThreadId: "provider-thread",
        providerTurnId: "turn-1",
        providerItemId: "item-1",
        projectionId: "message-1",
        itemKind: "message",
        contentHash: "hash-1",
        lastSeenAt: "2026-07-15T18:00:15.000Z",
      });
      yield* repository.upsertItemLink({
        providerInstanceId,
        providerThreadId: "provider-thread",
        providerTurnId: "turn-1",
        providerItemId: "item-1",
        projectionId: "message-1",
        itemKind: "message",
        contentHash: "hash-2",
        lastSeenAt: "2026-07-15T18:00:30.000Z",
      });
      const items = yield* repository.listItemLinks({
        providerInstanceId,
        providerThreadId: "provider-thread",
      });
      assert.equal(items.length, 1);
      assert.equal(items[0]?.contentHash, "hash-2");
    }),
  );
});
