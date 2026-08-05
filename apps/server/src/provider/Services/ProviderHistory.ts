import type { ProviderInstanceId } from "@t3tools/contracts";
import type * as Effect from "effect/Effect";

import type { ProviderAdapterRequestError } from "../Errors.ts";

export type ProviderHistoryTurnState = "completed" | "interrupted" | "failed" | "inProgress";

export type ProviderHistoryItem =
  | {
      readonly kind: "message";
      readonly providerItemId: string;
      readonly role: "user" | "assistant";
      readonly text: string;
    }
  | {
      readonly kind: "plan";
      readonly providerItemId: string;
      readonly text: string;
    }
  | {
      readonly kind: "activity";
      readonly providerItemId: string;
      readonly activityKind: "command" | "file-change";
      readonly summary: string;
      readonly payload: unknown;
      readonly status: string;
    };

export interface ProviderHistoryTurn {
  readonly providerTurnId: string;
  readonly state: ProviderHistoryTurnState;
  readonly startedAt: string;
  readonly completedAt: string | null;
  readonly items: ReadonlyArray<ProviderHistoryItem>;
}

export interface ProviderHistoryThreadSummary {
  readonly providerThreadId: string;
  readonly cwd: string;
  readonly title: string;
  readonly preview: string;
  readonly source: string;
  readonly archived: boolean;
  readonly ephemeral: boolean;
  readonly parentThreadId: string | null;
  readonly createdAt: string;
  readonly updatedAt: string;
  readonly externallyActive: boolean;
}

export interface ProviderHistoryThreadSnapshot extends ProviderHistoryThreadSummary {
  readonly turns: ReadonlyArray<ProviderHistoryTurn>;
}

export interface ProviderHistoryAdapterShape {
  readonly providerInstanceId: ProviderInstanceId;
  readonly listThreads: () => Effect.Effect<
    ReadonlyArray<ProviderHistoryThreadSummary>,
    ProviderAdapterRequestError
  >;
  readonly readThread: (
    providerThreadId: string,
    archived: boolean,
  ) => Effect.Effect<ProviderHistoryThreadSnapshot, ProviderAdapterRequestError>;
  readonly setName: (
    providerThreadId: string,
    name: string,
  ) => Effect.Effect<void, ProviderAdapterRequestError>;
  readonly setArchived: (
    providerThreadId: string,
    archived: boolean,
  ) => Effect.Effect<void, ProviderAdapterRequestError>;
}
