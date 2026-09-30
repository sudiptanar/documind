package com.documind.common.events;

import java.time.Instant;
import java.util.UUID;

/** Published by the ingestion worker once every chunk of a document is embedded and stored. */
public record DocumentIndexedEvent(
        UUID documentId,
        UUID ownerId,
        String ownerEmail,
        String fileName,
        int chunkCount,
        Instant indexedAt) implements DocumentEvent {
}
