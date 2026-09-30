package com.documind.common.events;

import java.time.Instant;
import java.util.UUID;

/** Published by the ingestion worker when a document cannot be indexed (after retries). */
public record DocumentFailedEvent(
        UUID documentId,
        UUID ownerId,
        String ownerEmail,
        String fileName,
        String reason,
        Instant failedAt) implements DocumentEvent {
}
