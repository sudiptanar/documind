package com.documind.common.events;

import java.time.Instant;
import java.util.UUID;

/** Published by the ingestion API after a PDF is stored in S3 and its row is committed. */
public record DocumentUploadedEvent(
        UUID documentId,
        UUID ownerId,
        String ownerEmail,
        String s3Bucket,
        String s3Key,
        String fileName,
        String contentType,
        Instant uploadedAt) implements DocumentEvent {
}
