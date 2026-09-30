package com.documind.ingestion.document;

import java.time.Instant;
import java.util.UUID;

public record DocumentDto(
        UUID id,
        String fileName,
        long sizeBytes,
        DocumentStatus status,
        Integer chunkCount,
        String error,
        Instant createdAt,
        Instant updatedAt) {

    public static DocumentDto from(DocumentRecord d) {
        return new DocumentDto(d.getId(), d.getFileName(), d.getSizeBytes(), d.getStatus(), d.getChunkCount(),
                d.getError(), d.getCreatedAt(), d.getUpdatedAt());
    }
}
