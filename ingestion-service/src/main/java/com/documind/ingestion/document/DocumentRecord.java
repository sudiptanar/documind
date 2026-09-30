package com.documind.ingestion.document;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

/** Metadata row for an uploaded PDF. Named to avoid clashing with Spring AI's {@code Document}. */
@Entity
@Table(name = "documents")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class DocumentRecord {

    @Id
    private UUID id;

    @Column(name = "owner_id", nullable = false)
    private UUID ownerId;

    @Column(name = "file_name", nullable = false)
    private String fileName;

    @Column(name = "content_type", nullable = false)
    private String contentType;

    @Column(name = "size_bytes", nullable = false)
    private long sizeBytes;

    @Column(name = "s3_key", nullable = false)
    private String s3Key;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private DocumentStatus status;

    @Column(name = "chunk_count")
    private Integer chunkCount;

    private String error;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    public static DocumentRecord uploaded(UUID id, UUID ownerId, String fileName, String contentType,
                                          long sizeBytes, String s3Key) {
        DocumentRecord doc = new DocumentRecord();
        doc.id = id;
        doc.ownerId = ownerId;
        doc.fileName = fileName;
        doc.contentType = contentType;
        doc.sizeBytes = sizeBytes;
        doc.s3Key = s3Key;
        doc.status = DocumentStatus.UPLOADED;
        doc.createdAt = Instant.now();
        doc.updatedAt = doc.createdAt;
        return doc;
    }
}
