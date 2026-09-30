package com.documind.ingestion.document;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface DocumentRepository extends JpaRepository<DocumentRecord, UUID> {

    List<DocumentRecord> findAllByOwnerIdOrderByCreatedAtDesc(UUID ownerId);

    Optional<DocumentRecord> findByIdAndOwnerId(UUID id, UUID ownerId);

    @Transactional
    @Modifying
    @Query("update DocumentRecord d set d.status = :status, d.error = null, d.updatedAt = :now where d.id = :id")
    int updateStatus(@Param("id") UUID id, @Param("status") DocumentStatus status, @Param("now") Instant now);

    @Transactional
    @Modifying
    @Query("""
            update DocumentRecord d set d.status = com.documind.ingestion.document.DocumentStatus.INDEXED,
                   d.chunkCount = :chunks, d.error = null, d.updatedAt = :now
            where d.id = :id""")
    int markIndexed(@Param("id") UUID id, @Param("chunks") int chunks, @Param("now") Instant now);

    @Transactional
    @Modifying
    @Query("""
            update DocumentRecord d set d.status = com.documind.ingestion.document.DocumentStatus.FAILED,
                   d.error = :error, d.updatedAt = :now
            where d.id = :id""")
    int markFailed(@Param("id") UUID id, @Param("error") String error, @Param("now") Instant now);
}
