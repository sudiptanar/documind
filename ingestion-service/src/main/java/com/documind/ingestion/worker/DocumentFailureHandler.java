package com.documind.ingestion.worker;

import com.documind.common.events.DocumentFailedEvent;
import com.documind.common.events.DocumentUploadedEvent;
import com.documind.common.events.Topics;
import com.documind.ingestion.document.DocumentRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Slf4j
@Component
@RequiredArgsConstructor
public class DocumentFailureHandler {

    private static final int MAX_REASON = 1000;

    private final DocumentRepository repo;
    private final KafkaTemplate<String, Object> kafka;

    public void markFailed(DocumentUploadedEvent event, String reason) {
        String trimmed = reason == null ? "Unknown error"
                : reason.length() > MAX_REASON ? reason.substring(0, MAX_REASON) : reason;
        repo.markFailed(event.documentId(), trimmed, Instant.now());
        kafka.send(Topics.DOCUMENT_FAILED, event.documentId().toString(),
                new DocumentFailedEvent(event.documentId(), event.ownerId(), event.ownerEmail(), event.fileName(),
                        trimmed, Instant.now()));
        log.warn("Document {} marked FAILED: {}", event.documentId(), trimmed);
    }
}
