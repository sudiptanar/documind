package com.documind.ingestion.document;

import com.documind.common.events.DocumentUploadedEvent;
import com.documind.common.events.Topics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

/**
 * Publishes only AFTER the upload transaction commits, so consumers never see an event for a rolled-back row.
 * The remaining gap (commit succeeds, send fails) is what the transactional outbox pattern closes.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DocumentEventRelay {

    private final KafkaTemplate<String, Object> kafka;

    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void onUploaded(DocumentUploadedEvent event) {
        // Key = document id: every event for one document lands on one partition, in order.
        kafka.send(Topics.DOCUMENT_UPLOADED, event.documentId().toString(), event)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to publish document.uploaded for {}", event.documentId(), ex);
                    } else {
                        log.info("Published document.uploaded for {} to partition {}",
                                event.documentId(), result.getRecordMetadata().partition());
                    }
                });
    }
}
