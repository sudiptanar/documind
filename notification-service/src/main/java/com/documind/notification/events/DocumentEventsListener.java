package com.documind.notification.events;

import com.documind.common.events.DocumentEvent;
import com.documind.common.events.DocumentFailedEvent;
import com.documind.common.events.DocumentIndexedEvent;
import com.documind.common.events.Topics;
import com.documind.notification.email.EmailSender;
import com.documind.notification.sse.SseHub;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

/**
 * Two consumer groups on purpose:
 * - "notification-email" is shared, so each email is sent once across all instances;
 * - the push group is unique per instance, so every instance sees every event and can reach its own SSE clients.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DocumentEventsListener {

    private final EmailSender email;
    private final SseHub sse;
    private final ProcessedEventStore processed;

    @KafkaListener(topics = Topics.DOCUMENT_INDEXED, groupId = "notification-email")
    public void indexed(DocumentIndexedEvent e) {
        sendOnce(Topics.DOCUMENT_INDEXED, e, "Your document is ready",
                """
                Good news: '%s' was indexed into %d chunks.

                You can now ask DocuMind questions about it.
                """.formatted(e.fileName(), e.chunkCount()));
    }

    @KafkaListener(topics = Topics.DOCUMENT_FAILED, groupId = "notification-email")
    public void failed(DocumentFailedEvent e) {
        sendOnce(Topics.DOCUMENT_FAILED, e, "We could not process your document",
                """
                We could not index '%s'.

                Reason: %s
                """.formatted(e.fileName(), e.reason()));
    }

    @KafkaListener(topics = {Topics.DOCUMENT_INDEXED, Topics.DOCUMENT_FAILED},
            groupId = "notification-push-#{T(java.util.UUID).randomUUID()}",
            properties = "auto.offset.reset=latest")
    public void push(ConsumerRecord<String, Object> record) {
        if (record.value() instanceof DocumentEvent event) {
            sse.sendToUser(event.ownerId(), record.topic(), event);
        }
    }

    private void sendOnce(String topic, DocumentEvent event, String subject, String body) {
        String key = topic + ":" + event.documentId();
        if (processed.alreadyProcessed(key)) {
            log.info("Skipping duplicate {} for document {}", topic, event.documentId());
            return;
        }
        if (StringUtils.hasText(event.ownerEmail())) {
            email.send(event.ownerEmail(), subject, body);
        } else {
            log.warn("No email address on {} for document {}; skipping email", topic, event.documentId());
        }
        processed.markProcessed(key);
    }
}
