package com.documind.ingestion.worker;

import com.documind.common.events.DocumentIndexedEvent;
import com.documind.common.events.DocumentUploadedEvent;
import com.documind.common.events.Topics;
import com.documind.ingestion.document.DocumentRepository;
import com.documind.ingestion.document.DocumentStatus;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.document.Document;
import org.springframework.ai.vectorstore.VectorStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.s3.S3Client;

import java.time.Instant;
import java.util.List;

/**
 * Consumes document.uploaded: download → extract → chunk → embed → store → document.indexed.
 * Idempotent: a redelivered event replaces the document's vectors instead of duplicating them.
 * Failures are retried and dead-lettered by the error handler in KafkaConfig.
 */
@Slf4j
@Component
public class IngestionWorker {

    private final S3Client s3;
    private final PdfChunker chunker;
    private final VectorStore vectorStore;
    private final DocumentRepository repo;
    private final KafkaTemplate<String, Object> kafka;
    private final int batchSize;
    private final Timer duration;
    private final Counter chunksCreated;

    public IngestionWorker(S3Client s3, PdfChunker chunker, VectorStore vectorStore, DocumentRepository repo,
                           KafkaTemplate<String, Object> kafka, MeterRegistry meters,
                           @Value("${documind.ingestion.embed-batch-size:100}") int batchSize) {
        this.s3 = s3;
        this.chunker = chunker;
        this.vectorStore = vectorStore;
        this.repo = repo;
        this.kafka = kafka;
        this.batchSize = batchSize;
        this.duration = Timer.builder("documind.ingestion.duration")
                .description("Time to download, chunk, embed and store one document").register(meters);
        this.chunksCreated = Counter.builder("documind.chunks.created").register(meters);
    }

    @KafkaListener(topics = Topics.DOCUMENT_UPLOADED, concurrency = "${documind.ingestion.concurrency:3}")
    public void handle(DocumentUploadedEvent event) {
        duration.record(() -> process(event));
    }

    private void process(DocumentUploadedEvent event) {
        if (repo.updateStatus(event.documentId(), DocumentStatus.PROCESSING, Instant.now()) == 0) {
            log.info("Document {} no longer exists (deleted?); skipping", event.documentId());
            return;
        }

        byte[] pdf = s3.getObjectAsBytes(b -> b.bucket(event.s3Bucket()).key(event.s3Key())).asByteArray();
        List<Document> chunks = chunker.chunk(pdf, event);

        vectorStore.delete("document_id == '" + event.documentId() + "'");
        for (int from = 0; from < chunks.size(); from += batchSize) {
            vectorStore.add(chunks.subList(from, Math.min(from + batchSize, chunks.size())));  // embeds + inserts
        }

        repo.markIndexed(event.documentId(), chunks.size(), Instant.now());
        chunksCreated.increment(chunks.size());
        kafka.send(Topics.DOCUMENT_INDEXED, event.documentId().toString(),
                new DocumentIndexedEvent(event.documentId(), event.ownerId(), event.ownerEmail(), event.fileName(),
                        chunks.size(), Instant.now()));
        log.info("Indexed document {} into {} chunks", event.documentId(), chunks.size());
    }
}
