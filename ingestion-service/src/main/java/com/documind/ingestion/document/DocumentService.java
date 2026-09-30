package com.documind.ingestion.document;

import com.documind.common.events.DocumentUploadedEvent;
import com.documind.ingestion.config.AwsProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.vectorstore.VectorStore;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;
import org.springframework.web.multipart.MultipartFile;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class DocumentService {

    static final String PDF = "application/pdf";
    private static final byte[] PDF_MAGIC = "%PDF-".getBytes(StandardCharsets.US_ASCII);
    private static final Set<String> ACCEPTED_TYPES = Set.of(PDF, "application/octet-stream", "application/x-pdf");

    private final S3Client s3;
    private final AwsProperties aws;
    private final DocumentRepository repo;
    private final VectorStore vectorStore;
    private final ApplicationEventPublisher events;

    /**
     * Fast path only: store the PDF, save the row, and (after commit) publish document.uploaded.
     * Parsing and embedding happen asynchronously in the worker.
     */
    @Transactional
    public DocumentDto upload(CurrentUser user, MultipartFile file) {
        byte[] bytes = validatePdf(file);
        UUID id = UUID.randomUUID();
        String key = "%s/%s.pdf".formatted(user.id(), id);
        String fileName = cleanFileName(file.getOriginalFilename());

        s3.putObject(b -> b.bucket(aws.s3().bucket()).key(key).contentType(PDF), RequestBody.fromBytes(bytes));
        DocumentRecord doc = repo.save(DocumentRecord.uploaded(id, user.id(), fileName, PDF, bytes.length, key));

        // Delivered to Kafka by DocumentEventRelay only once this transaction commits.
        events.publishEvent(new DocumentUploadedEvent(id, user.id(), user.email(), aws.s3().bucket(), key,
                fileName, PDF, Instant.now()));
        log.info("Stored document {} ({} bytes) for owner {}", id, bytes.length, user.id());
        return DocumentDto.from(doc);
    }

    @Transactional(readOnly = true)
    public List<DocumentDto> list(CurrentUser user) {
        return repo.findAllByOwnerIdOrderByCreatedAtDesc(user.id()).stream().map(DocumentDto::from).toList();
    }

    @Transactional(readOnly = true)
    public DocumentDto get(CurrentUser user, UUID id) {
        return repo.findByIdAndOwnerId(id, user.id()).map(DocumentDto::from)
                .orElseThrow(() -> new DocumentNotFoundException(id));
    }

    @Transactional
    public void delete(CurrentUser user, UUID id) {
        DocumentRecord doc = repo.findByIdAndOwnerId(id, user.id())
                .orElseThrow(() -> new DocumentNotFoundException(id));
        vectorStore.delete("document_id == '" + id + "'");
        s3.deleteObject(b -> b.bucket(aws.s3().bucket()).key(doc.getS3Key()));
        repo.delete(doc);
        log.info("Deleted document {} for owner {}", id, user.id());
    }

    private static byte[] validatePdf(MultipartFile file) {
        if (file == null || file.isEmpty()) {
            throw new InvalidUploadException("File is empty");
        }
        String type = file.getContentType();
        if (type != null && !ACCEPTED_TYPES.contains(type.toLowerCase())) {
            throw new InvalidUploadException("Only PDF files are supported (got " + type + ")");
        }
        byte[] bytes;
        try {
            bytes = file.getBytes();
        } catch (IOException e) {
            throw new InvalidUploadException("Could not read upload");
        }
        // Trust the bytes, not the extension or header: every PDF starts with %PDF-
        if (bytes.length < PDF_MAGIC.length || !Arrays.equals(bytes, 0, PDF_MAGIC.length, PDF_MAGIC, 0, PDF_MAGIC.length)) {
            throw new InvalidUploadException("File is not a PDF");
        }
        return bytes;
    }

    private static String cleanFileName(String original) {
        String name = StringUtils.hasText(original) ? StringUtils.getFilename(original.replace('\\', '/')) : null;
        if (!StringUtils.hasText(name)) {
            return "document.pdf";
        }
        return name.length() > 255 ? name.substring(name.length() - 255) : name;
    }
}
