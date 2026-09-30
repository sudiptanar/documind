package com.documind.ingestion.worker;

import com.documind.common.events.DocumentUploadedEvent;
import org.springframework.ai.document.Document;
import org.springframework.ai.reader.pdf.PagePdfDocumentReader;
import org.springframework.ai.reader.pdf.config.PdfDocumentReaderConfig;
import org.springframework.ai.transformer.splitter.TokenTextSplitter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** PDF bytes → one Document per page (page_number in metadata) → ~800-token chunks tagged with owner and document. */
@Component
public class PdfChunker {

    public static final String DOCUMENT_ID = "document_id";
    public static final String OWNER_ID = "owner_id";
    public static final String FILE_NAME = "file_name";
    public static final String PAGE_NUMBER = "page_number";
    public static final String CHUNK_INDEX = "chunk_index";

    private final TokenTextSplitter splitter;

    public PdfChunker(@Value("${documind.ingestion.chunk-size-tokens:800}") int chunkSize) {
        this.splitter = TokenTextSplitter.builder()
                .withChunkSize(chunkSize)
                .withMinChunkSizeChars(350)
                .withMinChunkLengthToEmbed(5)
                .withMaxNumChunks(10_000)
                .withKeepSeparator(true)
                .build();
    }

    public List<Document> chunk(byte[] pdf, DocumentUploadedEvent event) {
        List<Document> pages;
        try {
            PagePdfDocumentReader reader = new PagePdfDocumentReader(new ByteArrayResource(pdf),
                    PdfDocumentReaderConfig.builder().withPagesPerDocument(1).build());
            pages = reader.get();
        } catch (RuntimeException e) {
            throw new InvalidPdfException("Could not parse PDF: " + e.getMessage(), e);
        }

        List<Document> withText = pages.stream()
                .filter(p -> p.getText() != null && !p.getText().isBlank())
                .toList();
        if (withText.isEmpty()) {
            throw new InvalidPdfException("No extractable text; scanned PDFs (OCR) are not supported yet");
        }

        List<Document> chunks = splitter.apply(withText);
        List<Document> tagged = new ArrayList<>(chunks.size());
        for (int i = 0; i < chunks.size(); i++) {
            Document chunk = chunks.get(i);
            Map<String, Object> metadata = new HashMap<>();
            metadata.put(PAGE_NUMBER, chunk.getMetadata().getOrDefault(PAGE_NUMBER, 0));
            metadata.put(DOCUMENT_ID, event.documentId().toString());
            metadata.put(OWNER_ID, event.ownerId().toString());
            metadata.put(FILE_NAME, event.fileName());
            metadata.put(CHUNK_INDEX, i);
            tagged.add(new Document(chunk.getText(), metadata));
        }
        return tagged;
    }
}
