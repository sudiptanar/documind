package com.documind.query.rag;

import com.documind.query.config.QueryProperties;
import lombok.RequiredArgsConstructor;
import org.springframework.ai.document.Document;
import org.springframework.ai.vectorstore.SearchRequest;
import org.springframework.ai.vectorstore.VectorStore;
import org.springframework.ai.vectorstore.filter.Filter;
import org.springframework.ai.vectorstore.filter.FilterExpressionBuilder;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.UUID;

/**
 * Similarity search, always scoped to the caller. The owner_id filter is THE tenant-isolation control:
 * without it, one user's question could surface another user's documents.
 */
@Service
@RequiredArgsConstructor
public class RetrievalService {

    private final VectorStore vectorStore;
    private final QueryProperties props;

    public List<Document> retrieve(UUID ownerId, String question, List<UUID> documentIds, Integer topK) {
        FilterExpressionBuilder b = new FilterExpressionBuilder();
        FilterExpressionBuilder.Op ownerOnly = b.eq("owner_id", ownerId.toString());
        Filter.Expression filter = (documentIds == null || documentIds.isEmpty())
                ? ownerOnly.build()
                : b.and(ownerOnly, b.in("document_id", documentIds.stream().map(UUID::toString).toArray())).build();

        int k = topK == null ? props.defaultTopK() : Math.min(Math.max(topK, 1), props.maxTopK());
        return vectorStore.similaritySearch(SearchRequest.builder()
                .query(question)
                .topK(k)
                .similarityThreshold(props.similarityThreshold())
                .filterExpression(filter)
                .build());
    }
}
