package com.documind.query.rag;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

import java.util.List;
import java.util.UUID;

public final class QueryDtos {

    private QueryDtos() {
    }

    public record QueryRequest(
            @NotBlank @Size(max = 2000) String question,
            @Size(max = 50) List<UUID> documentIds,
            @Min(1) @Max(20) Integer topK) {
    }

    public record Source(int index, String documentId, String fileName, Integer page, Double score, String snippet) {
    }

    public record QueryResponse(String answer, List<Source> sources, String model, long latencyMs) {
    }
}
