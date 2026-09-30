package com.documind.query.rag;

import com.documind.query.rag.QueryDtos.QueryRequest;
import com.documind.query.rag.QueryDtos.QueryResponse;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

import java.util.List;
import java.util.UUID;

@RestController
@Validated
@RequiredArgsConstructor
public class QueryController {

    private final RagService rag;

    @PostMapping("/api/query")
    public QueryResponse query(@AuthenticationPrincipal Jwt jwt, @Valid @RequestBody QueryRequest request) {
        return rag.ask(UUID.fromString(jwt.getSubject()), request);
    }

    /** Server-Sent Events for a chat-style UI. Spring MVC streams a returned Flux. */
    @GetMapping(value = "/api/chat/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<Object>> stream(@AuthenticationPrincipal Jwt jwt,
                                                @RequestParam("q") @NotBlank @Size(max = 2000) String question,
                                                @RequestParam(required = false) List<UUID> documentIds,
                                                @RequestParam(required = false) Integer topK) {
        return rag.stream(UUID.fromString(jwt.getSubject()), new QueryRequest(question, documentIds, topK));
    }
}
