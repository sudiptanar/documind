package com.documind.query.rag;

import com.documind.query.rag.QueryDtos.QueryRequest;
import com.documind.query.rag.QueryDtos.QueryResponse;
import com.documind.query.rag.QueryDtos.Source;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.messages.UserMessage;
import org.springframework.ai.chat.metadata.Usage;
import org.springframework.ai.chat.model.ChatResponse;
import org.springframework.ai.document.Document;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** Retrieve → build a grounded prompt → call the LLM → return the answer with numbered sources. */
@Slf4j
@Service
public class RagService {

    static final String NOT_FOUND = "I could not find this in your documents.";

    static final String SYSTEM_PROMPT = """
            You are DocuMind, an assistant that answers questions ONLY from the provided context.
            Rules:
            - Use only facts stated in the context. Do not use outside knowledge.
            - Cite every claim inline with the context number in square brackets, for example [1] or [2][3].
            - If the context does not contain the answer, reply exactly: "%s"
            - Be concise.
            """.formatted(NOT_FOUND);

    private static final int SNIPPET_CHARS = 300;

    private final RetrievalService retrieval;
    private final ChatClient chat;
    private final MeterRegistry meters;
    private final Timer latency;

    public RagService(RetrievalService retrieval, ChatClient chat, MeterRegistry meters) {
        this.retrieval = retrieval;
        this.chat = chat;
        this.meters = meters;
        this.latency = Timer.builder("documind.query.latency").register(meters);
    }

    public QueryResponse ask(UUID ownerId, QueryRequest request) {
        long start = System.nanoTime();
        List<Document> hits = retrieval.retrieve(ownerId, request.question(), request.documentIds(), request.topK());
        if (hits.isEmpty()) {
            return new QueryResponse(NOT_FOUND, List.of(), null, elapsedMs(start));
        }

        ChatResponse response = chat.prompt()
                .system(SYSTEM_PROMPT)
                // a Message, not a template string: PDF text may contain {braces} a template engine would choke on
                .messages(new UserMessage(userPrompt(request.question(), hits)))
                .call()
                .chatResponse();

        String answer = response == null || response.getResult() == null ? NOT_FOUND
                : response.getResult().getOutput().getText();
        String model = response == null ? null : response.getMetadata().getModel();
        recordTokens(response, model);
        long ms = elapsedMs(start);
        latency.record(java.time.Duration.ofMillis(ms));
        log.info("Answered question for owner {} from {} chunks in {} ms", ownerId, hits.size(), ms);
        return new QueryResponse(answer, sources(hits), model, ms);
    }

    /** SSE: one "sources" event, then "token" events as the model streams, then "done". */
    public Flux<ServerSentEvent<Object>> stream(UUID ownerId, QueryRequest request) {
        List<Document> hits = retrieval.retrieve(ownerId, request.question(), request.documentIds(), request.topK());
        if (hits.isEmpty()) {
            return Flux.just(event("sources", List.of()), event("token", NOT_FOUND), event("done", ""));
        }
        Flux<ServerSentEvent<Object>> tokens = chat.prompt()
                .system(SYSTEM_PROMPT)
                .messages(new UserMessage(userPrompt(request.question(), hits)))
                .stream()
                .content()
                .map(token -> event("token", token));
        return Flux.concat(Flux.just(event("sources", sources(hits))), tokens, Flux.just(event("done", "")));
    }

    static String userPrompt(String question, List<Document> hits) {
        StringBuilder context = new StringBuilder();
        for (int i = 0; i < hits.size(); i++) {
            Document hit = hits.get(i);
            Map<String, Object> meta = hit.getMetadata();
            context.append('[').append(i + 1).append("] (")
                    .append(meta.getOrDefault("file_name", "unknown")).append(", p.")
                    .append(meta.getOrDefault("page_number", "?")).append(")\n")
                    .append(hit.getText()).append("\n\n");
        }
        return "Context:\n" + context + "Question: " + question;
    }

    static List<Source> sources(List<Document> hits) {
        List<Source> sources = new ArrayList<>(hits.size());
        for (int i = 0; i < hits.size(); i++) {
            Document hit = hits.get(i);
            Map<String, Object> meta = hit.getMetadata();
            String text = hit.getText() == null ? "" : hit.getText().strip();
            sources.add(new Source(i + 1,
                    String.valueOf(meta.get("document_id")),
                    String.valueOf(meta.getOrDefault("file_name", "")),
                    toInteger(meta.get("page_number")),
                    hit.getScore(),
                    text.length() > SNIPPET_CHARS ? text.substring(0, SNIPPET_CHARS) + "..." : text));
        }
        return sources;
    }

    private void recordTokens(ChatResponse response, String model) {
        if (response == null || response.getMetadata() == null) {
            return;
        }
        Usage usage = response.getMetadata().getUsage();
        if (usage != null && usage.getTotalTokens() != null) {
            meters.counter("documind.llm.tokens", "model", model == null ? "unknown" : model)
                    .increment(usage.getTotalTokens());
        }
    }

    private static Integer toInteger(Object value) {
        if (value instanceof Number n) {
            return n.intValue();
        }
        try {
            return value == null ? null : Integer.valueOf(value.toString());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static ServerSentEvent<Object> event(String name, Object data) {
        return ServerSentEvent.builder(data).event(name).build();
    }

    private static long elapsedMs(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000;
    }
}
