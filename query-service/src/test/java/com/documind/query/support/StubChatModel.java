package com.documind.query.support;

import org.springframework.ai.chat.messages.AssistantMessage;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.chat.model.ChatResponse;
import org.springframework.ai.chat.model.Generation;
import org.springframework.ai.chat.prompt.Prompt;
import reactor.core.publisher.Flux;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/** Returns a canned answer and records every prompt, so tests can assert what the LLM would have seen. */
public class StubChatModel implements ChatModel {

    public static final String ANSWER = "Either party may terminate with 30 days notice [1].";

    public final List<Prompt> prompts = new CopyOnWriteArrayList<>();

    @Override
    public ChatResponse call(Prompt prompt) {
        prompts.add(prompt);
        return new ChatResponse(List.of(new Generation(new AssistantMessage(ANSWER))));
    }

    @Override
    public Flux<ChatResponse> stream(Prompt prompt) {
        prompts.add(prompt);
        return Flux.just("Either party ", "may terminate ", "with 30 days notice [1].")
                .map(t -> new ChatResponse(List.of(new Generation(new AssistantMessage(t)))));
    }
}
