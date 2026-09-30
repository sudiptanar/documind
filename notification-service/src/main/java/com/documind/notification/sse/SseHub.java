package com.documind.notification.sse;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Open SSE connections on THIS instance, per user. Each instance consumes every event (unique consumer group),
 * so a user is reached whichever instance their browser happens to be connected to.
 */
@Slf4j
@Component
public class SseHub {

    private final Map<UUID, List<SseEmitter>> emitters = new ConcurrentHashMap<>();
    private final long timeoutMs;

    public SseHub(@Value("${documind.sse.timeout:30m}") Duration timeout) {
        this.timeoutMs = timeout.toMillis();
    }

    public SseEmitter register(UUID userId) {
        SseEmitter emitter = new SseEmitter(timeoutMs);
        emitters.computeIfAbsent(userId, id -> new CopyOnWriteArrayList<>()).add(emitter);
        Runnable remove = () -> remove(userId, emitter);
        emitter.onCompletion(remove);
        emitter.onTimeout(remove);
        emitter.onError(e -> remove.run());
        try {
            emitter.send(SseEmitter.event().name("connected").data("ok"));
        } catch (IOException e) {
            remove.run();
        }
        return emitter;
    }

    public void sendToUser(UUID userId, String eventName, Object data) {
        List<SseEmitter> list = emitters.getOrDefault(userId, List.of());
        for (SseEmitter emitter : list) {
            try {
                emitter.send(SseEmitter.event().name(eventName).data(data));
            } catch (IOException | IllegalStateException e) {
                remove(userId, emitter);
            }
        }
        log.debug("Pushed {} to {} connection(s) of user {}", eventName, list.size(), userId);
    }

    /** A comment every 25 s keeps load balancers (ALB idle timeout is 60 s) from closing quiet streams. */
    @Scheduled(fixedRateString = "${documind.sse.heartbeat:25s}")
    public void heartbeat() {
        emitters.forEach((userId, list) -> list.forEach(emitter -> {
            try {
                emitter.send(SseEmitter.event().comment("heartbeat"));
            } catch (IOException | IllegalStateException e) {
                remove(userId, emitter);
            }
        }));
    }

    public int connectionCount(UUID userId) {
        return emitters.getOrDefault(userId, List.of()).size();
    }

    private void remove(UUID userId, SseEmitter emitter) {
        emitters.computeIfPresent(userId, (id, list) -> {
            list.remove(emitter);
            return list.isEmpty() ? null : list;
        });
    }
}
