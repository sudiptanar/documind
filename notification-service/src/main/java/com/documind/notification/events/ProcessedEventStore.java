package com.documind.notification.events;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class ProcessedEventStore {

    private final JdbcTemplate jdbc;

    public boolean alreadyProcessed(String key) {
        Integer count = jdbc.queryForObject("SELECT count(*) FROM processed_events WHERE event_key = ?", Integer.class, key);
        return count != null && count > 0;
    }

    /** Recorded after the side effect succeeds, so a failed send is retried rather than silently skipped. */
    public void markProcessed(String key) {
        jdbc.update("INSERT INTO processed_events (event_key) VALUES (?) ON CONFLICT (event_key) DO NOTHING", key);
    }
}
