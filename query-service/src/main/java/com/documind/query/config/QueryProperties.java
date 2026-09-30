package com.documind.query.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties(prefix = "documind.query")
public record QueryProperties(
        @DefaultValue("5") int defaultTopK,
        @DefaultValue("20") int maxTopK,
        @DefaultValue("0.35") double similarityThreshold) {
}
