package com.documind.auth.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

@ConfigurationProperties(prefix = "jwt")
public record JwtProperties(
        String privateKey,
        String publicKey,
        String privateKeyPath,
        String publicKeyPath,
        @DefaultValue("PT1H") Duration ttl,
        @DefaultValue("documind-auth") String issuer,
        @DefaultValue("documind-key-1") String keyId) {
}
