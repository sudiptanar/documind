package com.documind.ingestion.document;

import org.springframework.security.oauth2.jwt.Jwt;

import java.util.UUID;

/** The caller, always taken from the verified token, never from the request body. */
public record CurrentUser(UUID id, String email) {

    public static CurrentUser from(Jwt jwt) {
        return new CurrentUser(UUID.fromString(jwt.getSubject()), jwt.getClaimAsString("email"));
    }
}
