package com.documind.gateway;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.reactive.server.WebTestClient;

import static org.springframework.security.test.web.reactive.server.SecurityMockServerConfigurers.mockJwt;

/** Downstream services are not running here, so a routed request surfaces as a 5xx, never a 401. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "AUTH_URL=http://localhost:1",
                "INGESTION_URL=http://localhost:1",
                "QUERY_URL=http://localhost:1",
                "NOTIFICATION_URL=http://localhost:1"})
@AutoConfigureWebTestClient
class GatewaySecurityTest {

    @Autowired
    WebTestClient client;

    @Test
    void protectedRouteWithoutTokenIsUnauthorized() {
        client.get().uri("/api/documents").exchange().expectStatus().isUnauthorized();
        client.post().uri("/api/query").exchange().expectStatus().isUnauthorized();
    }

    @Test
    void loginIsPublicAndRoutedToAuth() {
        client.post().uri("/auth/login").exchange().expectStatus().is5xxServerError();
    }

    @Test
    void authenticatedRequestIsRoutedAndGetsARequestId() {
        client.mutateWith(mockJwt()).get().uri("/api/documents").exchange()
                .expectStatus().is5xxServerError()
                .expectHeader().exists(RequestIdGlobalFilter.HEADER);
    }

    @Test
    void healthIsPublic() {
        client.get().uri("/actuator/health").exchange().expectStatus().isOk();
    }
}
