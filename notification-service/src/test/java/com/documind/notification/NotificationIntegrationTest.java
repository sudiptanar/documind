package com.documind.notification;

import com.documind.common.events.DocumentFailedEvent;
import com.documind.common.events.DocumentIndexedEvent;
import com.documind.common.events.Topics;
import com.documind.notification.email.EmailSender;
import com.documind.notification.sse.SseHub;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.request;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer",
        "spring.kafka.producer.value-serializer=org.springframework.kafka.support.serializer.JsonSerializer"})
@EmbeddedKafka(kraft = true, partitions = 1, topics = {Topics.DOCUMENT_INDEXED, Topics.DOCUMENT_FAILED})
@AutoConfigureMockMvc
@Testcontainers
class NotificationIntegrationTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("notification_db");

    record Mail(String to, String subject, String body) {
    }

    static final List<Mail> SENT = new CopyOnWriteArrayList<>();

    @TestConfiguration
    static class RecordingMail {
        @Bean
        @Primary
        EmailSender recordingEmailSender() {
            return (to, subject, body) -> SENT.add(new Mail(to, subject, body));
        }
    }

    @Autowired
    KafkaTemplate<String, Object> kafka;

    @Autowired
    KafkaListenerEndpointRegistry listeners;

    @Autowired
    EmbeddedKafkaBroker broker;

    @Autowired
    MockMvc mvc;

    @MockitoSpyBean
    SseHub sse;

    @BeforeEach
    void waitForListeners() {
        // The push group reads from "latest", so only publish once every listener owns its partitions.
        listeners.getListenerContainers().forEach(c ->
                ContainerTestUtils.waitForAssignment(c, c.getContainerProperties().getTopics().length));
    }

    List<Mail> mailTo(String address) {
        return SENT.stream().filter(m -> m.to().equals(address)).toList();
    }

    @Test
    void indexedEventEmailsTheOwnerExactlyOnceAndPushesLive() {
        UUID doc = UUID.randomUUID();
        UUID owner = UUID.randomUUID();
        String address = owner + "@example.com";
        DocumentIndexedEvent event = new DocumentIndexedEvent(doc, owner, address, "msa.pdf", 12, Instant.now());

        kafka.send(Topics.DOCUMENT_INDEXED, doc.toString(), event);
        kafka.send(Topics.DOCUMENT_INDEXED, doc.toString(), event);   // at-least-once redelivery

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(mailTo(address)).hasSize(1));
        assertThat(mailTo(address).get(0).subject()).isEqualTo("Your document is ready");
        assertThat(mailTo(address).get(0).body()).contains("msa.pdf").contains("12 chunks");

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                verify(sse, org.mockito.Mockito.atLeastOnce()).sendToUser(eq(owner), eq(Topics.DOCUMENT_INDEXED), any()));

        // still exactly one email after the duplicate has certainly been consumed
        await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(mailTo(address)).hasSize(1));
    }

    @Test
    void failedEventEmailsTheReason() {
        UUID doc = UUID.randomUUID();
        String address = "failure-" + doc + "@example.com";
        kafka.send(Topics.DOCUMENT_FAILED, doc.toString(),
                new DocumentFailedEvent(doc, UUID.randomUUID(), address, "scan.pdf", "No extractable text", Instant.now()));

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(mailTo(address)).hasSize(1));
        assertThat(mailTo(address).get(0).body()).contains("scan.pdf").contains("No extractable text");
    }

    @Test
    void streamOpensForAnAuthenticatedUser() throws Exception {
        UUID user = UUID.randomUUID();
        mvc.perform(get("/api/notifications/stream")
                        .with(SecurityMockMvcRequestPostProcessors.jwt().jwt(j -> j.subject(user.toString()))))
                .andExpect(request().asyncStarted());
        assertThat(sse.connectionCount(user)).isEqualTo(1);
    }

    @Test
    void streamRequiresAToken() throws Exception {
        mvc.perform(get("/api/notifications/stream")).andExpect(status().isUnauthorized());
    }
}
