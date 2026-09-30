package com.documind.ingestion;

import com.documind.common.events.Topics;
import com.documind.ingestion.document.DocumentRepository;
import com.documind.ingestion.document.DocumentStatus;
import com.documind.ingestion.support.FakeEmbeddingModel;
import com.documind.ingestion.support.TestPdfs;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.ai.embedding.EmbeddingModel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.RequestPostProcessor;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.multipart;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(properties = {
        "spring.ai.model.embedding=none",
        "documind.kafka.retry.max-retries=1",
        "documind.kafka.retry.initial-interval=100ms",
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}"})
@EmbeddedKafka(kraft = true, partitions = 3)
@AutoConfigureMockMvc
@Testcontainers
class IngestionPipelineIntegrationTest {

    static final String BUCKET = "documind-uploads";

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse("pgvector/pgvector:pg16").asCompatibleSubstituteFor("postgres"))
            .withDatabaseName("ingestion_db");

    @Container
    static LocalStackContainer localstack = new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8"))
            .withServices(LocalStackContainer.Service.S3);

    static KafkaConsumer<String, String> probe;

    @DynamicPropertySource
    static void awsProps(DynamicPropertyRegistry registry) {
        registry.add("aws.region", localstack::getRegion);
        registry.add("aws.s3.endpoint", () -> localstack.getEndpoint().toString());
    }

    @BeforeAll
    static void setUp() {
        try (S3Client s3 = S3Client.builder()
                .endpointOverride(localstack.getEndpoint())
                .region(Region.of(localstack.getRegion()))
                .forcePathStyle(true)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(localstack.getAccessKey(), localstack.getSecretKey())))
                .build()) {
            s3.createBucket(b -> b.bucket(BUCKET));
        }
    }

    @AfterAll
    static void tearDown() {
        if (probe != null) {
            probe.close();
        }
    }

    /** Created on first use: the embedded broker only exists once the Spring context is up. */
    KafkaConsumer<String, String> probe() {
        if (probe == null) {
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString());
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-probe");
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            probe = new KafkaConsumer<>(props);
            probe.subscribe(List.of(Topics.DOCUMENT_INDEXED, Topics.DOCUMENT_FAILED, Topics.DOCUMENT_UPLOADED_DLT));
        }
        return probe;
    }

    static final List<ConsumerRecord<String, String>> seen = new ArrayList<>();

    @TestConfiguration
    static class FakeAi {
        @Bean
        @Primary
        EmbeddingModel fakeEmbeddingModel() {
            return new FakeEmbeddingModel();
        }
    }

    @Autowired
    EmbeddedKafkaBroker broker;

    @Autowired
    MockMvc mvc;

    @Autowired
    DocumentRepository repo;

    @Autowired
    JdbcTemplate jdbc;

    @Autowired
    ObjectMapper json;

    static RequestPostProcessor user(UUID id) {
        return SecurityMockMvcRequestPostProcessors.jwt()
                .jwt(j -> j.subject(id.toString()).claim("email", id + "@example.com"));
    }

    UUID upload(UUID owner, byte[] bytes, String name) throws Exception {
        String body = mvc.perform(multipart("/api/documents")
                        .file(new MockMultipartFile("file", name, "application/pdf", bytes))
                        .with(user(owner)))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.status").value("UPLOADED"))
                .andReturn().getResponse().getContentAsString();
        return UUID.fromString(json.readTree(body).get("id").asText());
    }

    ConsumerRecord<String, String> awaitRecord(String topic, UUID key) {
        long deadline = System.currentTimeMillis() + 60_000;
        while (System.currentTimeMillis() < deadline) {
            probe().poll(Duration.ofMillis(200)).forEach(seen::add);
            for (ConsumerRecord<String, String> r : seen) {
                if (r.topic().equals(topic) && key.toString().equals(r.key())) {
                    return r;
                }
            }
        }
        throw new AssertionError("No record on " + topic + " for " + key + "; seen: "
                + seen.stream().map(r -> r.topic() + "/" + r.key()).toList());
    }

    @Test
    void uploadedPdfIsChunkedEmbeddedAndAnnounced() throws Exception {
        UUID owner = UUID.randomUUID();
        UUID id = upload(owner, TestPdfs.contract(), "msa.pdf");

        await().atMost(Duration.ofSeconds(60)).untilAsserted(() ->
                assertThat(repo.findById(id).orElseThrow().getStatus()).isEqualTo(DocumentStatus.INDEXED));

        List<Map<String, Object>> rows = jdbc.queryForList(
                "SELECT metadata->>'page_number' AS page, metadata->>'owner_id' AS owner, content "
                        + "FROM vector_store WHERE metadata->>'document_id' = ?", id.toString());
        assertThat(rows).isNotEmpty();
        assertThat(rows).allSatisfy(r -> assertThat(r.get("owner")).isEqualTo(owner.toString()));
        assertThat(rows).extracting(r -> r.get("page")).contains("1", "2");
        assertThat(repo.findById(id).orElseThrow().getChunkCount()).isEqualTo(rows.size());

        ConsumerRecord<String, String> indexed = awaitRecord(Topics.DOCUMENT_INDEXED, id);
        assertThat(indexed.value()).contains("\"chunkCount\":" + rows.size()).contains("msa.pdf");

        mvc.perform(get("/api/documents/{id}", id).with(user(owner)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("INDEXED"));
    }

    @Test
    void corruptPdfIsMarkedFailedAndDeadLettered() throws Exception {
        UUID owner = UUID.randomUUID();
        byte[] corrupt = "%PDF-1.7\nthis is not really a pdf body".getBytes(StandardCharsets.US_ASCII);
        UUID id = upload(owner, corrupt, "broken.pdf");

        await().atMost(Duration.ofSeconds(60)).untilAsserted(() ->
                assertThat(repo.findById(id).orElseThrow().getStatus()).isEqualTo(DocumentStatus.FAILED));
        assertThat(repo.findById(id).orElseThrow().getError()).isNotBlank();

        assertThat(awaitRecord(Topics.DOCUMENT_FAILED, id).value()).contains("broken.pdf");
        assertThat(awaitRecord(Topics.DOCUMENT_UPLOADED_DLT, id)).isNotNull();
    }

    @Test
    void nonPdfIsRejectedUpFront() throws Exception {
        mvc.perform(multipart("/api/documents")
                        .file(new MockMultipartFile("file", "notes.pdf", "application/pdf",
                                "just some text".getBytes(StandardCharsets.UTF_8)))
                        .with(user(UUID.randomUUID())))
                .andExpect(status().isBadRequest());
    }

    @Test
    void usersOnlySeeTheirOwnDocuments() throws Exception {
        UUID alice = UUID.randomUUID();
        UUID bob = UUID.randomUUID();
        UUID id = upload(alice, TestPdfs.contract(), "alice.pdf");

        mvc.perform(get("/api/documents/{id}", id).with(user(bob))).andExpect(status().isNotFound());
        mvc.perform(get("/api/documents").with(user(bob)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(0));
        mvc.perform(get("/api/documents").with(user(alice)))
                .andExpect(jsonPath("$[0].id").value(id.toString()));
    }

    @Test
    void deleteRemovesVectorsAndRow() throws Exception {
        UUID owner = UUID.randomUUID();
        UUID id = upload(owner, TestPdfs.contract(), "to-delete.pdf");
        await().atMost(Duration.ofSeconds(60)).untilAsserted(() ->
                assertThat(repo.findById(id).orElseThrow().getStatus()).isEqualTo(DocumentStatus.INDEXED));

        mvc.perform(delete("/api/documents/{id}", id).with(user(owner))).andExpect(status().isNoContent());

        assertThat(repo.findById(id)).isEmpty();
        Integer left = jdbc.queryForObject(
                "SELECT count(*) FROM vector_store WHERE metadata->>'document_id' = ?", Integer.class, id.toString());
        assertThat(left).isZero();
    }

    @Test
    void requestsWithoutTokenAreRejected() throws Exception {
        mvc.perform(get("/api/documents")).andExpect(status().isUnauthorized());
    }
}
