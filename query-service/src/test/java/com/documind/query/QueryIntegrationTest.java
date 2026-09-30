package com.documind.query;

import com.documind.query.support.FakeEmbeddingModel;
import com.documind.query.support.StubChatModel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.document.Document;
import org.springframework.ai.embedding.EmbeddingModel;
import org.springframework.ai.vectorstore.VectorStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.RequestPostProcessor;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.is;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.asyncDispatch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.request;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(properties = {
        "spring.ai.model.chat=none",
        "spring.ai.model.embedding=none",
        "spring.datasource.hikari.read-only=false",     // the test seeds vectors; production uses a read-only role
        "documind.query.similarity-threshold=0.15"})    // the fake bag-of-words embedder scores lower than a real model
@AutoConfigureMockMvc
@Testcontainers
class QueryIntegrationTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse("pgvector/pgvector:pg16").asCompatibleSubstituteFor("postgres"))
            .withDatabaseName("ingestion_db")
            .withInitScript("vector-schema.sql");

    static final StubChatModel STUB = new StubChatModel();

    @TestConfiguration
    static class FakeAi {
        @Bean
        @Primary
        EmbeddingModel fakeEmbeddingModel() {
            return new FakeEmbeddingModel();
        }

        @Bean
        @Primary
        ChatModel stubChatModel() {
            return STUB;
        }
    }

    static final UUID ALICE = UUID.randomUUID();
    static final UUID BOB = UUID.randomUUID();
    static final UUID ALICE_CONTRACT = UUID.randomUUID();
    static final UUID ALICE_HANDBOOK = UUID.randomUUID();
    static final UUID BOB_CONTRACT = UUID.randomUUID();

    @Autowired
    MockMvc mvc;

    @Autowired
    VectorStore vectorStore;

    @Autowired
    JdbcTemplate jdbc;

    @BeforeEach
    void seed() {
        jdbc.update("DELETE FROM vector_store");
        STUB.prompts.clear();
        vectorStore.add(List.of(
                chunk(ALICE, ALICE_CONTRACT, "alice-msa.pdf", 9,
                        "Termination. Either party may terminate this agreement with thirty days written notice."),
                chunk(ALICE, ALICE_CONTRACT, "alice-msa.pdf", 4,
                        "Payment terms. Invoices are payable within forty five days of receipt."),
                chunk(ALICE, ALICE_HANDBOOK, "handbook.pdf", 2,
                        "Employees may terminate employment with two weeks notice to their manager."),
                chunk(BOB, BOB_CONTRACT, "bob-lease.pdf", 1,
                        "Termination of this lease requires ninety days written notice by the tenant.")));
    }

    static Document chunk(UUID owner, UUID doc, String file, int page, String text) {
        return new Document(text, Map.of("owner_id", owner.toString(), "document_id", doc.toString(),
                "file_name", file, "page_number", page));
    }

    static RequestPostProcessor as(UUID user) {
        return SecurityMockMvcRequestPostProcessors.jwt().jwt(j -> j.subject(user.toString()));
    }

    @Test
    void answersWithNumberedSourcesFromTheCallersDocuments() throws Exception {
        mvc.perform(post("/api/query").with(as(ALICE)).contentType(MediaType.APPLICATION_JSON)
                        .content("{\"question\":\"How much notice is needed to terminate the agreement?\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.answer").value(StubChatModel.ANSWER))
                .andExpect(jsonPath("$.sources[0].index").value(1))
                .andExpect(jsonPath("$.sources[0].fileName").value("alice-msa.pdf"))
                .andExpect(jsonPath("$.sources[0].page").value(9))
                .andExpect(jsonPath("$.sources[*].documentId",
                        everyItem(is(org.hamcrest.Matchers.oneOf(ALICE_CONTRACT.toString(), ALICE_HANDBOOK.toString())))));

        String prompt = STUB.prompts.get(0).getContents();
        assertThat(prompt).contains("[1] (alice-msa.pdf, p.9)").contains("thirty days written notice");
        assertThat(prompt).doesNotContain("ninety days");   // Bob's lease never reaches Alice's prompt
    }

    @Test
    void ownerIsolation_bobNeverSeesAlicesChunks() throws Exception {
        mvc.perform(post("/api/query").with(as(BOB)).contentType(MediaType.APPLICATION_JSON)
                        .content("{\"question\":\"How much notice is needed to terminate the agreement?\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.sources.length()").value(1))   // non-empty, so the next check is not vacuous
                .andExpect(jsonPath("$.sources[*].documentId", everyItem(is(BOB_CONTRACT.toString()))));
    }

    @Test
    void documentIdsNarrowTheSearch() throws Exception {
        mvc.perform(post("/api/query").with(as(ALICE)).contentType(MediaType.APPLICATION_JSON)
                        .content("{\"question\":\"How much notice to terminate?\",\"documentIds\":[\"" + ALICE_HANDBOOK + "\"]}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.sources.length()").value(1))
                .andExpect(jsonPath("$.sources[*].documentId", everyItem(is(ALICE_HANDBOOK.toString()))));
    }

    @Test
    void noRelevantContextSkipsTheLlm() throws Exception {
        mvc.perform(post("/api/query").with(as(UUID.randomUUID())).contentType(MediaType.APPLICATION_JSON)
                        .content("{\"question\":\"What is the termination notice?\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.answer").value("I could not find this in your documents."))
                .andExpect(jsonPath("$.sources.length()").value(0));
        assertThat(STUB.prompts).isEmpty();
    }

    @Test
    void blankQuestionIsBadRequest() throws Exception {
        mvc.perform(post("/api/query").with(as(ALICE)).contentType(MediaType.APPLICATION_JSON)
                        .content("{\"question\":\"  \"}"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void streamsSourcesThenTokensThenDone() throws Exception {
        MvcResult started = mvc.perform(get("/api/chat/stream").param("q", "notice to terminate the agreement")
                        .with(as(ALICE)))
                .andExpect(request().asyncStarted())
                .andReturn();
        String body = mvc.perform(asyncDispatch(started)).andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();

        assertThat(body).contains("event:sources").contains("alice-msa.pdf")
                .contains("event:token").contains("may terminate").contains("event:done");
        assertThat(body.indexOf("event:sources")).isLessThan(body.indexOf("event:token"));
    }

    @Test
    void requiresAToken() throws Exception {
        mvc.perform(post("/api/query").contentType(MediaType.APPLICATION_JSON).content("{\"question\":\"x\"}"))
                .andExpect(status().isUnauthorized());
    }
}
