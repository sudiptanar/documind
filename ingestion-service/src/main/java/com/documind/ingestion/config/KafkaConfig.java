package com.documind.ingestion.config;

import com.documind.common.events.DocumentUploadedEvent;
import com.documind.common.events.Topics;
import com.documind.ingestion.worker.DocumentFailureHandler;
import com.documind.ingestion.worker.InvalidPdfException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.springframework.ai.retry.NonTransientAiException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.kafka.core.KafkaAdmin;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

@Slf4j
@Configuration
public class KafkaConfig {

    @Value("${documind.kafka.partitions}")
    private int partitions;

    @Value("${documind.kafka.replicas}")
    private int replicas;

    /** Declared here so a fresh broker (tests, MSK) gets the topics without a manual script. */
    @Bean
    KafkaAdmin.NewTopics documindTopics() {
        return new KafkaAdmin.NewTopics(
                topic(Topics.DOCUMENT_UPLOADED),
                topic(Topics.DOCUMENT_INDEXED),
                topic(Topics.DOCUMENT_FAILED),
                topic(Topics.DOCUMENT_UPLOADED_DLT));
    }

    private NewTopic topic(String name) {
        return TopicBuilder.name(name).partitions(partitions).replicas(replicas).build();
    }

    /**
     * Retries transient failures with exponential backoff, then marks the document FAILED, emits
     * document.failed and parks the record on document.uploaded.DLT. Bad input is not retried.
     */
    @Bean
    DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, Object> jsonTemplate,
                                          KafkaProperties kafkaProperties,
                                          DocumentFailureHandler failures,
                                          @Value("${documind.kafka.retry.max-retries}") int maxRetries,
                                          @Value("${documind.kafka.retry.initial-interval}") Duration initialInterval,
                                          @Value("${documind.kafka.retry.multiplier}") double multiplier) {
        // Records that failed deserialization carry raw bytes, which the JSON template cannot re-send.
        Map<String, Object> byteProps = kafkaProperties.buildProducerProperties(null);
        byteProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        KafkaTemplate<String, byte[]> bytesTemplate = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(byteProps));

        Map<Class<?>, KafkaOperations<?, ?>> templates = new LinkedHashMap<>();
        templates.put(byte[].class, bytesTemplate);
        templates.put(Object.class, jsonTemplate);
        // Explicit destination: recent spring-kafka defaults to "<topic>-dlt", but our topics use ".DLT".
        DeadLetterPublishingRecoverer deadLetters = new DeadLetterPublishingRecoverer(templates,
                (record, ex) -> new TopicPartition(record.topic() + ".DLT", record.partition()));

        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(maxRetries);
        backOff.setInitialInterval(initialInterval.toMillis());
        backOff.setMultiplier(multiplier);

        DefaultErrorHandler handler = new DefaultErrorHandler((record, ex) -> {
            String reason = NestedExceptionUtils.getMostSpecificCause(ex).getMessage();
            log.error("Giving up on record {} after retries: {}", record.key(), reason);
            if (record.value() instanceof DocumentUploadedEvent event) {
                failures.markFailed(event, reason);
            }
            deadLetters.accept(record, ex);
        }, backOff);
        // Retrying cannot fix these: a bad PDF, an unreadable message, a missing object, a rejected API key.
        handler.addNotRetryableExceptions(InvalidPdfException.class, DeserializationException.class,
                NoSuchKeyException.class, NonTransientAiException.class);
        return handler;
    }
}
