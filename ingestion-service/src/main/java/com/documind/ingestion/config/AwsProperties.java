package com.documind.ingestion.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "aws")
public record AwsProperties(String region, S3 s3) {

    /** @param endpoint set only for LocalStack; empty on AWS so the SDK uses real S3 and the task role */
    public record S3(String bucket, String endpoint) {
    }
}
