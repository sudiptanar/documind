package com.documind.ingestion.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

import java.net.URI;

@Configuration
public class S3Config {

    @Bean
    S3Client s3Client(AwsProperties props) {
        S3ClientBuilder builder = S3Client.builder().region(Region.of(props.region()));
        if (StringUtils.hasText(props.s3().endpoint())) {
            // LocalStack: path-style URLs and dummy credentials
            builder.endpointOverride(URI.create(props.s3().endpoint()))
                    .forcePathStyle(true)
                    .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")));
        }
        // On AWS the default credentials chain resolves the ECS task role.
        return builder.build();
    }
}
