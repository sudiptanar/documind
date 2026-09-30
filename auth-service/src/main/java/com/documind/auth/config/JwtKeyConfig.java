package com.documind.auth.config;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.source.ImmutableJWKSet;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.converter.RsaKeyConverters;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtEncoder;
import org.springframework.security.oauth2.jwt.JwtValidators;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;
import org.springframework.security.oauth2.jwt.NimbusJwtEncoder;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;

import static org.springframework.util.StringUtils.hasText;

@Slf4j
@Configuration
public class JwtKeyConfig {

    @Bean
    RSAKey rsaKey(JwtProperties props) throws IOException, NoSuchAlgorithmException {
        RSAPrivateKey privateKey;
        RSAPublicKey publicKey;
        if (hasText(props.privateKey()) && hasText(props.publicKey())) {
            privateKey = RsaKeyConverters.pkcs8().convert(stream(props.privateKey()));
            publicKey = RsaKeyConverters.x509().convert(stream(props.publicKey()));
        } else if (hasText(props.privateKeyPath()) && hasText(props.publicKeyPath())) {
            try (InputStream priv = Files.newInputStream(Path.of(props.privateKeyPath()));
                 InputStream pub = Files.newInputStream(Path.of(props.publicKeyPath()))) {
                privateKey = RsaKeyConverters.pkcs8().convert(priv);
                publicKey = RsaKeyConverters.x509().convert(pub);
            }
        } else {
            log.warn("No JWT key configured; generating an ephemeral RSA key pair. Tokens will not survive a restart.");
            KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
            generator.initialize(2048);
            KeyPair pair = generator.generateKeyPair();
            privateKey = (RSAPrivateKey) pair.getPrivate();
            publicKey = (RSAPublicKey) pair.getPublic();
        }
        return new RSAKey.Builder(publicKey).privateKey(privateKey).keyID(props.keyId()).build();
    }

    @Bean
    JwtEncoder jwtEncoder(RSAKey rsaKey) {
        return new NimbusJwtEncoder(new ImmutableJWKSet<>(new JWKSet(rsaKey)));
    }

    /** Auth validates its own tokens locally (for /auth/me) with the same public key it publishes. */
    @Bean
    JwtDecoder jwtDecoder(RSAKey rsaKey, JwtProperties props) throws JOSEException {
        NimbusJwtDecoder decoder = NimbusJwtDecoder.withPublicKey(rsaKey.toRSAPublicKey()).build();
        decoder.setJwtValidator(JwtValidators.createDefaultWithIssuer(props.issuer()));
        return decoder;
    }

    private static InputStream stream(String pem) {
        return new ByteArrayInputStream(pem.replace("\\n", "\n").getBytes(StandardCharsets.UTF_8));
    }
}
