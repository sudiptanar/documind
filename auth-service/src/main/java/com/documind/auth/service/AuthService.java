package com.documind.auth.service;

import com.documind.auth.user.User;
import com.documind.auth.user.UserRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Locale;
import java.util.Optional;

@Service
@RequiredArgsConstructor
public class AuthService {

    private final UserRepository users;
    private final PasswordEncoder passwordEncoder;
    private final TokenService tokens;

    /** Compared against when the email is unknown, so both failure paths take the same BCrypt time. */
    private volatile String dummyHash;

    @Transactional
    public User register(String email, String password, String fullName) {
        String normalized = normalize(email);
        if (users.existsByEmail(normalized)) {
            throw new EmailAlreadyUsedException();
        }
        try {
            return users.saveAndFlush(User.create(normalized, passwordEncoder.encode(password), fullName));
        } catch (DataIntegrityViolationException race) {
            throw new EmailAlreadyUsedException();
        }
    }

    @Transactional(readOnly = true)
    public TokenService.IssuedToken login(String email, String password) {
        Optional<User> user = users.findByEmail(normalize(email));
        if (user.isEmpty()) {
            passwordEncoder.matches(password, dummyHash());
            throw new InvalidCredentialsException();
        }
        if (!passwordEncoder.matches(password, user.get().getPasswordHash())) {
            throw new InvalidCredentialsException();
        }
        return tokens.issue(user.get());
    }

    private String dummyHash() {
        if (dummyHash == null) {
            dummyHash = passwordEncoder.encode("timing-equaliser");
        }
        return dummyHash;
    }

    private static String normalize(String email) {
        return email.trim().toLowerCase(Locale.ROOT);
    }
}
