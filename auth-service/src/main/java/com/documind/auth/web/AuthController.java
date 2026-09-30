package com.documind.auth.web;

import com.documind.auth.service.AuthService;
import com.documind.auth.service.TokenService;
import com.documind.auth.user.User;
import com.documind.auth.web.AuthDtos.LoginRequest;
import com.documind.auth.web.AuthDtos.RegisterRequest;
import com.documind.auth.web.AuthDtos.TokenResponse;
import com.documind.auth.web.AuthDtos.UserResponse;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/auth")
@RequiredArgsConstructor
public class AuthController {

    private final AuthService authService;

    @PostMapping("/register")
    @ResponseStatus(HttpStatus.CREATED)
    public UserResponse register(@Valid @RequestBody RegisterRequest request) {
        User user = authService.register(request.email(), request.password(), request.fullName());
        return new UserResponse(user.getId(), user.getEmail(), user.getFullName(), List.of(user.getRole()));
    }

    @PostMapping("/login")
    public TokenResponse login(@Valid @RequestBody LoginRequest request) {
        TokenService.IssuedToken token = authService.login(request.email(), request.password());
        return new TokenResponse(token.value(), "Bearer", token.expiresInSeconds());
    }

    @GetMapping("/me")
    public UserResponse me(@AuthenticationPrincipal Jwt jwt) {
        return new UserResponse(UUID.fromString(jwt.getSubject()), jwt.getClaimAsString("email"),
                jwt.getClaimAsString("name"), jwt.getClaimAsStringList("roles"));
    }
}
