package com.documind.auth.service;

/** One message for unknown email and wrong password, so callers cannot enumerate accounts. */
public class InvalidCredentialsException extends RuntimeException {

    public InvalidCredentialsException() {
        super("Invalid credentials");
    }
}
