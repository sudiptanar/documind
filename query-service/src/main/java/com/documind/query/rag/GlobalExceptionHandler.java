package com.documind.query.rag;

import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.retry.NonTransientAiException;
import org.springframework.ai.retry.TransientAiException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler extends ResponseEntityExceptionHandler {

    /** Rate limits / provider outages that survived Spring AI's retries. */
    @ExceptionHandler(TransientAiException.class)
    ProblemDetail busy(TransientAiException ex) {
        log.warn("LLM provider unavailable: {}", ex.getMessage());
        return ProblemDetail.forStatusAndDetail(HttpStatus.SERVICE_UNAVAILABLE,
                "The AI service is busy right now. Please try again in a moment.");
    }

    @ExceptionHandler(NonTransientAiException.class)
    ProblemDetail providerError(NonTransientAiException ex) {
        log.error("LLM provider rejected the request: {}", ex.getMessage());
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_GATEWAY, "The AI service could not process this request.");
    }
}
