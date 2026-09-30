package com.documind.ingestion.worker;

/** The PDF itself is the problem (corrupt, encrypted, image-only). Retrying cannot help. */
public class InvalidPdfException extends RuntimeException {

    public InvalidPdfException(String message) {
        super(message);
    }

    public InvalidPdfException(String message, Throwable cause) {
        super(message, cause);
    }
}
