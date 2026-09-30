package com.documind.ingestion.document;

import java.util.UUID;

/** Also thrown for another user's document: a 404, not a 403, reveals nothing about its existence. */
public class DocumentNotFoundException extends RuntimeException {

    public DocumentNotFoundException(UUID id) {
        super("Document " + id + " not found");
    }
}
