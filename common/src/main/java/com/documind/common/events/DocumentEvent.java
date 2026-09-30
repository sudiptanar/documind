package com.documind.common.events;

import java.util.UUID;

/** Fields every document lifecycle event carries, so consumers can route without knowing the concrete type. */
public interface DocumentEvent {

    UUID documentId();

    UUID ownerId();

    String ownerEmail();

    String fileName();
}
