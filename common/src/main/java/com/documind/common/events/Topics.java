package com.documind.common.events;

public final class Topics {

    public static final String DOCUMENT_UPLOADED = "document.uploaded";
    public static final String DOCUMENT_INDEXED = "document.indexed";
    public static final String DOCUMENT_FAILED = "document.failed";
    public static final String DOCUMENT_UPLOADED_DLT = DOCUMENT_UPLOADED + ".DLT";

    private Topics() {
    }
}
