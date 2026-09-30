CREATE TABLE documents (
    id           UUID PRIMARY KEY,
    owner_id     UUID          NOT NULL,
    file_name    VARCHAR(500)  NOT NULL,
    content_type VARCHAR(100)  NOT NULL,
    size_bytes   BIGINT        NOT NULL,
    s3_key       VARCHAR(1000) NOT NULL,
    status       VARCHAR(20)   NOT NULL,   -- UPLOADED | PROCESSING | INDEXED | FAILED
    chunk_count  INT,
    error        TEXT,
    created_at   TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE INDEX idx_documents_owner ON documents (owner_id, created_at DESC);
