-- The columns Spring AI's PgVectorStore expects. Flyway owns the schema (initialize-schema: false).
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS hstore;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS vector_store (
    id        UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    content   TEXT,
    metadata  JSON,
    -- must equal the embedding model's dimensions: 1536 for text-embedding-3-small, 1024 for Titan V2
    embedding VECTOR(${embedding_dimensions})
);

CREATE INDEX IF NOT EXISTS vector_store_hnsw ON vector_store USING hnsw (embedding vector_cosine_ops);

-- Owner-scoped search and per-document deletes filter on these keys.
CREATE INDEX IF NOT EXISTS vector_store_owner ON vector_store ((metadata ->> 'owner_id'));
CREATE INDEX IF NOT EXISTS vector_store_document ON vector_store ((metadata ->> 'document_id'));
