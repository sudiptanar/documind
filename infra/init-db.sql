-- Runs once, when the Postgres volume is first created.
-- One instance, one database per service: the boundary lets you split them onto separate RDS instances later.

CREATE DATABASE auth_db;
CREATE DATABASE ingestion_db;      -- documents metadata + vector_store
CREATE DATABASE notification_db;   -- processed_events (idempotency)

-- Read-only role the query service uses to search vectors (a deliberate shared read model).
CREATE ROLE query_ro LOGIN PASSWORD 'query_ro';
GRANT CONNECT ON DATABASE ingestion_db TO query_ro;

\c ingestion_db
CREATE EXTENSION IF NOT EXISTS vector;
GRANT USAGE ON SCHEMA public TO query_ro;
-- Tables are created later by the ingestion service's Flyway migrations; grant SELECT on them in advance.
ALTER DEFAULT PRIVILEGES FOR ROLE documind IN SCHEMA public GRANT SELECT ON TABLES TO query_ro;
