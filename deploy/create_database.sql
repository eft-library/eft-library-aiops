-- Run once as PostgreSQL administrator, using psql (not the application).
-- Existing role/database: review and skip their CREATE statements rather than recreating them.
\set ON_ERROR_STOP on
CREATE ROLE aiops_app LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE;
SET password_encryption = 'scram-sha-256';
\password aiops_app
CREATE DATABASE aiops OWNER aiops_app;
