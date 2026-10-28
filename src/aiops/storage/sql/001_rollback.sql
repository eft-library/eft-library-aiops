-- Destructive: stop Worker/producers, back up aiops schema, and review before manual application.
-- Only this migration's objects are removed. No CASCADE; foreign references abort the transaction.
BEGIN;
SET LOCAL lock_timeout = '5s';
DROP TABLE aiops.job_attempts;
DROP TABLE aiops.analysis_jobs;
DROP TABLE aiops.report_dependencies;
DROP TABLE aiops.report_versions;
DROP TABLE aiops.reports;
DROP TABLE aiops.batches;
DELETE FROM aiops.schema_migrations WHERE version=1;
-- Keep schema and migration ledger for explicit administrator review.
COMMIT;
