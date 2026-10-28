-- Destructive: stop Worker/producers, back up aiops schema, and review before manual application.
-- Only this migration's objects are removed. No CASCADE; foreign references abort the transaction.
BEGIN;
SET LOCAL lock_timeout = '5s';
DROP TABLE public.aiops_job_attempts;
DROP TABLE public.aiops_analysis_jobs;
DROP TABLE public.aiops_report_dependencies;
DROP TABLE public.aiops_report_versions;
DROP TABLE public.aiops_reports;
DROP TABLE public.aiops_batches;
DELETE FROM public.aiops_schema_migrations WHERE version IN (1,2);
-- Keep schema and migration ledger for explicit administrator review.
COMMIT;
