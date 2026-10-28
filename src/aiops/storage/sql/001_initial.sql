-- Review and apply explicitly to the selected AIOps database. No application tables are altered.
BEGIN;
SET LOCAL lock_timeout = '5s';
-- public is the application schema; table names are scoped with aiops_.
CREATE TABLE IF NOT EXISTS public.aiops_schema_migrations (
    version integer PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS public.aiops_batches (
    batch_id text PRIMARY KEY,
    collected_at timestamptz NOT NULL,
    period_start timestamptz NOT NULL,
    period_end timestamptz NOT NULL,
    relative_path text NOT NULL,
    coverage jsonb NOT NULL,
    totals jsonb NOT NULL,
    masked boolean NOT NULL CHECK (masked)
);
CREATE TABLE IF NOT EXISTS public.aiops_reports (
    period_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    kind text NOT NULL CHECK (kind IN ('daily','weekly','monthly')),
    period_start date NOT NULL,
    period_end date NOT NULL CHECK (period_end > period_start),
    timezone text NOT NULL DEFAULT 'Asia/Seoul' CHECK (timezone = 'Asia/Seoul'),
    UNIQUE(kind, period_start)
);
CREATE TABLE IF NOT EXISTS public.aiops_report_versions (
    report_id text PRIMARY KEY,
    period_id bigint NOT NULL REFERENCES public.aiops_reports,
    revision integer NOT NULL CHECK (revision > 0),
    fingerprint text NOT NULL CHECK (fingerprint ~ '^[a-f0-9]{64}$'),
    status text NOT NULL CHECK (status IN ('PREPARED','RUNNING','COMPLETED','FAILED','CANCELLED')),
    created_at timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz,
    input_json jsonb NOT NULL,
    manifest_json jsonb NOT NULL,
    coverage_complete boolean NOT NULL,
    result_json jsonb,
    error text,
    UNIQUE(period_id, revision), UNIQUE(period_id, fingerprint),
    CHECK (status <> 'COMPLETED' OR result_json IS NOT NULL)
);
CREATE TABLE IF NOT EXISTS public.aiops_report_dependencies (
    report_id text NOT NULL REFERENCES public.aiops_report_versions,
    dependency_type text NOT NULL CHECK (dependency_type IN ('batch','daily','weekly_context')),
    dependency_id text NOT NULL,
    batch_id text REFERENCES public.aiops_batches DEFERRABLE INITIALLY DEFERRED,
    child_report_id text REFERENCES public.aiops_report_versions DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY(report_id, dependency_type, dependency_id),
    CHECK ((dependency_type='batch' AND batch_id IS NOT NULL AND batch_id=dependency_id AND child_report_id IS NULL)
        OR (dependency_type IN ('daily','weekly_context') AND child_report_id IS NOT NULL AND child_report_id=dependency_id AND batch_id IS NULL))
);
CREATE TABLE IF NOT EXISTS public.aiops_analysis_jobs (
    job_id uuid PRIMARY KEY,
    report_id text NOT NULL UNIQUE REFERENCES public.aiops_report_versions,
    priority integer NOT NULL DEFAULT 2 CHECK (priority BETWEEN 0 AND 3),
    status text NOT NULL CHECK (status IN ('PENDING','RUNNING','COMPLETED','FAILED','TIMEOUT','CANCELLED')),
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    max_attempts integer NOT NULL DEFAULT 3 CHECK (max_attempts BETWEEN 1 AND 100),
    available_at timestamptz NOT NULL DEFAULT now(),
    requested_at timestamptz NOT NULL DEFAULT now(),
    started_at timestamptz, finished_at timestamptz,
    worker_id uuid, error text
);
CREATE INDEX IF NOT EXISTS aiops_analysis_jobs_pending ON public.aiops_analysis_jobs(priority, available_at, requested_at)
    WHERE status='PENDING';
CREATE TABLE IF NOT EXISTS public.aiops_job_attempts (
    job_id uuid NOT NULL REFERENCES public.aiops_analysis_jobs,
    attempt integer NOT NULL,
    worker_id uuid NOT NULL,
    status text NOT NULL CHECK (status IN ('RUNNING','COMPLETED','FAILED','TIMEOUT','CANCELLED')),
    started_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz, error text,
    provider_job_id text, thread_id text,
    PRIMARY KEY(job_id, attempt)
);
INSERT INTO public.aiops_schema_migrations(version) VALUES (1),(2) ON CONFLICT DO NOTHING;
COMMIT;
