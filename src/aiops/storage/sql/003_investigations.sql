BEGIN;
SET LOCAL lock_timeout = '5s';
CREATE TABLE IF NOT EXISTS public.aiops_investigations (
    investigation_id uuid PRIMARY KEY,
    report_id text NOT NULL REFERENCES public.aiops_report_versions,
    request_key text NOT NULL CHECK (length(request_key) BETWEEN 1 AND 128),
    status text NOT NULL CHECK (status IN ('PENDING','RUNNING','COMPLETED','FAILED','TIMEOUT')),
    requested_at timestamptz NOT NULL DEFAULT now(),
    available_at timestamptz NOT NULL DEFAULT now(),
    started_at timestamptz, finished_at timestamptz,
    attempt_count integer NOT NULL DEFAULT 0,
    max_attempts integer NOT NULL DEFAULT 3 CHECK (max_attempts BETWEEN 1 AND 3),
    worker_id uuid,
    model text,
    policy_hash text NOT NULL,
    input_json jsonb,
    result_json jsonb,
    error text,
    UNIQUE(report_id, request_key),
    CHECK (status <> 'COMPLETED' OR result_json IS NOT NULL)
);
CREATE INDEX IF NOT EXISTS aiops_investigations_pending ON public.aiops_investigations(available_at, requested_at)
    WHERE status='PENDING';
CREATE TABLE IF NOT EXISTS public.aiops_investigation_attempts (
    investigation_id uuid NOT NULL REFERENCES public.aiops_investigations,
    attempt integer NOT NULL,
    started_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    input_json jsonb,
    result_json jsonb,
    error text,
    PRIMARY KEY(investigation_id, attempt)
);
INSERT INTO public.aiops_schema_migrations(version) VALUES (3) ON CONFLICT DO NOTHING;
COMMIT;
