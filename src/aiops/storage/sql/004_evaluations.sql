BEGIN;
SET LOCAL lock_timeout = '5s';
CREATE TABLE IF NOT EXISTS public.aiops_finding_evaluations (
    evaluation_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    report_id text NOT NULL REFERENCES public.aiops_report_versions(report_id),
    finding_key text NOT NULL CHECK (length(finding_key) BETWEEN 1 AND 200),
    revision integer NOT NULL CHECK (revision > 0),
    finding_snapshot jsonb NOT NULL CHECK (jsonb_typeof(finding_snapshot) = 'object'),
    verdict text NOT NULL CHECK (verdict IN ('correct','partial','incorrect','unconfirmed')),
    actual_result text NOT NULL DEFAULT '' CHECK (length(actual_result) <= 4000),
    evidence text NOT NULL DEFAULT '' CHECK (length(evidence) <= 4000),
    usefulness text CHECK (usefulness IN ('helpful','neutral','unhelpful')),
    reviewer text NOT NULL CHECK (length(reviewer) BETWEEN 1 AND 128),
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (report_id, finding_key, revision)
);
CREATE INDEX IF NOT EXISTS aiops_finding_evaluations_latest
    ON public.aiops_finding_evaluations(report_id, finding_key, revision DESC);
INSERT INTO public.aiops_schema_migrations(version) VALUES (4) ON CONFLICT DO NOTHING;
COMMIT;
