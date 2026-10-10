-- Observability tables (design doc section 21)

CREATE TABLE IF NOT EXISTS audit.pipeline_run (
  pipeline_run_id      text PRIMARY KEY,          -- {dag_id}__{task_id}__{run_id}__try{n}
  dag_id               text        NOT NULL,
  task_id              text        NOT NULL,
  source_system        text,
  data_interval_start  timestamptz NOT NULL,
  data_interval_end    timestamptz NOT NULL,
  started_at           timestamptz NOT NULL,
  ended_at             timestamptz,
  status               text        NOT NULL CHECK (status IN ('running', 'success', 'failed', 'skipped')),
  http_status          int,
  api_calls            int,
  bytes_in             bigint,
  records_in           bigint,
  records_out          bigint,
  records_quarantined  bigint,
  records_duplicate    bigint,
  raw_prefix           text,
  error_message        text
);

CREATE INDEX IF NOT EXISTS pipeline_run_source_interval_idx
  ON audit.pipeline_run (source_system, data_interval_start);

CREATE TABLE IF NOT EXISTS audit.dq_result (
  dq_result_id     bigserial PRIMARY KEY,
  pipeline_run_id  text REFERENCES audit.pipeline_run (pipeline_run_id) ON DELETE CASCADE,
  rule_id          text        NOT NULL,
  dataset          text        NOT NULL,
  severity         text        NOT NULL CHECK (severity IN ('FAIL_RUN', 'QUARANTINE', 'WARN')),
  records_checked  bigint,
  records_failed   bigint,
  passed           boolean     NOT NULL,
  message          text,
  checked_at       timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS dq_result_rule_idx ON audit.dq_result (rule_id, checked_at);

-- Hours that can never be recovered (e.g. GISTDA missed for > 24h). Shown on the dashboard,
-- never filled with invented values.
CREATE TABLE IF NOT EXISTS audit.data_gap (
  source_system  text        NOT NULL,
  dataset        text        NOT NULL,
  gap_start      timestamptz NOT NULL,
  gap_end        timestamptz NOT NULL,
  reason         text,
  detected_at    timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (source_system, dataset, gap_start)
);

-- Freshness per source, for the Pipeline Health page
CREATE OR REPLACE VIEW audit.v_source_freshness AS
SELECT source_system,
       max(ended_at) FILTER (WHERE status = 'success')                       AS last_success_at,
       round(extract(epoch FROM now() - max(ended_at) FILTER (WHERE status = 'success')) / 3600, 1)
                                                                              AS hours_since_success,
       count(*) FILTER (WHERE status = 'failed' AND started_at > now() - interval '7 days') AS failures_7d,
       sum(records_out) FILTER (WHERE status = 'success' AND started_at > now() - interval '7 days')
                                                                              AS records_out_7d
FROM audit.pipeline_run
GROUP BY source_system;
