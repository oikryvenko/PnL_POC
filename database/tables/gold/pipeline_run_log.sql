-- Metric definitions (set by each task)
CREATE TABLE IF NOT EXISTS cibc.gold.pipeline_run_log (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    run_id STRING,
    task_run_id STRING,
    task_name STRING,
    run_date DATE,
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    status STRING,
    source_table STRING,
    target_table STRING,
    requested_tickers BIGINT,
    input_rows BIGINT,
    output_rows BIGINT,
    rejected_rows BIGINT,
    duplicate_rows BIGINT,
    issue_counts MAP<STRING, BIGINT>,
    error_type STRING,
    error_message STRING
)
USING DELTA;
