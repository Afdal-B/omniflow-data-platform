select
    pipeline_name,
    batch_id,
    cast(started_at as date)        as run_date,
    started_at,
    finished_at,
    run_status,
    run_status = 'success'          as is_success,
    ingest_status,
    load_status,
    extraction_mode,
    duration_seconds,
    rows_read,
    rows_valid,
    rows_rejected,
    dlq_rate,
    rows_loaded,
    duration_seconds / 3600.0 * (
        {{ var('finops_vcpu_per_job') }} * {{ var('finops_usd_per_vcpu_hour') }}
        + {{ var('finops_memory_gb_per_job') }} * {{ var('finops_usd_per_gb_hour') }}
    )                               as estimated_cost_usd,
    error_message
from {{ ref('stg_ops__pipeline_runs') }}
