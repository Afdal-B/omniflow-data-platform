select
    runs.pipeline_name,
    runs.batch_id,
    cast(runs.started_at as date)               as run_date,
    kv.json_key                                 as rejection_reason,
    cast(kv.json_value as {{ dbt.type_int() }}) as rejected_rows
from {{ ref('stg_ops__pipeline_runs') }} as runs
{{ unnest_json_object('runs', 'rejection_reasons') }}
