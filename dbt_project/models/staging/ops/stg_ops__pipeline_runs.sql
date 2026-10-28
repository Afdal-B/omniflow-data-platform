with source as (

    select * from {{ source('ops', 'pipeline_runs') }}

),

renamed as (

    select
        pipeline            as pipeline_name,
        batch_id,
        status              as run_status,
        ingest_status,
        load_status,
        started_at,
        finished_at,
        duration_s          as duration_seconds,
        mode                as extraction_mode,
        rows_read,
        rows_valid,
        rows_rejected,
        dlq_rate,
        rejection_reasons,
        rows_loaded,
        error               as error_message
    from source

)

select * from renamed
