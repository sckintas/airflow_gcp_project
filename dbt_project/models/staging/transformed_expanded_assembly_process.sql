{{ config(
    materialized='table'
) }}

with assembly_data as (
    select * from {{ source('snow_gcp', 'EXPANDED_ASSEMBLY_PROCESS') }}
),

transformed as (
    select
        assembly_id,
        flask_id,
        core_id,
        stage,
        process,
        min(arrival_time) as first_arrival_time,
        max(departure_time) as last_departure_time,
        sum(estimated_time) as total_estimated_time,
        timestamp_diff(max(departure_time), min(arrival_time), minute) as total_duration_minutes
    from assembly_data
    group by assembly_id, flask_id, core_id, stage, process
)

select * from transformed
