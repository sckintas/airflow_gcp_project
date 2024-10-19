{{ config(
    materialized='table'
) }}

with flask_data as (
    select * from {{ source('snow_gcp', 'EXPANDED_FLASK_PREPARATION') }}
),

transformed as (
    select
        flask_id,
        stage,
        process,
        array_agg(distinct chemicals_used) as chemicals_used_list,
        min(arrival_time) as first_arrival_time,
        max(departure_time) as last_departure_time,
        sum(estimated_time) as total_estimated_time,
        timestamp_diff(max(departure_time), min(arrival_time), minute) as total_duration_minutes
    from flask_data
    group by flask_id, stage, process
)

select * from transformed
