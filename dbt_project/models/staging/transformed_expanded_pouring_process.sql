{{ config(
    materialized='table'
) }}

with pouring_data as (
    select * from {{ source('snow_gcp', 'EXPANDED_POURING_PROCESS') }}
),

transformed as (
    select
        pouring_id,
        assembly_id,
        ladle_id,
        melt_id,
        process,
        min(arrival_time) as first_arrival_time,
        max(departure_time) as last_departure_time,
        sum(estimated_time) as total_estimated_time,
        timestamp_diff(max(departure_time), min(arrival_time), minute) as total_duration_minutes
    from pouring_data
    group by pouring_id, assembly_id, ladle_id, melt_id, process
)

select * from transformed
