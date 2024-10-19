{{ config(
    materialized='table'
) }}

with transport_data as (
    select * from {{ source('snow_gcp', 'EXPANDED_TRANSPORT_TRACKING') }}
),

transformed as (
    select
        equipment_id,
        equipment_type,
        source_stage,
        destination_stage,
        transport_mechanism,
        min(transport_start_time) as transport_start_time,
        max(transport_end_time) as transport_end_time,
        timestamp_diff(max(transport_end_time), min(transport_start_time), minute) as total_transport_duration_minutes
    from transport_data
    group by equipment_id, equipment_type, source_stage, destination_stage, transport_mechanism
)

select * from transformed
