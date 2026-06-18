-- Clean, typed view over the raw ride-grain table.
with src as (
    select * from {{ source('raw', 'rides') }}
)

select
    ride_id,
    rider_id,
    driver_id,
    lower(vehicle_type)                as vehicle_type,
    city,
    cast(event_date as date)           as ride_date,
    cast(ride_duration_min as float)   as duration_min,
    cast(distance_km as float)         as distance_km,
    cast(fare_amount as float)         as fare_amount,
    cast(paid_amount as float)         as paid_amount,
    payment_method
from src
where ride_id is not null
