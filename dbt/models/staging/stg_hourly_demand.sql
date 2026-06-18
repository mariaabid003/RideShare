with src as (
    select * from {{ source('raw', 'silver_hourly_demand') }}
)

select
    cast(event_date as date)      as demand_date,
    city,
    cast(hour as integer)         as hour_of_day,
    cast(ride_requests as integer) as ride_requests,
    cast(is_peak_hour as boolean) as is_peak_hour
from src
