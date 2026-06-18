-- Gold: hourly ride demand per city with the peak hour flagged.
select
    demand_date,
    city,
    hour_of_day,
    ride_requests,
    is_peak_hour
from {{ ref('stg_hourly_demand') }}
