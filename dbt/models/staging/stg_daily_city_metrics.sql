with src as (
    select * from {{ source('raw', 'silver_daily_city_metrics') }}
)

select
    cast(event_date as date)        as metric_date,
    city,
    cast(total_rides as integer)    as total_rides,
    cast(avg_duration_min as float) as avg_duration_min,
    cast(revenue_pkr as float)      as revenue_pkr,
    cast(paid_rides as integer)     as paid_rides
from src
