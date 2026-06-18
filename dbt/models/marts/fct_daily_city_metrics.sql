-- Gold: clean daily metrics per city (passthrough of the Silver aggregates).
select
    metric_date,
    city,
    total_rides,
    avg_duration_min,
    revenue_pkr,
    paid_rides,
    case when total_rides > 0
         then round(revenue_pkr / total_rides, 2)
         else 0 end as revenue_per_ride
from {{ ref('stg_daily_city_metrics') }}
