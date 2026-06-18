-- Gold fact: one row per completed ride, joined to driver & rider dimensions.
with rides as (
    select * from {{ ref('stg_rides') }}
),
drivers as (
    select * from {{ ref('dim_driver') }}
),
riders as (
    select * from {{ ref('dim_rider') }}
)

select
    r.ride_id,
    r.ride_date,
    r.city,
    r.vehicle_type,
    -- rider attributes
    r.rider_id,
    ri.rider_name,
    ri.segment            as rider_segment,
    -- driver attributes
    r.driver_id,
    d.driver_name,
    d.rating              as driver_rating,
    d.home_zone           as driver_home_zone,
    -- ride measures
    r.duration_min,
    r.distance_km,
    r.fare_amount,
    coalesce(r.paid_amount, r.fare_amount) as revenue,
    r.payment_method
from rides r
left join drivers d on r.driver_id = d.driver_id
left join riders  ri on r.rider_id = ri.rider_id
