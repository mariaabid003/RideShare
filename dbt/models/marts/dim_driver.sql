-- Driver dimension (from seed reference data).
select
    driver_id,
    driver_name,
    cast(rating as float)   as rating,
    home_zone,
    primary_vehicle,
    cast(joined_date as date) as joined_date
from {{ ref('seed_drivers') }}
