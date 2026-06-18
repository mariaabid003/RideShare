-- Rider dimension (from seed reference data).
select
    rider_id,
    rider_name,
    segment,
    cast(signup_date as date) as signup_date
from {{ ref('seed_riders') }}
