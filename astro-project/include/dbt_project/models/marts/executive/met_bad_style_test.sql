-- This model intentionally violates SQLFluff rules to test CI
-- Violations: lowercase keywords, bad indentation, lowercase functions
select
  pickup_borough,
  count(*) as trip_count,
        avg(fare_amount) as avg_fare,
sum(total_amount)as total_rev
from {{ ref('fct_taxi_trips') }}
where taxi_type = 'yellow'
group by pickup_borough
order by trip_count desc
