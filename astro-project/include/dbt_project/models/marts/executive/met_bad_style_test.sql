-- Intentionally bad SQL to test Slack CI failure notification
select
  pickup_borough,
  count(*) as trip_count,
        avg(fare_amount) as avg_fare
from {{ ref('fct_taxi_trips') }}
where taxi_type = 'yellow'
group by pickup_borough
