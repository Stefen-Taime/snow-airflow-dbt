-- Validates that met_daily_revenue totals align with met_executive_summary
-- within a 1% tolerance (rounding differences from GROUP BY granularity)
WITH daily AS (
    SELECT
        DATE_TRUNC('month', trip_date) AS trip_month,
        taxi_type,
        SUM(total_revenue) AS daily_total_revenue
    FROM {{ ref('met_daily_revenue') }}
    GROUP BY 1, 2
),

executive AS (
    SELECT
        trip_month,
        taxi_type,
        total_revenue AS exec_total_revenue
    FROM {{ ref('met_executive_summary') }}
)

SELECT
    d.trip_month,
    d.taxi_type,
    d.daily_total_revenue,
    e.exec_total_revenue,
    ABS(d.daily_total_revenue - e.exec_total_revenue) AS diff
FROM daily d
JOIN executive e
    ON d.trip_month = e.trip_month
    AND d.taxi_type = e.taxi_type
WHERE ABS(d.daily_total_revenue - e.exec_total_revenue) > ABS(e.exec_total_revenue) * 0.01
