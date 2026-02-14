/* @bruin
name: reports.rate_code_report
type: bq.sql

depends:
  - staging.trips

materialization:
  type: table
  strategy: time_interval
  incremental_key: trip_date
  time_granularity: date

columns:
  - name: trip_date
    type: date
    primary_key: true
    checks:
      - name: not_null
  - name: taxi_type
    type: string
    primary_key: true
  - name: rate_code_name
    type: string
    primary_key: true
  - name: trip_count
    type: bigint
    checks:
      - name: positive
  - name: avg_fare
    type: float
  - name: avg_distance
    type: float
  - name: avg_tip
    type: float
  - name: total_revenue
    type: float
    checks:
      - name: non_negative
@bruin */

SELECT
    CAST(pickup_datetime AS DATE) AS trip_date,
    taxi_type,
    COALESCE(rate_code_name, 'unknown') AS rate_code_name,
    COUNT(*) AS trip_count,
    ROUND(AVG(fare_amount), 2) AS avg_fare,
    ROUND(AVG(trip_distance), 2) AS avg_distance,
    ROUND(AVG(tip_amount), 2) AS avg_tip,
    SUM(total_amount) AS total_revenue
FROM staging.trips
WHERE pickup_datetime >= '{{ start_datetime }}'
  AND pickup_datetime < '{{ end_datetime }}'
GROUP BY 1, 2, 3
