/* @bruin
name: reports.trips_report
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
    checks:
      - name: not_null
  - name: payment_type_name
    type: string
    primary_key: true
  - name: trip_count
    type: bigint
    checks:
      - name: positive
  - name: total_passengers
    type: bigint
    checks:
      - name: non_negative
  - name: total_fare
    type: float
    checks:
      - name: non_negative
  - name: total_tip
    type: float
    checks:
      - name: non_negative
  - name: total_tolls
    type: float
    checks:
      - name: non_negative
  - name: total_revenue
    type: float
    checks:
      - name: non_negative
  - name: avg_fare
    type: float
  - name: avg_tip
    type: float
  - name: avg_distance
    type: float
  - name: avg_duration_minutes
    type: float
  - name: avg_passengers
    type: float

custom_checks:
  - name: avg_fare_reasonable
    query: |
      SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END
      FROM reports.trips_report
      WHERE trip_count > 10
        AND payment_type_name NOT IN ('dispute', 'unknown', 'voided_trip')
        AND (avg_fare > 500 OR avg_fare < 0)
    value: 1
@bruin */

SELECT
    CAST(pickup_datetime AS DATE) AS trip_date,
    taxi_type,
    payment_type_name,
    COUNT(*) AS trip_count,
    SUM(passenger_count) AS total_passengers,
    SUM(fare_amount) AS total_fare,
    SUM(tip_amount) AS total_tip,
    SUM(tolls_amount) AS total_tolls,
    SUM(total_amount) AS total_revenue,
    ROUND(AVG(fare_amount), 2) AS avg_fare,
    ROUND(AVG(tip_amount), 2) AS avg_tip,
    ROUND(AVG(trip_distance), 2) AS avg_distance,
    ROUND(AVG(trip_duration_minutes), 2) AS avg_duration_minutes,
    ROUND(AVG(passenger_count), 2) AS avg_passengers
FROM staging.trips
WHERE pickup_datetime >= '{{ start_datetime }}'
  AND pickup_datetime < '{{ end_datetime }}'
GROUP BY 1, 2, 3
