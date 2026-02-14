/* @bruin
name: staging.trips
type: bq.sql

depends:
  - ingestion.trips
  - ingestion.payment_lookup
  - ingestion.zone_lookup
  - ingestion.rate_code_lookup

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: timestamp

columns:
  - name: pickup_datetime
    type: timestamp
    primary_key: true
    checks:
      - name: not_null
  - name: dropoff_datetime
    type: timestamp
    checks:
      - name: not_null
  - name: pu_location_id
    type: integer
    primary_key: true
    checks:
      - name: not_null
  - name: do_location_id
    type: integer
    primary_key: true
    checks:
      - name: not_null
  - name: fare_amount
    type: float
    primary_key: true
    checks:
      - name: not_null
  - name: taxi_type
    type: string
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - yellow
          - green
  - name: passenger_count
    type: integer
    checks:
      - name: non_negative
  - name: trip_distance
    type: float
    checks:
      - name: non_negative
  - name: payment_type_name
    type: string
  - name: rate_code_name
    type: string
  - name: tip_amount
    type: float
    checks:
      - name: non_negative
  - name: tolls_amount
    type: float
    checks:
      - name: non_negative
  - name: total_amount
    type: float
    checks:
      - name: not_null
  - name: congestion_surcharge
    type: float
  - name: pickup_borough
    type: string
  - name: pickup_zone
    type: string
  - name: dropoff_borough
    type: string
  - name: dropoff_zone
    type: string
  - name: trip_duration_minutes
    type: float
    checks:
      - name: non_negative

custom_checks:
  - name: row_count_greater_than_zero
    query: |
      SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END
      FROM staging.trips
    value: 1
  - name: dropoff_after_pickup
    query: |
      SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END
      FROM staging.trips
      WHERE dropoff_datetime < pickup_datetime
    value: 1
@bruin */

SELECT
    t.pickup_datetime,
    t.dropoff_datetime,
    t.taxi_type,
    t.passenger_count,
    t.trip_distance,
    t.fare_amount,
    t.tip_amount,
    t.tolls_amount,
    t.total_amount,
    t.pu_location_id,
    t.do_location_id,
    t.rate_code_id,
    t.congestion_surcharge,
    t.vendor_id,
    p.payment_type_name,
    r.rate_code_name,
    puz.borough AS pickup_borough,
    puz.zone AS pickup_zone,
    puz.service_zone AS pickup_service_zone,
    doz.borough AS dropoff_borough,
    doz.zone AS dropoff_zone,
    doz.service_zone AS dropoff_service_zone,
    ROUND(TIMESTAMP_DIFF(t.dropoff_datetime, t.pickup_datetime, SECOND) / 60.0, 2) AS trip_duration_minutes
FROM ingestion.trips t
LEFT JOIN ingestion.payment_lookup p
    ON t.payment_type = p.payment_type_id
LEFT JOIN ingestion.rate_code_lookup r
    ON t.rate_code_id = r.rate_code_id
LEFT JOIN ingestion.zone_lookup puz
    ON t.pu_location_id = puz.location_id
LEFT JOIN ingestion.zone_lookup doz
    ON t.do_location_id = doz.location_id
WHERE t.pickup_datetime >= '{{ start_datetime }}'
  AND t.pickup_datetime < '{{ end_datetime }}'
  AND t.fare_amount >= 0
  AND t.trip_distance >= 0
  AND t.total_amount >= 0
  AND t.dropoff_datetime >= t.pickup_datetime
  AND (t.tip_amount IS NULL OR t.tip_amount >= 0)
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY t.pickup_datetime, t.dropoff_datetime, t.pu_location_id, t.do_location_id, t.fare_amount
    ORDER BY t.extracted_at DESC
) = 1
