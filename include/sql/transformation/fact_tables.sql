INSERT INTO dwh.all_taxi_trips (
    vendor_id,
    taxi_type_id,
    pickup_datetime,
    dropoff_datetime,
    passenger_count,
    trip_distance,
    pickup_location_id,
    dropoff_location_id,
    payment_type,
    fare_amount,
    extra,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee,
    rate_code_id,
    store_and_fwd_flag,
    mta_tax,
    trip_type,
    ehail_fee
)
SELECT 
    COALESCE(s.VendorID, -1) as vendor_id,
    1 as taxi_type_id, -- Yellow taxi
    s.tpep_pickup_datetime as pickup_datetime,
    s.tpep_dropoff_datetime as dropoff_datetime,
    s.passenger_count,
    s.trip_distance,
    COALESCE(s.PULocationID, -1) as pickup_location_id,
    COALESCE(s.DOLocationID, -1) as dropoff_location_id,
    COALESCE(s.payment_type, -1) as payment_type,
    s.fare_amount,
    s.extra,
    s.tip_amount,
    s.tolls_amount,
    s.improvement_surcharge,
    s.total_amount,
    s.congestion_surcharge,
    s.airport_fee,
    COALESCE(s.RatecodeID::INTEGER, -1) as rate_code_id,
    CASE WHEN s.store_and_fwd_flag = 'Y' THEN TRUE ELSE FALSE END as store_and_fwd_flag,
    s.mta_tax,
    NULL as trip_type, -- Not applicable for yellow taxis
    NULL as ehail_fee  -- Not applicable for yellow taxis
FROM stg.yellow_tripdata s
WHERE s.tpep_pickup_datetime IS NOT NULL 
  AND s.tpep_dropoff_datetime IS NOT NULL
  AND s.tpep_pickup_datetime < s.tpep_dropoff_datetime
  AND s.trip_distance >= 0
  AND s.total_amount >= 0;

-- Insert Green Taxi Data
INSERT INTO dwh.all_taxi_trips (
    vendor_id,
    taxi_type_id,
    pickup_datetime,
    dropoff_datetime,
    passenger_count,
    trip_distance,
    pickup_location_id,
    dropoff_location_id,
    payment_type,
    fare_amount,
    extra,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee,
    rate_code_id,
    store_and_fwd_flag,
    mta_tax,
    trip_type,
    ehail_fee
)
SELECT 
    COALESCE(s.VendorID, -1) as vendor_id,
    2 as taxi_type_id, -- Green taxi
    s.lpep_pickup_datetime as pickup_datetime,
    s.lpep_dropoff_datetime as dropoff_datetime,
    s.passenger_count,
    s.trip_distance,
    COALESCE(s.PULocationID, -1) as pickup_location_id,
    COALESCE(s.DOLocationID, -1) as dropoff_location_id,
    COALESCE(s.payment_type, -1) as payment_type,
    s.fare_amount,
    s.extra,
    s.tip_amount,
    s.tolls_amount,
    s.improvement_surcharge,
    s.total_amount,
    s.congestion_surcharge,
    s.airport_fee,
    COALESCE(s.RatecodeID::INTEGER, -1) as rate_code_id,
    CASE WHEN s.store_and_fwd_flag = 'Y' THEN TRUE ELSE FALSE END as store_and_fwd_flag,
    s.mta_tax,
    s.trip_type,
    s.ehail_fee
FROM stg.green_tripdata s
WHERE s.lpep_pickup_datetime IS NOT NULL 
  AND s.lpep_dropoff_datetime IS NOT NULL
  AND s.lpep_pickup_datetime < s.lpep_dropoff_datetime
  AND s.trip_distance >= 0
  AND s.total_amount >= 0;