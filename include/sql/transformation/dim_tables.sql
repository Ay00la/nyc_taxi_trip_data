-- dim_vendor
INSERT INTO dwh.dim_vendor (vendor_id, vendor_name)
VALUES 
    (1, 'Creative Mobile Technologies'),
    (2, 'VeriFone Inc'),
    (-1, 'Unknown Vendor')
ON CONFLICT (vendor_id) DO NOTHING;

-- dim_taxi_type
INSERT INTO dwh.dim_taxi_type (taxi_type_id, taxi_type_name, taxi_type_description)
VALUES 
    (1, 'Yellow', 'Yellow taxi cabs - can pick up passengers anywhere in NYC'),
    (2, 'Green', 'Green taxi cabs - can pick up passengers in outer boroughs and upper Manhattan'),
    (-1, 'Unknown', 'Unknown taxi type')
ON CONFLICT (taxi_type_id) DO NOTHING;

-- dim_rate_code
INSERT INTO dwh.dim_rate_code (rate_code_id, rate_code_name)
VALUES 
    (1, 'Standard rate'),
    (2, 'JFK'),
    (3, 'Newark'),
    (4, 'Nassau or Westchester'),
    (5, 'Negotiated fare'),
    (6, 'Group ride'),
    (-1, 'Unknown Rate Code')
ON CONFLICT (rate_code_id) DO NOTHING;

INSERT INTO dwh.dim_rate_code (rate_code_id, rate_code_name)
SELECT DISTINCT s.RatecodeID::INTEGER, 'Unknown Rate Code'
FROM stg.yellow_tripdata s
WHERE s.RatecodeID IS NOT NULL
  AND s.RatecodeID::INTEGER NOT IN (SELECT rate_code_id FROM dwh.dim_rate_code);

-- dim_payment_type
INSERT INTO dwh.dim_payment_type (payment_type_id, payment_type)
VALUES 
    (1, 'Credit card'),
    (2, 'Cash'),
    (3, 'No charge'),
    (4, 'Dispute'),
    (5, 'Unknown'),
    (6, 'Voided trip'),
    (-1, 'Unknown Payment Type')
ON CONFLICT (payment_type_id) DO NOTHING;

-- dim_location from taxi_zone_lookup
INSERT INTO dwh.dim_location (location_id, borough, zone, service_zone)
SELECT 
    LocationID as location_id,
    Borough as borough,
    Zone as zone,
    service_zone
FROM stg.taxi_zone_lookup
UNION ALL
SELECT -1, 'Unknown', 'Unknown', 'Unknown'
ON CONFLICT (location_id) DO NOTHING;

-- Insert unique locations from yellow_tripdata into dim_location
INSERT INTO dwh.dim_location (location_id)
SELECT DISTINCT PULocationID
FROM stg.yellow_tripdata
WHERE PULocationID IS NOT NULL
  AND PULocationID NOT IN (SELECT location_id FROM dwh.dim_location)

UNION

SELECT DISTINCT DOLocationID
FROM stg.yellow_tripdata
WHERE DOLocationID IS NOT NULL
  AND DOLocationID NOT IN (SELECT location_id FROM dwh.dim_location);

-- dim_date (for date range that covers your data)
INSERT INTO dwh.dim_date (date_id, date, year, month, day, day_of_week, week_of_year, is_weekend)
SELECT 
    TO_CHAR(date_series, 'YYYYMMDD')::INTEGER as date_id,
    date_series::DATE as date,
    EXTRACT(YEAR FROM date_series)::INTEGER as year,
    EXTRACT(MONTH FROM date_series)::INTEGER as month,
    EXTRACT(DAY FROM date_series)::INTEGER as day,
    TO_CHAR(date_series, 'Day') as day_of_week,
    EXTRACT(WEEK FROM date_series)::INTEGER as week_of_year,
    CASE WHEN EXTRACT(DOW FROM date_series) IN (0, 6) THEN TRUE ELSE FALSE END as is_weekend
FROM generate_series('2020-01-01'::DATE, '2030-12-31'::DATE, '1 day'::INTERVAL) as date_series
ON CONFLICT (date_id) DO NOTHING;


