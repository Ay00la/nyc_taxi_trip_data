DROP TABLE IF EXISTS stg.yellow_tripdata;
CREATE TABLE IF NOT EXISTS stg.yellow_tripdata (
    VendorID INTEGER,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count DOUBLE PRECISION,
    trip_distance DOUBLE PRECISION,
    RatecodeID DOUBLE PRECISION,
    store_and_fwd_flag TEXT,
    PULocationID INTEGER,
    DOLocationID INTEGER,
    payment_type INTEGER,
    fare_amount DOUBLE PRECISION,
    extra DOUBLE PRECISION,
    mta_tax DOUBLE PRECISION,
    tip_amount DOUBLE PRECISION,
    tolls_amount DOUBLE PRECISION,
    improvement_surcharge DOUBLE PRECISION,
    total_amount DOUBLE PRECISION,
    congestion_surcharge DOUBLE PRECISION,
    airport_fee DOUBLE PRECISION
);

DROP TABLE IF EXISTS stg.green_tripdata;
CREATE TABLE IF NOT EXISTS stg.green_tripdata (
    VendorID INTEGER,
    lpep_pickup_datetime TIMESTAMP,
    lpep_dropoff_datetime TIMESTAMP,
    store_and_fwd_flag TEXT,
    RatecodeID DOUBLE PRECISION,
    PULocationID INTEGER,
    DOLocationID INTEGER,
    passenger_count DOUBLE PRECISION,
    trip_distance DOUBLE PRECISION,
    fare_amount DOUBLE PRECISION,
    extra DOUBLE PRECISION,
    mta_tax DOUBLE PRECISION,
    tip_amount DOUBLE PRECISION,
    tolls_amount DOUBLE PRECISION,
    improvement_surcharge DOUBLE PRECISION,
    total_amount DOUBLE PRECISION,
    payment_type INTEGER,
    trip_type INTEGER,
    congestion_surcharge DOUBLE PRECISION,
    airport_fee DOUBLE PRECISION,
    ehail_fee DECIMAL(8,2)
);

DROP TABLE IF EXISTS stg.taxi_zone_lookup;
CREATE TABLE IF NOT EXISTS stg.taxi_zone_lookup (
    locationid INTEGER NOT NULL,
    borough VARCHAR(50) NOT NULL,
    zone VARCHAR(100) NOT NULL,
    service_zone VARCHAR(50) NOT NULL,
    CONSTRAINT pk_taxi_zone_lookup PRIMARY KEY (locationid)
);

DROP TABLE IF EXISTS dwh.dim_date CASCADE;
CREATE TABLE dwh.dim_date (
    date_id INTEGER PRIMARY KEY,
    date DATE NOT NULL,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    day INTEGER NOT NULL,
    day_of_week VARCHAR(20) NOT NULL,
    week_of_year INTEGER NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

-- Location
DROP TABLE IF EXISTS dwh.dim_location CASCADE;
CREATE TABLE dwh.dim_location (
    location_id INTEGER PRIMARY KEY,
    borough VARCHAR(50),
    zone VARCHAR(100),
    service_zone VARCHAR(50)
);

-- Payment Type
DROP TABLE IF EXISTS dwh.dim_payment_type CASCADE;
CREATE TABLE dwh.dim_payment_type (
    payment_type_id INTEGER PRIMARY KEY,
    payment_type VARCHAR(50) NOT NULL
);

-- Vendor
DROP TABLE IF EXISTS dwh.dim_vendor CASCADE;
CREATE TABLE dwh.dim_vendor (
    vendor_id INTEGER PRIMARY KEY,
    vendor_name VARCHAR(100) NOT NULL
);

DROP TABLE IF EXISTS dwh.dim_taxi_type CASCADE;
CREATE TABLE dwh.dim_taxi_type (
    taxi_type_id INTEGER PRIMARY KEY,
    taxi_type_name VARCHAR(50) NOT NULL,
    taxi_type_description VARCHAR(200)
);

-- Rate Code
DROP TABLE IF EXISTS dwh.dim_rate_code CASCADE;
CREATE TABLE dwh.dim_rate_code (
    rate_code_id INTEGER PRIMARY KEY,
    rate_code_name VARCHAR(100) NOT NULL
);

-- Yellow Taxi Trips
DROP TABLE IF EXISTS dwh.all_taxi_trips CASCADE;
CREATE TABLE dwh.all_taxi_trips (
    trip_id SERIAL PRIMARY KEY,
    vendor_id INTEGER REFERENCES dwh.dim_vendor(vendor_id),
    taxi_type_id INTEGER REFERENCES dwh.dim_taxi_type(taxi_type_id),
    pickup_datetime TIMESTAMP NOT NULL,
    dropoff_datetime TIMESTAMP NOT NULL,
    passenger_count DOUBLE PRECISION,
    trip_distance DOUBLE PRECISION,
    pickup_location_id INTEGER REFERENCES dwh.dim_location(location_id),
    dropoff_location_id INTEGER REFERENCES dwh.dim_location(location_id),
    payment_type INTEGER REFERENCES dwh.dim_payment_type(payment_type_id),
    fare_amount DOUBLE PRECISION,
    extra DOUBLE PRECISION,
    tip_amount DOUBLE PRECISION,
    tolls_amount DOUBLE PRECISION,
    improvement_surcharge DOUBLE PRECISION,
    total_amount DOUBLE PRECISION,
    congestion_surcharge DOUBLE PRECISION,
    airport_fee DOUBLE PRECISION,
    
    -- Additional fields for analytics
    rate_code_id INTEGER REFERENCES dwh.dim_rate_code(rate_code_id),
    store_and_fwd_flag BOOLEAN,
    mta_tax DOUBLE PRECISION,
    
    -- Green taxi specific fields
    trip_type INTEGER, -- 1=Street-hail, 2=Dispatch
    ehail_fee DOUBLE PRECISION,
    
    -- Audit fields
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
