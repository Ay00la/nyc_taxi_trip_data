-- Indexes on fact table foreign keys
CREATE INDEX idx_all_taxi_trips_vendor_id ON dwh.all_taxi_trips(vendor_id);
CREATE INDEX idx_all_taxi_trips_taxi_type_id ON dwh.all_taxi_trips(taxi_type_id);
CREATE INDEX idx_all_taxi_trips_pickup_datetime ON dwh.all_taxi_trips(pickup_datetime);
CREATE INDEX idx_all_taxi_trips_dropoff_datetime ON dwh.all_taxi_trips(dropoff_datetime);
CREATE INDEX idx_all_taxi_trips_pickup_location_id ON dwh.all_taxi_trips(pickup_location_id);
CREATE INDEX idx_all_taxi_trips_dropoff_location_id ON dwh.all_taxi_trips(dropoff_location_id);
CREATE INDEX idx_all_taxi_trips_payment_type ON dwh.all_taxi_trips(payment_type);
CREATE INDEX idx_all_taxi_trips_rate_code_id ON dwh.all_taxi_trips(rate_code_id);

-- Composite indexes
CREATE INDEX idx_all_taxi_trips_pickup_date_location ON dwh.all_taxi_trips(DATE(pickup_datetime), pickup_location_id);
CREATE INDEX idx_all_taxi_trips_vendor_payment ON dwh.all_taxi_trips(vendor_id, payment_type);
CREATE INDEX idx_all_taxi_trips_taxi_type_date ON dwh.all_taxi_trips(taxi_type_id, DATE(pickup_datetime));

-- Indexes on dimension tables
CREATE INDEX idx_dim_date_date ON dwh.dim_date(date);
CREATE INDEX idx_dim_date_year_month ON dwh.dim_date(year, month);
CREATE INDEX idx_dim_location_borough ON dwh.dim_location(borough);
CREATE INDEX idx_dim_location_zone ON dwh.dim_location(zone);