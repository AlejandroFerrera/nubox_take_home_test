
CREATE TABLE dim_locality (
	locality_sk INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	locality_name VARCHAR(300) NOT NULL UNIQUE,
	country_code VARCHAR(20) NOT NULL,
	country_name VARCHAR(150) NOT NULL,
	UNIQUE (locality_name, country_code)
);

CREATE TABLE dim_station (
	station_sk INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	station_id INTEGER NOT NULL UNIQUE,
	station_name VARCHAR(300) NOT NULL,
	provider_name VARCHAR(200),
	latitude DOUBLE PRECISION CHECK (latitude BETWEEN -90 AND 90),
	longitude DOUBLE PRECISION CHECK (longitude BETWEEN -180 AND 180),
	locality_sk INTEGER NOT NULL REFERENCES dim_locality(locality_sk)
);

CREATE TABLE fact_air_quality_measurement (
	station_sk INTEGER NOT NULL REFERENCES dim_station(station_sk),
	measurement_timestamp TIMESTAMPTZ NOT NULL,
	parameter VARCHAR(50) NOT NULL,
	value NUMERIC(10,4) NOT NULL,
	unit VARCHAR(50) NOT NULL,
	PRIMARY KEY (station_sk, parameter, measurement_timestamp)
);

