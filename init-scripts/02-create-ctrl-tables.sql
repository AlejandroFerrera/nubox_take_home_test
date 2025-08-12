
CREATE TABLE config_country (
	country_sk INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	country_id INTEGER NOT NULL UNIQUE,
	country_code VARCHAR(20) NOT NULL UNIQUE,
	country_name VARCHAR(150) NOT NULL,
	UNIQUE (country_code, country_name)
);


CREATE TABLE config_parameter_to_monitor (
	parameter_sk INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	parameter_id INTEGER NOT NULL UNIQUE,
	parameter_name VARCHAR(50) NOT NULL UNIQUE,
	unit VARCHAR(50) NOT NULL
);

CREATE TABLE ctrl_parameter_high_watermark (
	station_sk INTEGER NOT NULL REFERENCES dim_station(station_sk),
	parameter_sk INTEGER NOT NULL REFERENCES config_parameter_to_monitor(parameter_sk),
	last_updated_at TIMESTAMPTZ,
	PRIMARY KEY (station_sk, parameter_sk)
);  