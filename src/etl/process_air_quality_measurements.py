import os
import sys
from typing import List, Dict, Optional, Tuple

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional, Tuple
from utils.api_client import APIClient
from db.handler import DBHandler
from config.logger import setup_logger

logger = setup_logger(__name__)


class AirQualityMeasurementsProcessor:
    """
    Processes air quality measurement data from the OpenAQ API and manages measurement information in the database.
    Handles incremental loading using station-parameter watermarks for efficient data processing.
    """

    def __init__(self, db_client: DBHandler, api_client: APIClient):
        """
        Initialize the AirQualityMeasurementsProcessor.

        Args:
            db_client: Database handler for database operations.
            api_client: API client for making HTTP requests to OpenAQ API.
        """
        self.db_client = db_client
        self.api_client = api_client

    def process_measurements_for_station(
        self, station_id: int, station_sk: int
    ) -> bool:
        """
        Processes air quality measurements for a specific station.
        Fetches sensors, filters by monitored parameters, and loads measurements incrementally.

        Args:
            station_id: The business key ID of the station from OpenAQ API.
            station_sk: The surrogate key of the station in the database.

        Returns:
            bool: True if measurements were processed successfully, False otherwise.
        """
        try:
            station_sensors = self._get_station_sensors(station_id)
            if not station_sensors:
                logger.info("No sensors found for station %s", station_id)
                return False

            parameters_to_monitor = self._get_parameters_to_monitor()
            if not parameters_to_monitor:
                logger.warning("No parameters configured to monitor")
                return False

            relevant_sensors = self._filter_sensors_by_monitored_parameters(
                station_sensors, parameters_to_monitor
            )

            if not relevant_sensors:
                logger.info("No relevant sensors found for station %s", station_id)
                return False

            success = True
            for sensor_id, parameter_info in relevant_sensors.items():
                parameter_sk = parameter_info["parameter_sk"]
                parameter_name = parameter_info["parameter_name"]
                parameter_unit = parameter_info["unit"]

                last_measurement_timestamp = self._get_last_measurement_timestamp(
                    station_sk, parameter_sk
                )

                sensor_success = self._process_sensor_measurements(
                    sensor_id,
                    station_sk,
                    parameter_sk,
                    parameter_name,
                    parameter_unit,
                    last_measurement_timestamp,
                )
                success = success and sensor_success

            return success

        except Exception as e:
            logger.error(
                "Error processing measurements for station %s: %s", station_id, e
            )
            return False

    def _get_station_sensors(self, station_id: int) -> List[Dict]:
        """
        Fetches sensors for a specific station from the OpenAQ API.

        Args:
            station_id: The ID of the station to fetch sensors for.

        Returns:
            List[Dict]: A list of sensor dictionaries with parameter information.
        """
        try:
            response = self.api_client.get(f"/v3/locations/{station_id}")
            results = response.json().get("results", [])

            if not results:
                logger.info("No location data found for station %s", station_id)
                return []

            location_data = results[0]
            sensors = location_data.get("sensors", [])

            logger.info("Found %d sensors for station %s", len(sensors), station_id)
            return sensors

        except Exception as e:
            logger.error("Failed to fetch sensors for station %s: %s", station_id, e)
            return []

    def _get_parameters_to_monitor(self) -> List[Dict]:
        """
        Retrieves the list of air quality parameters to monitor from the database.

        Returns:
            List[Dict]: List of parameter dictionaries with keys: parameter_sk, parameter_id, parameter_name, unit.
        """
        try:
            parameters_table = self.db_client.metadata.tables[
                "config_parameter_to_monitor"
            ]
            result = self.db_client.select(table=parameters_table, criteria={})

            if not result:
                logger.error("Failed to query config_parameter_to_monitor table.")
                return []

            parameters = result.fetchall()
            if not parameters:
                logger.warning(
                    "No parameters found in config_parameter_to_monitor table."
                )
                return []

            return [
                {
                    "parameter_sk": row[0],
                    "parameter_id": row[1],
                    "parameter_name": row[2],
                    "unit": row[3],
                }
                for row in parameters
            ]
        except Exception as e:
            logger.error("Error retrieving parameters to monitor: %s", e)
            return []

    def _filter_sensors_by_monitored_parameters(
        self, station_sensors: List[Dict], parameters_to_monitor: List[Dict]
    ) -> Dict[int, Dict]:
        """
        Filters station sensors to only include those monitoring configured air quality parameters.

        Args:
            station_sensors: List of sensor dictionaries from API.
            parameters_to_monitor: List of parameter dictionaries from database.

        Returns:
            Dict[int, Dict]: Dictionary mapping sensor_id to parameter info for relevant sensors.
        """
        monitored_params = {
            param["parameter_id"]: param for param in parameters_to_monitor
        }

        relevant_sensors = {}
        for sensor in station_sensors:
            parameter = sensor.get("parameter", {})
            parameter_id = parameter.get("id")

            if parameter_id in monitored_params:
                sensor_id = sensor.get("id")
                if sensor_id:
                    relevant_sensors[sensor_id] = monitored_params[parameter_id].copy()
                    sensor_unit = parameter.get("units")
                    if sensor_unit:
                        relevant_sensors[sensor_id]["sensor_unit"] = sensor_unit

        logger.info(
            "Found %d relevant sensors out of %d total sensors",
            len(relevant_sensors),
            len(station_sensors),
        )
        return relevant_sensors

    def _get_last_measurement_timestamp(
        self, station_sk: int, parameter_sk: int
    ) -> Optional[datetime]:
        """
        Retrieves the last measurement timestamp for a station-parameter combination from watermark table.

        Args:
            station_sk: The surrogate key of the station.
            parameter_sk: The surrogate key of the parameter.

        Returns:
            Optional[datetime]: The last measurement timestamp or None if not found.
        """
        try:
            watermark_table = self.db_client.metadata.tables[
                "ctrl_parameter_high_watermark"
            ]
            result = self.db_client.select(
                table=watermark_table,
                criteria={"station_sk": station_sk, "parameter_sk": parameter_sk},
            )

            if not result:
                return None

            row = result.fetchone()
            if not row or not row[2]: 
                return None

            return row[2]

        except Exception as e:
            logger.error(
                "Error retrieving last measurement timestamp for station %s, parameter %s: %s",
                station_sk,
                parameter_sk,
                e,
            )
            return None

    def _process_sensor_measurements(
        self,
        sensor_id: int,
        station_sk: int,
        parameter_sk: int,
        parameter_name: str,
        parameter_unit: str,
        last_measurement_timestamp: Optional[datetime],
    ) -> bool:
        """
        Processes air quality measurements for a specific sensor.

        Args:
            sensor_id: The ID of the sensor from OpenAQ API.
            station_sk: The surrogate key of the station.
            parameter_sk: The surrogate key of the parameter.
            parameter_name: The name of the parameter (e.g., 'pm25', 'no2').
            parameter_unit: The unit of the parameter (e.g., 'µg/m³').
            last_measurement_timestamp: The last measurement timestamp for incremental loading.

        Returns:
            bool: True if measurements were processed successfully.
        """
        try:
            measurements_raw = self._get_measurements_raw_data(
                sensor_id, last_measurement_timestamp
            )

            if not measurements_raw:
                logger.info("No measurements found for sensor %s", sensor_id)
                return True 

            measurements_data = self._extract_measurements_fields(
                measurements_raw, station_sk, parameter_name, parameter_unit
            )

            return self._load_measurements(measurements_data, station_sk, parameter_sk)

        except Exception as e:
            logger.error(
                "Error processing measurements for sensor %s: %s", sensor_id, e
            )
            return False

    def _get_measurements_raw_data(
        self, sensor_id: int, last_measurement_timestamp: Optional[datetime]
    ) -> List[Dict]:
        """
        Fetches raw measurement data from the OpenAQ API with pagination support.
        Handles API responses where 'found' may be '>100' format.

        Args:
            sensor_id: The ID of the sensor to fetch measurements for.
            last_measurement_timestamp: The last measurement timestamp to filter new measurements.

        Returns:
            List[Dict]: A list of dictionaries representing measurements from the API.
        """
        start_date, end_date = self._get_date_range(last_measurement_timestamp)
        all_measurements = []
        page = 1
        limit = 200

        try:
            while True:
                measurements = self._fetch_measurements_page(
                    sensor_id, start_date, end_date, page, limit
                )

                if not measurements:
                    logger.debug(
                        "No measurements found on page %d for sensor %s",
                        page,
                        sensor_id,
                    )
                    break

                all_measurements.extend(measurements)
                logger.debug(
                    "Fetched %d measurements on page %d for sensor %s",
                    len(measurements),
                    page,
                    sensor_id,
                )

                if not self._should_continue_pagination(measurements, limit):
                    break

                page += 1

            logger.info(
                "Fetched %d measurements for sensor %s",
                len(all_measurements),
                sensor_id,
            )
            return all_measurements

        except Exception as e:
            logger.error("Failed to fetch measurements for sensor %s: %s", sensor_id, e)
            return []

    def _get_date_range(
        self, last_measurement_timestamp: Optional[datetime]
    ) -> Tuple[str, str]:
        """
        Determines the date range for fetching measurements.

        Args:
            last_measurement_timestamp: The last measurement timestamp or None.

        Returns:
            Tuple[str, str]: Start and end date in ISO format.
        """
        now_utc = datetime.now(timezone.utc)

        if last_measurement_timestamp is None:
            logger.info(
                "No last measurement timestamp provided, fetching measurements from the last 7 days."
            )
            start_date = (now_utc - timedelta(days=7)).isoformat()
        else:
            start_date = last_measurement_timestamp.isoformat()

        return start_date, now_utc.isoformat()

    def _fetch_measurements_page(
        self, sensor_id: int, start_date: str, end_date: str, page: int, limit: int
    ) -> List[Dict]:
        """
        Fetches a single page of measurements from the API.

        Args:
            sensor_id: The ID of the sensor.
            start_date: Start date in ISO format.
            end_date: End date in ISO format.
            page: Page number to fetch.
            limit: Number of results per page.

        Returns:
            List[Dict]: List of measurements for this page.
        """
        params = {
            "datetime_from": start_date,
            "datetime_to": end_date,
            "page": page,
            "limit": limit,
        }

        response = self.api_client.get(
            f"/v3/sensors/{sensor_id}/measurements", params=params
        )
        response_data = response.json()
        return response_data.get("results", [])

    def _should_continue_pagination(self, measurements: List[Dict], limit: int) -> bool:
        """
        Determines if pagination should continue based on the number of results.

        Args:
            measurements: Current page measurements.
            limit: Results per page limit.

        Returns:
            bool: True if pagination should continue.
        """
        return len(measurements) == limit

    def _extract_measurements_fields(
        self,
        measurements_raw: List[Dict],
        station_sk: int,
        parameter_name: str,
        parameter_unit: str,
    ) -> List[Dict]:
        """
        Extracts measurement fields from raw OpenAQ API data.

        Args:
            measurements_raw: List of raw measurement dictionaries from API.
            station_sk: The surrogate key of the station.
            parameter_name: The name of the parameter.
            parameter_unit: The unit of the parameter.

        Returns:
            List[Dict]: List of processed measurement dictionaries ready for database insertion.
        """
        measurements_data = []
        for measurement in measurements_raw:
            try:
                measurement_data = self._extract_measurement_fields(
                    measurement, station_sk, parameter_name, parameter_unit
                )
                if measurement_data:
                    measurements_data.append(measurement_data)
            except Exception as e:
                logger.warning("Error extracting measurement fields: %s", e)
                continue

        return measurements_data

    def _extract_measurement_fields(
        self,
        measurement: Dict,
        station_sk: int,
        parameter_name: str,
        parameter_unit: str,
    ) -> Optional[Dict]:
        """
        Extracts measurement fields from a single measurement from OpenAQ API.

        Fields mapping for fact_air_quality_measurement table:
        - 'station_sk': Surrogate key of the station
        - 'measurement_timestamp': 'period.datetimeFrom.utc' from API
        - 'parameter': parameter name (e.g., 'pm25', 'no2')
        - 'value': measurement value from API
        - 'unit': parameter unit (e.g., 'µg/m³')

        Args:
            measurement: A dictionary representing a single measurement from API.
            station_sk: The surrogate key of the station.
            parameter_name: The name of the parameter.
            parameter_unit: The unit of the parameter.

        Returns:
            Optional[Dict]: A dictionary containing processed measurement fields or None if invalid.
        """
        try:
            value = measurement.get("value")
            if value is None:
                logger.warning("Measurement value is missing")
                return None

            period = measurement.get("period", {})
            datetime_from = period.get("datetimeFrom", {})
            timestamp = datetime_from.get("utc")

            if not timestamp:
                logger.warning("Measurement timestamp is missing")
                return None

            return {
                "station_sk": station_sk,
                "measurement_timestamp": timestamp,
                "parameter": parameter_name,
                "value": round(float(value), 4),
                "unit": parameter_unit,
            }

        except (ValueError, TypeError) as e:
            logger.warning("Error processing measurement value: %s", e)
            return None

    def _load_measurements(
        self, measurements_data: List[Dict], station_sk: int, parameter_sk: int
    ) -> bool:
        """
        Loads air quality measurements into the database and updates parameter watermark.
        Uses upsert_many to handle duplicate measurements gracefully and efficiently.

        Args:
            measurements_data: List of processed measurement dictionaries.
            station_sk: The surrogate key of the station.
            parameter_sk: The surrogate key of the parameter.

        Returns:
            bool: True if measurements were loaded successfully.
        """
        if not measurements_data:
            logger.debug(
                "No measurements to insert for station %s, parameter %s",
                station_sk,
                parameter_sk,
            )
            return True

        try:
            fact_measurement_table = self.db_client.metadata.tables[
                "fact_air_quality_measurement"
            ]

            result = self.db_client.upsert_many(
                table=fact_measurement_table,
                data=measurements_data,
                conflict_columns=[
                    "station_sk",
                    "parameter",
                    "measurement_timestamp",
                ],
                update_columns=[
                    "value",
                    "unit",
                ],
                returning_columns=["measurement_timestamp"],
            )

            latest_timestamp = None
            processed_count = 0

            if result:
                rows = result.fetchall()
                processed_count = len(rows)

                if rows:
                    latest_timestamp = max(row[0] for row in rows)

            if latest_timestamp:
                self._update_parameter_watermark(
                    station_sk, parameter_sk, latest_timestamp
                )

            logger.info(
                "Processed %d measurements for station %s, parameter %s. Last timestamp: %s",
                processed_count,
                station_sk,
                parameter_sk,
                latest_timestamp,
            )
            return True

        except Exception as e:
            logger.error(
                "Error loading measurements for station %s, parameter %s: %s",
                station_sk,
                parameter_sk,
                e,
            )
            return False

    def _update_parameter_watermark(
        self, station_sk: int, parameter_sk: int, last_timestamp: datetime
    ):
        """
        Updates the parameter watermark with the last measurement timestamp.

        Args:
            station_sk: The surrogate key of the station.
            parameter_sk: The surrogate key of the parameter.
            last_timestamp: The last measurement timestamp to update.
        """
        try:
            watermark_table = self.db_client.metadata.tables[
                "ctrl_parameter_high_watermark"
            ]

            self.db_client.update(
                watermark_table,
                values={
                    "station_sk": station_sk,
                    "parameter_sk": parameter_sk,
                    "last_updated_at": last_timestamp,
                },
                matching_columns=["station_sk", "parameter_sk"],
                fields_to_update=["last_updated_at"],
            )

        except Exception as e:
            logger.error(
                "Error updating watermark for station %s, parameter %s: %s",
                station_sk,
                parameter_sk,
                e,
            )


if __name__ == "__main__":
    from config.config import config
    from db.handler import DBHandler
    from utils.api_client import APIClient

    db_handler = DBHandler(config)
    api_client = APIClient(
        base_url=config.OPENAQ_API_BASE_URL, api_key=config.OPENAQ_API_KEY
    )

    air_quality_processor = AirQualityMeasurementsProcessor(
        db_client=db_handler, api_client=api_client
    )

    station_id = 25 
    station_sk = 12 

    success = air_quality_processor.process_measurements_for_station(
        station_id=station_id, station_sk=station_sk
    )

    if success:
        logger.info(
            "Successfully processed air quality measurements for station %s", station_id
        )
    else:
        logger.error(
            "Failed to process air quality measurements for station %s", station_id
        )
