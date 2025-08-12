import os
import sys
from typing import List, Dict, Optional, Tuple

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.api_client import APIClient
from db.handler import DBHandler
from config.logger import setup_logger

logger = setup_logger(__name__)


class StationProcessor:
    """
    Processes station data from the OpenAQ API and manages station information in the database.
    """

    def __init__(self, db_client: DBHandler, api_client: APIClient):
        """
        Initialize the StationProcessor.

        Args:
            db_client: Database handler for database operations.
            api_client: API client for making HTTP requests.
        """
        self.db_client = db_client
        self.api_client = api_client

    def process_station(self, country_id: int, locality: str) -> List[Tuple[int, str]]:
        """
        Fetches all stations from a specific locality and stores them in the database.

        Args:
            country_id: The ID of the country to filter stations by.
            locality: The locality name to filter stations by.

        Returns:
            List of stations found in the specified locality.
        """
        try:
            stations_data = self._fetch_stations_data(country_id)
            if not stations_data:
                return []

            stations_in_locality = self._filter_stations_by_locality(
                stations_data, locality
            )
            if not stations_in_locality:
                logger.info(f"No stations found in locality: {locality}")
                return []

            locality_sk = self._get_or_create_locality(country_id, locality)
            if not locality_sk:
                return []

            parameters_to_monitor = self._get_parameters_to_monitor_tuples()
            stations = self._process_stations_batch(
                stations_in_locality, locality_sk, parameters_to_monitor
            )

            logger.info(
                f"Successfully processed {len(stations_in_locality)} stations in {locality}"
            )
            return stations

        except Exception as e:
            logger.error(
                f"Error processing stations for country {country_id}, locality {locality}: {e}"
            )
            return []

    def _fetch_stations_data(self, country_id: int) -> List[Dict]:
        """
        Fetch stations data from the API.

        Args:
            country_id: The ID of the country to fetch stations for.
        Returns:
            List of station data dictionaries.
        """
        try:
            response = self.api_client.get(
                endpoint="/v3/locations",
                params={"countries_id": [country_id], "limit": 200},
            )
            stations_data = response.json().get("results", [])

            if not stations_data:
                logger.info("No stations found for country_id %s", country_id)

            return stations_data
        except Exception as e:
            logger.error(f"Failed to fetch stations data for country {country_id}: {e}")
            return []

    def _get_or_create_locality(self, country_id: int, locality: str) -> Optional[int]:
        """
        Get or create locality in the database and return its surrogate key.

        Args:
            country_id: The ID of the country to which the locality belongs.
            locality: The name of the locality to get or create.
        Returns:
            Optional[int]: The surrogate key of the locality if found or created, None otherwise.

        """
        country_info = self._get_country_info(country_id)
        if not country_info:
            return None

        country_code, country_name = country_info
        return self._upsert_locality(locality, country_code, country_name)

    def _get_country_info(self, country_id: int) -> Optional[Tuple[str, str]]:
        """
        Retrieve country information from the database.

        Args:
            country_id: The ID of the country to retrieve information for.
        Returns:
            Optional[Tuple[str, str]]: A tuple containing country code and name if found, None otherwise.
        """
        try:
            country_result = self.db_client.select(
                table=self.db_client.metadata.tables["config_country"],
                criteria={"country_id": country_id},
            )
            country_row = country_result.fetchone() if country_result else None

            if not country_row:
                logger.error(f"Country with ID {country_id} not found in database.")
                return None

            _, _, country_code, country_name = country_row
            return country_code, country_name
        except Exception as e:
            logger.error(f"Error retrieving country info for ID {country_id}: {e}")
            return None

    def _upsert_locality(
        self, locality: str, country_code: str, country_name: str
    ) -> Optional[int]:
        """
        Insert or update locality and return its surrogate key.

        Args:
            locality: The name of the locality to insert or update.
            country_code: The code of the country to which the locality belongs.
            country_name: The name of the country to which the locality belongs.
        Returns:
            Optional[int]: The surrogate key of the locality if successfully inserted or updated, None otherwise.
        """
        try:
            locality_table = self.db_client.metadata.tables["dim_locality"]
            locality_result = self.db_client.upsert(
                table=locality_table,
                values={
                    "locality_name": locality,
                    "country_code": country_code,
                    "country_name": country_name,
                },
                conflict_columns=["locality_name"],
                update_columns=["country_code", "country_name"],
                returning_columns=["locality_sk"],
            )

            locality_row = locality_result.fetchone() if locality_result else None
            if not locality_row:
                logger.error(f"Failed to insert or retrieve locality '{locality}'.")
                return None

            return locality_row[0]
        except Exception as e:
            logger.error(f"Error upserting locality {locality}: {e}")
            return None

    def _process_stations_batch(
        self,
        stations: List[Dict],
        locality_sk: int,
        parameters_to_monitor: List[Tuple[int, str]],
    ) -> List[Tuple[int, str]]:
        """
        Process a batch of stations.

        Args:
            stations: List of station data dictionaries to process.
            locality_sk: The surrogate key of the locality to associate with the stations.
            parameters_to_monitor: List of tuples containing parameter surrogate keys and IDs to monitor.
        Returns:
            List[Tuple[int, str]]: List of tuples containing (station_sk, station_id) for successfully processed stations.
        """

        stations_processed = []

        for station in stations:
            try:
                result = self._process_single_station(station, locality_sk)
                if result:
                    station_sk, station_id = result
                    self._create_parameter_watermarks(station_sk, parameters_to_monitor)
                    stations_processed.append((station_sk, station_id))
            except Exception as e:
                station_id = station.get("id", "unknown")
                logger.error(f"Error processing station {station_id}: {e}")

        if not stations_processed:
            logger.warning(f"No stations processed for locality SK {locality_sk}.")
            return []

        logger.info(
            f"Successfully processed {len(stations_processed)} stations for locality SK {locality_sk}."
        )

        return stations_processed

    def _process_single_station(
        self, station: Dict, locality_sk: int
    ) -> Optional[Tuple[int, str]]:
        """
        Process a single station and return its surrogate key and station ID.

        Args:
            station: Dictionary containing station data.
            locality_sk: The surrogate key of the locality to associate with the station.
        Returns:
            Optional[Tuple[int, str]]: Tuple of (station_sk, station_id) if successful, None otherwise.
        """
        station_data = self._extract_station_fields(station)
        station_data["locality_sk"] = locality_sk

        station_table = self.db_client.metadata.tables["dim_station"]
        station_result = self.db_client.upsert(
            table=station_table,
            values=station_data,
            conflict_columns=["station_id"],
            update_columns=["station_name", "provider_name", "latitude", "longitude"],
            returning_columns=["station_sk", "station_id"],
        )

        station_row = station_result.fetchone() if station_result else None
        if not station_row:
            logger.error(
                f"Failed to get station_sk for station {station_data['station_id']}"
            )
            return None

        station_sk = station_row[0]
        station_id = station_row[1]

        return station_sk, station_id 

    def _create_parameter_watermarks(
        self, station_sk: int, parameters_to_monitor: List[Tuple[int, str]]
    ) -> None:
        """
        Create parameter watermark entries for a station.

        Args:
            station_sk: The surrogate key of the station.
            parameters_to_monitor: List of tuples containing parameter surrogate keys and IDs to monitor.
        Returns:
            None
        """
        watermark_table = self.db_client.metadata.tables[
            "ctrl_parameter_high_watermark"
        ]

        for parameter_sk, _ in parameters_to_monitor:
            try:
                self.db_client.insert_if_not_exists_one(
                    table=watermark_table,
                    values={"station_sk": station_sk, "parameter_sk": parameter_sk},
                    conflict_columns=["station_sk", "parameter_sk"],
                )
            except Exception as e:
                logger.error(
                    f"Error creating watermark for station {station_sk}, parameter {parameter_sk}: {e}"
                )

    def _filter_stations_by_locality(
        self, stations: List[Dict], locality: str
    ) -> List[Dict]:
        """
        Filters stations by locality name.

        Args:
            stations: List of station data from API.
            locality: The locality name to filter by.

        Returns:
            List of stations matching the locality.
        """
        return [
            station
            for station in stations
            if self._matches_locality(station.get("locality"), locality)
        ]

    def _matches_locality(
        self, station_locality: Optional[str], target_locality: str
    ) -> bool:
        """
        Check if station locality matches the target locality.

        Args:
            station_locality: Locality of the station.
            target_locality: Locality to match against.
        Returns:
            bool: True if the localities match, False otherwise.

        """
        if not station_locality:
            return False
        return station_locality.strip().lower() == target_locality.lower()

    def _extract_station_fields(self, station: Dict) -> Dict:
        """
        Extracts relevant fields from a station dictionary.

        Args:
            station: Dictionary containing station data.

        Returns:
            Dictionary containing station data.
        """
        coordinates = station.get("coordinates", {})
        provider = station.get("provider", {})

        return {
            "station_id": station.get("id"),
            "station_name": station.get("name"),
            "provider_name": provider.get("name"),
            "latitude": coordinates.get("latitude"),
            "longitude": coordinates.get("longitude"),
        }

    def _get_parameters_to_monitor(self) -> List[Dict]:
        """
        Retrieves the list of parameters to monitor from the database.

        Returns:
            List of dictionaries containing parameter information.
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
            logger.error(f"Error retrieving parameters to monitor: {e}")
            return []

    def _get_parameters_to_monitor_tuples(self) -> List[Tuple[int, str]]:
        """
        Get parameters to monitor as tuples of (parameter_sk, parameter_id).

        Returns:
            List[Tuple[int, str]]: List of tuples containing parameter surrogate keys and IDs.
        """
        parameters = self._get_parameters_to_monitor()
        return [(param["parameter_sk"], param["parameter_id"]) for param in parameters]


if __name__ == "__main__":
    from config.config import config
    from db.handler import DBHandler
    from utils.api_client import APIClient

    db_handler = DBHandler(config)
    api_client = APIClient(
        base_url=config.OPENAQ_API_BASE_URL, api_key=config.OPENAQ_API_KEY
    )

    station_processor = StationProcessor(db_client=db_handler, api_client=api_client)
    station_processor.process_station(country_id=3, locality="Puerto Montt")
