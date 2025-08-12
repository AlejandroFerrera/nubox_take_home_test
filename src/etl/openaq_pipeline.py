from etl.process_station import StationProcessor
from etl.process_country import CountryProcessor
from etl.process_air_quality_measurements import AirQualityMeasurementsProcessor
from insights.get_insights import generate_air_quality_insights
from db.handler import DBHandler
from utils.api_client import APIClient
from config.config import config
from config.logger import setup_logger

logger = setup_logger(__name__)


class OpenAQPipeline:
    """Main pipeline class for processing OpenAQ data."""

    def __init__(self):
        """Initialize the pipeline with all components."""
        self.db_handler = DBHandler(config)
        self.api_client = APIClient(
            base_url=config.OPENAQ_API_BASE_URL,
            timeout=config.API_TIMEOUT,
            api_key=config.OPENAQ_API_KEY,
        )
        logger.info("Initialized API client and database handler")

        self.country_processor = CountryProcessor(self.db_handler, self.api_client)
        self.station_processor = StationProcessor(self.db_handler, self.api_client)
        self.measurement_processor = AirQualityMeasurementsProcessor(
            self.db_handler, self.api_client
        )

    def run(self, country: str, locality: str):
        """
        Run the complete OpenAQ data pipeline.

        Args:
            country (str): Country to process
            locality (str): Locality to process
        """
        try:
            country = country.strip()
            locality = locality.strip()

            logger.info(f"Processing country: {country}")
            country_sk, country_id, country_code, country_name = (
                self.country_processor.process_country(country)
            )
            logger.info(
                f"Country processed. SK: {country_sk}, ID: {country_id}, Code: {country_code}, Name: {country_name}"
            )

            logger.info(f"Processing locality: {locality}")
            stations = self.station_processor.process_station(country_id, locality)

            if not stations:
                logger.warning(f"No stations found for locality: {locality}")
                return

            logger.info(f"Found {len(stations)} stations in {locality}")

            # Esto se podria ejecutar con threading o multiprocessing para mejorar el rendimiento
            # Se mantiene secuencial para observabilidad de logs
            for station_sk, station_id in stations:
                logger.info(
                    f"Processing measurements for station {station_id} (SK: {station_sk})"
                )
                success = self.measurement_processor.process_measurements_for_station(
                    station_id=int(station_id), station_sk=station_sk
                )
                if success:
                    logger.info(
                        f"Successfully processed measurements for station {station_id}"
                    )
                else:
                    logger.warning(
                        f"Failed to process measurements for station {station_id}"
                    )

            logger.info("OpenAQ data pipeline completed successfully")
            
            logger.info("Generating air quality insights...")
            generate_air_quality_insights(country, locality, self.db_handler)

        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            raise  # RETRY OR ALERT IN THE MACRO SYSTEM
        finally:
            self._cleanup()

    def _cleanup(self):
        """Clean up resources."""
        if self.api_client:
            self.api_client.close()
        if self.db_handler:
            self.db_handler.close()
