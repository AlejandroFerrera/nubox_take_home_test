import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


from utils.api_client import APIClient
from db.handler import DBHandler
from config.logger import setup_logger

logger = setup_logger(__name__)


class CountryProcessor:
    """
    Processes country data from the OpenAQ API and manages country information in the database.
    """

    def __init__(self, db_client: DBHandler, api_client: APIClient):
        """
        Initialize the CountryProcessor.

        Args:
            db_client: Database handler for database operations.
            api_client: API client for making HTTP requests.
        """
        self.db_client = db_client
        self.api_client = api_client

    def process_country(self, country_name: str) -> tuple[int, int, str, str]:
        """
        Fetches country constants by country name, calls the API if the country is not in the database.

        Args:
            country_name: The name of the country to process.

        Returns:
            tuple[int, int, str, str]: A tuple containing the country_sk, country_id, country_code and country_name
        """
        # Check if country exists in database
        existing_country = self._get_existing_country(country_name)
        if existing_country:
            return existing_country

        # Fetch from API and insert into database
        return self._fetch_and_insert_country(country_name)

    def _get_existing_country(
        self, country_name: str
    ) -> tuple[int, int, str, str] | None:
        """
        Retrieves existing country from database.

        Args:
            country_name: The name of the country to search for.

        Returns:
            tuple[int, int, str, str] | None: Country data if found, None otherwise.
        """
        country_table = self.db_client.metadata.tables["config_country"]
        result = self.db_client.select(
            table=country_table, criteria={"country_name": country_name}
        )

        row = result.fetchone() if result else None
        if row:
            country_sk, country_id, country_code, country_name = row
            logger.info(
                "Country '%s' found in database with SK %d.",
                country_name,
                country_sk,
            )
            return country_sk, country_id, country_code, country_name

        return None

    def _fetch_and_insert_country(self, country_name: str) -> tuple[int, int, str, str]:
        """
        Fetches country from API and inserts into database.

        Args:
            country_name: The name of the country to fetch and insert.

        Returns:
            tuple[int, int, str, str]: The inserted country data.
        """
        logger.info(
            "Country '%s' not found in database. Fetching from API.", country_name
        )

        country_data = self._fetch_country_from_api(country_name)
        return self._insert_country(country_data)

    def _fetch_country_from_api(self, country_name: str) -> dict:
        """
        Fetches country data from the API.

        Args:
            country_name: The name of the country to fetch.

        Returns:
            dict: Country data from the API.
        """
        response = self.api_client.get(endpoint="/v3/countries", params={"limit": 200})
        countries = response.json().get("results", [])

        country_data = next(
            (
                country
                for country in countries
                if country.get("name").lower() == country_name.lower()
            ),
            None,
        )

        if not country_data:
            logger.error("Country '%s' not found in API response.", country_name)
            raise ValueError(f"Country '{country_name}' not found in API response.")

        country_id = country_data.get("id")
        country_code = country_data.get("code")
        country_name = country_data.get("name")

        if not country_id or not country_code or not country_name:
            logger.error(
                "Mandatory fields 'id', 'code' or 'name' are missing in API response for country '%s'.",
                country_name,
            )
            raise ValueError(
                f"Mandatory fields 'id', 'code' or 'name' are missing in API response for country '{country_name}'."
            )

        return {
            "country_id": country_id,
            "country_code": country_code,
            "country_name": country_name,
        }

    def _insert_country(self, country_data: dict) -> tuple[int, int, str, str]:
        """
        Inserts country data into the database.

        Args:
            country_data: Dictionary containing country data to insert.

        Returns:
            tuple[int, int, str, str]: The inserted country data.
        """
        country_table = self.db_client.metadata.tables["config_country"]

        result = self.db_client.insert_many(
            table=country_table,
            data=[country_data],
            returning_cols=["country_sk", "country_id", "country_code", "country_name"],
        )

        if result is None:
            logger.error(
                "Failed to insert country '%s' into the database.",
                country_data["country_name"],
            )
            raise ValueError(
                f"Failed to insert country '{country_data['country_name']}' into the database."
            )

        row = result.fetchone()
        if not row:
            logger.error(
                "No row returned after inserting country '%s'.",
                country_data["country_name"],
            )
            raise ValueError(
                f"No row returned after inserting country '{country_data['country_name']}'."
            )

        country_sk, country_id, country_code, country_name = row
        logger.info(
            "Inserted country '%s' with SK %d into the database.",
            country_name,
            country_sk,
        )

        return country_sk, country_id, country_code, country_name


if __name__ == "__main__":
    from config.config import config
    from db.handler import DBHandler
    from utils.api_client import APIClient

    db_handler = DBHandler(config)
    api_client = APIClient(
        base_url=config.OPENAQ_API_BASE_URL, api_key=config.OPENAQ_API_KEY
    )

    country_processor = CountryProcessor(db_client=db_handler, api_client=api_client)
    country_sk, country_id, country_code, country_name = (
        country_processor.process_country("Chile")
    )
    print(
        f"Country SK: {country_sk}, ID: {country_id}, Code: {country_code}, Name: {country_name}"
    )
