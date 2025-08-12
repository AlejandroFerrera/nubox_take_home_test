import argparse
from etl.openaq_pipeline import OpenAQPipeline
from config.logger import setup_logger

logger = setup_logger(__name__)


def main():
    try:
        parser = argparse.ArgumentParser(description="Open AQ data pipeline")
        parser.add_argument("--country", help="Country to process", required=True)
        parser.add_argument("--locality", help="Locality to process", required=True)
        args = parser.parse_args()

        locality = args.locality.strip()
        country = args.country.strip()

        pipeline = OpenAQPipeline()
        pipeline.run(country=country, locality=locality)

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise  # RETRY OR ALERT IN THE MACRO SYSTEM


if __name__ == "__main__":
    main()
