import requests
from typing import Optional, Dict
import functools
import time
from config.logger import setup_logger

logger = setup_logger(__name__)

MAX_RETRIES = 5

def retry_request_on_failure(
    max_retries: int = MAX_RETRIES, delay: float = 1.0, backoff: float = 2.0
):
    """
    Decorator that retries HTTP requests on failure with exponential backoff.

    Args:
        max_retries: Maximum number of retry attempts.
        delay: Initial delay between retries in seconds.
        backoff: Backoff multiplier for exponential delay.

    Returns:
        Decorator function that wraps the original function with retry logic.
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except requests.exceptions.RequestException as e:
                    if attempt == max_retries - 1:
                        logger.error(
                            f"Request failed after {max_retries} attempts: {e}"
                        )
                        raise
                    wait_time = delay * (backoff**attempt)
                    logger.warning(
                        f"Request failed (attempt {attempt + 1}/{max_retries}): {e}. Retrying in {wait_time}s..."
                    )
                    time.sleep(wait_time)

        return wrapper

    return decorator


class APIClient:
	def __init__(self, base_url: str, timeout: int = 30, api_key: Optional[str] = None):
		"""
		Initialize the API client.

		Args:
			base_url: The base URL for the API.
			timeout: Request timeout in seconds.
			api_key: Optional API key for authentication.
		"""
		self.base_url = base_url.rstrip("/")
		self.timeout = timeout
		self.session = requests.Session()
		
		if api_key:
			self.session.headers.update({"Authorization": f"Bearer {api_key}"})
			self.session.headers.update({"X-API-Key": api_key})
		
		self.ping()

	def ping(self):
		"""Ping the base URL and raise an error if the site is down."""
		try:
			response = self.session.get(self.base_url, timeout=self.timeout)
			response.raise_for_status()
		except requests.exceptions.RequestException as e:
			logger.error(f"Failed to ping base URL '{self.base_url}': {e}")
			raise RuntimeError(f"Base URL '{self.base_url}' is not reachable.") from e

	@retry_request_on_failure(delay=1.0, backoff=2.0)
	def get(self, endpoint: str, params: Optional[Dict] = None) -> requests.Response:
		"""
		Make a GET request to the specified endpoint.

		Args:
			endpoint: The API endpoint to request.
			params: Optional query parameters to include in the request.

		Returns:
			requests.Response: The HTTP response object.
		"""
		url = f"{self.base_url}/{endpoint.lstrip('/')}"
		response = self.session.get(url, params=params, timeout=self.timeout)
		response.raise_for_status()
		return response

	def close(self):
		"""Close the session."""
		self.session.close()
