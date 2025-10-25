import requests, re

from google.cloud import bigquery
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
import google.auth

class APIRequestError(Exception): pass
class AuthenticationError(APIRequestError): pass

class KEAuth:

    def __init__(self):
        self.__credentials = None
        self.__project = None

    def _get_credentials(self) -> Credentials:
            if self.__credentials is None:
                self.__credentials, self.__project = google.auth.default()

            if not self.__credentials.valid:
                try:
                    self.__credentials.refresh(Request())
                except Exception as e:
                    raise AuthenticationError(f"Failed to refresh Google credentials: {e}") from e

            return self.__credentials

    def _get_headers(self) -> dict:
        credentials = self._get_credentials()
        return {
          "Authorization": f"Bearer {credentials.token}",
          "Content-Type": "application/json"
        }

    def get_url_content(self, url: str) -> str:
            headers = self._get_headers()
            try:
                response = requests.get(url, headers=headers)
                response.raise_for_status() # Raises for 4xx or 5xx status codes
                return response.text

            except requests.exceptions.HTTPError as e:
                if e.response.status_code in (401, 403):
                    raise AuthenticationError(
                        f"Access Denied (HTTP {e.response.status_code}) fetching {url}. "
                        "Ensure your service account has the necessary IAM roles."
                    ) from e

                raise APIRequestError(f"HTTP Error {e.response.status_code} fetching {url}: {e.response.text}") from e

            except requests.exceptions.RequestException as e:
                raise APIRequestError(f"Network error fetching {url}: {e}") from e