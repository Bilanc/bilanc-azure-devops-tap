from typing import Any, Dict, Optional

import backoff
import requests

NANGO_API_URL = "https://api.nango.dev"
NANGO_PROVIDER_CONFIG_KEY = "azure-devops"
NANGO_REQUEST_TIMEOUT = 30
NANGO_MAX_TRIES = 5


class NangoServerError(Exception):
    """Nango answered with a 5xx; retried by `fetch_nango_connection`."""


class NangoRefreshError(Exception):
    """Nango could not serve credentials for the connection (non-retriable)."""

    def __init__(self, message: str, response: Optional[requests.Response] = None):
        super().__init__(message)
        self.response = response
        self.status_code = response.status_code if response is not None else None


def nango_error_details(response: requests.Response) -> str:
    """Return Nango's `error.code: error.message` from an error body.

    Only the error fields are surfaced so alerts never carry the connection
    payload (end user name/email, credentials).
    """
    try:
        body = response.json()
    except ValueError:
        body = None

    error = body.get("error") if isinstance(body, dict) else None
    if isinstance(error, dict):
        code = error.get("code") or error.get("type")
        message = error.get("message")
        details = ": ".join(str(part) for part in (code, message) if part)
        if details:
            return details
    if isinstance(error, str) and error:
        return error

    return (response.reason or "").strip() or f"HTTP {response.status_code}"


@backoff.on_exception(
    backoff.expo,
    (
        requests.Timeout,
        requests.ConnectionError,
        requests.exceptions.ChunkedEncodingError,
        NangoServerError,
    ),
    max_tries=NANGO_MAX_TRIES,
    factor=2,
)
def fetch_nango_connection(
    config: Dict[str, Any], provider_config_key: str
) -> requests.Response:
    """GET the Nango connection, retrying timeouts, connection errors and 5xx."""
    url = (
        f"{NANGO_API_URL}/connection/{config['nango_connection_id']}"
        f"?provider_config_key={provider_config_key}"
    )

    response = requests.get(
        url,
        headers={
            "Authorization": f"Bearer {config['nango_secret_key']}",
            "Content-Type": "application/json",
        },
        timeout=NANGO_REQUEST_TIMEOUT,
    )

    if response.status_code >= 500:
        raise NangoServerError(
            f"Nango returned {response.status_code} fetching connection: "
            f"{nango_error_details(response)}"
        )

    return response


def get_nango_token(config: Dict[str, Any]) -> str:
    """Return the Azure DevOps PAT Nango holds for the connection.

    The connection is a Basic-auth (PAT) connection, so there is no token
    refresh involved: timeouts, connection errors and Nango 5xx are backed off
    and retried, any other non-2xx raises `NangoRefreshError` carrying Nango's
    error code and message.
    """
    if not config.get("nango_secret_key"):
        raise ValueError("nango_secret_key not configured")
    if not config.get("nango_connection_id"):
        raise ValueError("nango_connection_id not configured")

    provider_config_key = config.get(
        "nango_provider_config_key", NANGO_PROVIDER_CONFIG_KEY
    )

    response = fetch_nango_connection(config, provider_config_key)

    if not response.ok:
        raise NangoRefreshError(
            f"Nango returned {response.status_code} for connection "
            f"{config['nango_connection_id']} ({provider_config_key}): "
            f"{nango_error_details(response)}",
            response,
        )

    return response.json().get("credentials", {}).get("password")
