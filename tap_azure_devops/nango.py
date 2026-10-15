import time
from typing import Any, Dict

import backoff
import requests

NANGO_API_URL = "https://api.nango.dev"
NANGO_PROVIDER_CONFIG_KEY = "azure-devops"
NANGO_REQUEST_TIMEOUT = 30
NANGO_MAX_TRIES = 5
NANGO_MAX_RETRY_AFTER = 60


class NangoServerError(Exception):
    """Nango answered 429 or 5xx; retried by `fetch_nango_connection`."""


class NangoRefreshError(Exception):
    """Nango could not serve credentials for the connection."""


def nango_error_details(response: requests.Response) -> str:
    """Nango's `error.code: error.message`; never the connection payload."""
    try:
        error = response.json().get("error")
    except (ValueError, AttributeError):
        error = None
    if isinstance(error, dict):
        error = ": ".join(str(v) for v in (error.get("code"), error.get("message")) if v)
    return str(error or response.reason or f"HTTP {response.status_code}")


def _retry_after(response: requests.Response) -> int:
    try:
        return min(int(response.headers.get("Retry-After", 0)), NANGO_MAX_RETRY_AFTER)
    except ValueError:
        return 0


@backoff.on_exception(
    backoff.expo,
    (requests.RequestException, NangoServerError),
    max_tries=NANGO_MAX_TRIES,
)
def fetch_nango_connection(
    config: Dict[str, Any], provider_config_key: str
) -> requests.Response:
    """GET the Nango connection, retrying timeouts, connection errors, 429 and 5xx."""
    response = requests.get(
        f"{NANGO_API_URL}/connection/{config['nango_connection_id']}"
        f"?provider_config_key={provider_config_key}",
        headers={"Authorization": f"Bearer {config['nango_secret_key']}"},
        timeout=NANGO_REQUEST_TIMEOUT,
    )

    if response.status_code == 429:
        time.sleep(_retry_after(response))
    if response.status_code == 429 or response.status_code >= 500:
        raise NangoServerError(
            f"Nango returned {response.status_code}: {nango_error_details(response)}"
        )
    return response


def get_nango_token(config: Dict[str, Any]) -> str:
    """Return the Azure DevOps PAT Nango holds for the connection (Basic auth, no refresh)."""
    for key in ("nango_secret_key", "nango_connection_id"):
        if not config.get(key):
            raise ValueError(f"{key} not configured")

    provider_config_key = config.get("nango_provider_config_key", NANGO_PROVIDER_CONFIG_KEY)
    response = fetch_nango_connection(config, provider_config_key)

    if not response.ok:
        raise NangoRefreshError(
            f"Nango returned {response.status_code} for connection "
            f"{config['nango_connection_id']}: {nango_error_details(response)}"
        )

    return response.json().get("credentials", {}).get("password")
