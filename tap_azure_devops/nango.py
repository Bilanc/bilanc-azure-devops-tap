import requests
from typing import Any, Dict


def get_nango_token(config: Dict[str, Any]) -> str:
    if not config.get("nango_secret_key"):
        raise ValueError("nango_secret_key not configured")
    if not config.get("nango_connection_id"):
        raise ValueError("nango_connection_id not configured")

    provider_config_key = config.get("nango_provider_config_key", "azure-devops")
    url = (
        f"https://api.nango.dev/connection/{config['nango_connection_id']}"
        f"?provider_config_key={provider_config_key}"
    )

    response = requests.get(
        url,
        headers={
            "Authorization": f"Bearer {config['nango_secret_key']}",
            "Content-Type": "application/json",
        },
    )
    response.raise_for_status()
    return response.json().get("credentials", {}).get("access_token")
