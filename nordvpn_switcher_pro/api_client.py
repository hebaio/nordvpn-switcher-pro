from typing import Any, Callable, Dict, List, Optional
import time

import requests
from fake_useragent import UserAgent

from .exceptions import ApiClientError


class NordVpnApiClient:
    """A client for interacting with the public NordVPN API."""
    
    _DEFAULT_SERVER_FIELDS = {
        "fields[servers.id]": "",
        "fields[servers.name]": "",
        "fields[servers.hostname]": "",
        "fields[servers.load]": "",
        "fields[servers.locations.id]": "",
        "fields[servers.locations.country.id]": ""
    }

    def __init__(self, os_name: str = None):
        self.session = requests.Session()
        if os_name:
            self.session.headers.update({"User-Agent": UserAgent(os=os_name).random})
        else:
            self.session.headers.update({"User-Agent": UserAgent().random})
        self._dns_flush_callback: Optional[Callable[[], None]] = None

    def register_dns_flusher(self, dns_flush_callback: Callable[[], None]):
        """
        Registers a callable that flushes the DNS cache between retry cycles.

        Args:
            dns_flush_callback: A no-argument callable (e.g., controller.flush_dns_cache)
                that performs the actual cache flush.
        """
        self._dns_flush_callback = dns_flush_callback

    def _get(self, url: str, params: Dict = None, error_message_prefix: str = None) -> Any:
        """
        Performs a GET request to a given API URL with a multi-stage retry strategy.

        The method first retries transient network issues (ConnectionError, Timeout, etc.)
        with increasing delays. If these retries fail, it optionally flushes the DNS cache
        via an attached controller and repeats the process using a secondary delay schedule.
        HTTP errors (4xx, 5xx) are retried once with a fixed delay to guard against
        short-lived API hiccups before propagating an error upstream.

        Args:
            url: The API URL to request.
            params: Optional query parameters.
            error_message_prefix: Optional custom prefix for error messages. If provided,
                                 messages will be: "{error_message_prefix}. Waiting {delay}s
                                 before re-checking (Attempt {attempt + 1}/{max_retries})..."
        
        Returns:
            The JSON response from the API.
        
        Raises:
            ApiClientError: If the request fails after all retries.
        """
        last_exception = None
        delays = [3, 5, 7, 10]  # Increasing delays for retries
        max_retries = len(delays)
        dns_retry_delays = delays[1::2]  # Secondary cycle delays after DNS flushes

        for dns_attempt in range(len(dns_retry_delays) + 1):
            http_retry_attempted = False

            for attempt in range(max_retries):
                try:
                    response = self.session.get(url, params=params, timeout=20)
                    response.raise_for_status()
                    return response.json()

                except requests.exceptions.HTTPError as e:
                    last_exception = e
                    if not http_retry_attempted:
                        http_retry_attempted = True
                        print("\x1b[33mHTTP error encountered. Retrying once after 10s...\x1b[0m")
                        time.sleep(10)
                        continue
                    raise ApiClientError(
                        f"HTTP Error for {url}: {e.response.status_code} - {e.response.text}"
                    ) from e

                except requests.exceptions.RequestException as e:
                    last_exception = e
                    if attempt < max_retries - 1:
                        delay = delays[attempt]
                        prefix = error_message_prefix if error_message_prefix else "Network request failed"
                        print(f"\x1b[33m{prefix}. Waiting {delay}s before re-checking (Attempt {attempt + 1}/{max_retries})...\x1b[0m")
                        time.sleep(delay)
                    else:
                        print(f"\x1b[91mError: Network request failed after {max_retries} attempts.\x1b[0m")

            # Inner loop failed: optionally flush DNS and retry the whole cycle.
            if dns_attempt < len(dns_retry_delays) and self._dns_flush_callback:
                flush_delay = dns_retry_delays[dns_attempt]
                print("\x1b[33mNetwork still unstable. Flushing DNS cache before retrying...\x1b[0m")
                try:
                    self._dns_flush_callback()
                except Exception as flush_error:
                    print(f"\x1b[91mDNS flush failed: {flush_error}\x1b[0m")
                print(f"\x1b[33mWaiting {flush_delay}s after DNS flush before retrying...\x1b[0m")
                time.sleep(flush_delay)
                continue
            break

        # If all retries fail, raise the final exception.
        raise ApiClientError(f"Request failed for {url} after {max_retries} attempts") from last_exception

    def get_current_ip_info(self, error_message_prefix: str = None) -> Dict:
        """
        Fetches information about the current IP address.
        
        Args:
            error_message_prefix: Optional custom prefix for error messages.
        """
        url = "https://api.nordvpn.com/v1/helpers/ips/insights"
        return self._get(url, error_message_prefix=error_message_prefix)

    def get_countries(self) -> List[Dict]:
        """Fetches a list of all countries with NordVPN servers."""
        url = "https://api.nordvpn.com/v1/servers/countries"
        return self._get(url)

    def get_groups(self) -> List[Dict]:
        """Fetches a list of all server groups (e.g., P2P, Regions)."""
        url = "https://api.nordvpn.com/v1/servers/groups"
        return self._get(url)
    
    def get_technologies(self) -> List[Dict]:
        """Fetches a list of all supported technologies."""
        url = "https://api.nordvpn.com/v1/technologies"
        return self._get(url)
    
    def get_group_server_count(self, group_id: int) -> Dict:
        """Fetches the number of servers in a specific group."""
        url = "https://api.nordvpn.com/v1/servers/count"
        params = {"filters[servers_groups][id]": group_id}
        return self._get(url, params=params)

    def get_recommendations(self, params: Dict) -> List[Dict]:
        """
        Fetches recommended servers based on filters.
        """
        url = "https://api.nordvpn.com/v1/servers/recommendations"
        return self._get(url, params=params)

    def get_servers_v2(self, params: Dict) -> Dict:
        """
        Fetches server data from the efficient v2 endpoint.
        """
        url = "https://api.nordvpn.com/v2/servers"
        return self._get(url, params=params)

    def get_server_details(self, server_id: int) -> List[Dict]:
        """
        Fetches detailed information for a single server by its ID.
        """
        url = "https://api.nordvpn.com/v1/servers"
        params = self._DEFAULT_SERVER_FIELDS.copy()
        params.update({
            "filters[servers.id]": server_id,
            "fields[servers.status]": "",
            "fields[servers.locations.country.city.name]": "",
        })
        return self._get(url, params=params)