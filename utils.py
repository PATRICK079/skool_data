"""
utils.py

last updated: 2025-07-12

todo
- add unit tests
- add google style docs for doc generation
- decouple supabase logic
- define scope for this file
"""

# external
from datetime import datetime
import time
import humanize
import pytz

# internal
from deps.settings import (
    RETRY_MAX_ATTEMPTS,
    RETRY_BACKOFF_SECONDS,
    get_timeout_for_request,
)


def local_to_utc(date_str, local_tz_str):
    """
    Convert a local date string (YYYY-MM-DD) to UTC datetime string (ISO format).
    """
    local_tz = pytz.timezone(local_tz_str)
    local_dt = local_tz.localize(datetime.strptime(date_str, "%Y-%m-%d"))
    utc_dt = local_dt.astimezone(pytz.utc)
    return utc_dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def ensure_iso(timestamp):
    """
    Ensures a timestamp is in ISO format.
    If already ISO string, returns it unchanged.
    If nanosecond timestamp (int/str), converts to ISO.
    """
    # If it's already a string that looks like ISO format
    if isinstance(timestamp, str) and any(c in timestamp for c in ["-", "T", ":"]):
        return timestamp

    # Otherwise convert using nano_to_iso
    return nano_to_iso(timestamp)


def nano_to_iso(nano_timestamp):
    # Handle string input if necessary
    if isinstance(nano_timestamp, str):
        nano_timestamp = int(nano_timestamp)

    # Ensure we're working with a reasonable timestamp
    # Convert nanoseconds to milliseconds first to avoid precision issues
    millis = nano_timestamp // 1000000
    seconds = millis / 1000.0

    # Convert to datetime and format with standard ISO format
    dt = datetime.fromtimestamp(seconds)
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]  # Trim microseconds to 3 digits


def get_human_readable_duration(start_time: float) -> str:
    """
    Convert a time difference (from time.time()) into a human-readable string.

    Args:
        start_time: The start time from time.time()

    Returns:
        A human-readable string like "2 mins", "1 hr and 30 mins", etc.
    """
    elapsed = time.time() - start_time
    human_readable = humanize.naturaldelta(elapsed)

    # Replace full words with abbreviations
    replacements = {
        " second": " sec",
        " minute": " min",
        " hour": " hr",
        " day": " d",
        " week": " wk",
        " month": " mo",
        " year": " yr",
    }

    for word, abbrev in replacements.items():
        human_readable = human_readable.replace(word, abbrev)

    return human_readable


def sort_organizations_by_last_scrape(organizations):
    import datetime

    """
    Sort organizations by their last full scrape timestamp.
    Organizations with missing or invalid timestamps will be placed at the beginning.
    """

    def get_last_scrape_time(org):
        metadata = org.get("public_metadata", {})
        last_full_scrape = metadata.get("last_full_scrape")

        # Debug output
        org_name = org.get("name", "Unknown")
        skool_slug = metadata.get("skool_slug", "No slug")
        membership = metadata.get("membership", "No membership")
        is_trial = metadata.get("is_trial", "No trial status")
        print(
            f"🔍 DEBUG - Organization: {org_name} | Skool Slug: {skool_slug} | Last Full Scrape: {last_full_scrape} | Membership: {membership} | Is Trial: {is_trial}"
        )

        # Handle missing or invalid timestamps
        if not last_full_scrape:
            print(f"⚠️ DEBUG - No last_full_scrape timestamp for {org_name}")
            return datetime.datetime.min  # Earliest possible date

        try:
            # Try to parse the timestamp
            # Assuming format is ISO 8601 (e.g., "2023-01-01T12:00:00Z")
            return datetime.datetime.fromisoformat(
                last_full_scrape.replace("Z", "+00:00")
            )
        except (ValueError, TypeError) as e:
            print(f"⚠️ DEBUG - Invalid timestamp format for {org_name}: {e}")
            return datetime.datetime.min  # Earliest possible date

    # Sort organizations by last scrape time (oldest first)
    sorted_orgs = sorted(organizations, key=get_last_scrape_time)

    # Print the sorted order for verification
    print("\n📊 DEBUG - Organizations sorted by last scrape time (oldest first):")
    for i, org in enumerate(sorted_orgs):
        metadata = org.get("public_metadata", {})
        last_full_scrape = metadata.get("last_full_scrape", "Never")
        org_name = org.get("name", "Unknown")
        skool_slug = metadata.get("skool_slug", "No slug")
        membership = metadata.get("membership", "No membership")
        is_trial = metadata.get("is_trial", "No trial status")
        print(
            f"{i+1}. {org_name} | Skool Slug: {skool_slug} | Last Full Scrape: {last_full_scrape} | Membership: {membership} | Is Trial: {is_trial}"
        )

    return sorted_orgs


def request_with_retries(
    method,
    url,
    max_retries=RETRY_MAX_ATTEMPTS,
    backoff=RETRY_BACKOFF_SECONDS,
    timeout=None,
    skip_retry_on_404=False,
    skip_retry_on_401=False,
    **kwargs,
):
    """
    Make an HTTP request with retries and exponential backoff using cloudscraper.
    Args:
        method (str): 'get', 'post', etc.
        url (str): The URL to request.
        max_retries (int): Maximum number of attempts.
        backoff (int): Base backoff time in seconds.
        timeout (int): Timeout for each request. If None, will use proxy-aware timeout.
        skip_retry_on_404 (bool): If True, do not retry on 404 responses (return immediately).
        skip_retry_on_401 (bool): If True, do not retry on 401 responses (return immediately).
        **kwargs: Passed to cloudscraper.request.
    Returns:
        cloudscraper.Response or None if all retries fail.
    """
    import cloudscraper
    from requests.exceptions import (
        ProxyError,
        Timeout,
        ConnectionError,
        RequestException,
    )
    import time

    scraper = cloudscraper.create_scraper()

    # Determine if we're using proxies and set appropriate timeout
    use_proxy = "proxies" in kwargs and kwargs["proxies"] is not None
    if timeout is None:
        timeout = get_timeout_for_request(use_proxy)

    for attempt in range(1, max_retries + 1):
        try:
            # Remove skip_retry_on_404 and skip_retry_on_401 from kwargs before passing to cloudscraper
            kwargs_to_pass = kwargs.copy()
            kwargs_to_pass.pop("skip_retry_on_404", None)
            kwargs_to_pass.pop("skip_retry_on_401", None)

            response = scraper.request(method, url, timeout=timeout, **kwargs_to_pass)
            if skip_retry_on_404 and response.status_code == 404:
                # Immediately return 404 responses without retrying or logging as error (for Clerk only)
                return response
            if skip_retry_on_401 and response.status_code == 401:
                # Immediately return 401 responses without retrying or logging as error
                return response
            response.raise_for_status()
            return response
        except (ProxyError, Timeout, ConnectionError, RequestException) as e:
            print(f"Request failed (attempt {attempt}/{max_retries}): {e}")

            # If this is a proxy error and we have more retries, try without proxy on next attempt
            if isinstance(e, ProxyError) and attempt < max_retries and use_proxy:
                print("Proxy failed, trying without proxy on next attempt...")
                kwargs["proxies"] = None
                use_proxy = False
                timeout = get_timeout_for_request(use_proxy)

            if attempt == max_retries:
                print("Max retries reached. Giving up.")
                return None
            sleep_time = backoff * attempt
            print(f"Retrying in {sleep_time} seconds...")
            time.sleep(sleep_time)
