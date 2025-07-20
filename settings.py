"""
settings.py

last updated: 2025-07-12

todo
- add unit tests
- add google style docs for doc generation
- decouple supabase logic
- define scope for this file
- add more settings / decouople from other files
"""

# external
import random
import os

delay = 1.5

RETRY_MAX_ATTEMPTS = 5
RETRY_BACKOFF_SECONDS = 2
RETRY_TIMEOUT_SECONDS = 30

# Proxy timeout settings (longer for proxy connections)
PROXY_TIMEOUT_SECONDS = 60
PROXY_CONNECT_TIMEOUT = 30

# Proxy configuration - use environment variables for security
# Set BRIGHT_DATA_PROXY_LIST in your .env file with comma-separated proxy URLs
# Example: BRIGHT_DATA_PROXY_LIST=proxy1,proxy2,proxy3
PROXY_LIST = os.getenv("BRIGHT_DATA_PROXY_LIST", "").split(",") if os.getenv("BRIGHT_DATA_PROXY_LIST") else []

# Environment variable to disable proxies if needed
USE_PROXIES = os.getenv("USE_PROXIES", "true").lower() == "true"


def get_proxies():
    """Get a random proxy from the proxy list, or None if proxies are disabled"""
    if not USE_PROXIES:
        print("Proxies disabled via USE_PROXIES environment variable")
        return None

    if not PROXY_LIST:
        print("No proxies configured. Set BRIGHT_DATA_PROXY_LIST environment variable.")
        return None

    proxy = random.choice(PROXY_LIST)
    # Only show host:port, not credentials for security
    proxy_parts = proxy.split('@')
    if len(proxy_parts) > 1:
        print(f"Using proxy: {proxy_parts[1]}")
    else:
        print(f"Using proxy: {proxy}")
    return {"http": f"http://{proxy}", "https": f"http://{proxy}"}


def get_timeout_for_request(use_proxy=True):
    """Get appropriate timeout based on whether proxy is being used"""
    if use_proxy:
        return PROXY_TIMEOUT_SECONDS
    return RETRY_TIMEOUT_SECONDS
