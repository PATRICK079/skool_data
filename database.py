"""
database.py
last updated: 2025-07-12

todo
- add testing
- add google style docs for doc generation
- create class similar to clerk client for supabase commands
- add logic from other files here to decouple
"""

# external
import os
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv(dotenv_path=".env")


def connect_to_supabase() -> Client:
    """
    Connect to Supabase using the provided URL and API key.
    """
    url = os.getenv("SUPABASE_URL_TANGO")
    key = os.getenv("SUPABASE_KEY_TANGO")
    supabase: Client = create_client(url, key)
    return supabase
