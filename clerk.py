"""
clerk.py

last updated: 2025-07-12

Provides a ClerkClient class for Clerk functionality

Scope
All clerk functionality

todo
- clean up get_all_organizations loop to get all organizations a better way
- add testing
- add google style docs for doc generation
"""

# external
import os
from dotenv import load_dotenv
from typing import Dict, Any, Optional
from datetime import datetime

# internal
from deps.settings import get_proxies
from deps.utils import request_with_retries

load_dotenv(override=True)


class ClerkClient:
    def __init__(self, api_key: Optional[str] = None, type: Optional[str] = None):
        """
        Initialize the Clerk client with API key from env or directly provided
        """
        if type == "dev":
            self.api_key = api_key or os.getenv("CLERK_SECRET_KEY_DEV")
        elif type == "prod":
            self.api_key = api_key or os.getenv("CLERK_SECRET_KEY_PROD")
        else:
            self.api_key = api_key or os.getenv("CLERK_SECRET_KEY")
        if not self.api_key:
            raise ValueError("CLERK_SECRET_KEY not found in environment variables")

        self.base_url = "https://api.clerk.com/v1"
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

    def get_organization_by_slug(self, slug: str) -> Dict[str, Any]:
        """
        Find an organization by its slug

        Args:
            slug: The organization slug

        Returns:
            Organization data as a dictionary
        """
        url = f"{self.base_url}/organizations/{slug}"
        response = request_with_retries(
            "get",
            url,
            headers=self.headers,
            proxies=get_proxies(),
            timeout=10,
            skip_retry_on_404=True,
        )
        response.raise_for_status()
        return response.json()

    def get_organization_metadata(self, slug: str) -> Dict[str, Any]:
        """
        Get organization metadata by slug

        Args:
            slug: The organization slug

        Returns:
            Organization metadata as a dictionary
        """
        org_data = self.get_organization_by_slug(slug)
        return org_data.get("public_metadata", {})

    def update_organization_metadata(
        self, slug: str, metadata_updates: Dict[str, Any], org_id: str
    ) -> Dict[str, Any]:
        """
        Update specific fields in an organization's metadata without affecting existing metadata

        Args:
            slug: The organization slug
            metadata_updates: Dictionary containing the fields to update

        Returns:
            Updated organization data as a dictionary
        """

        url = f"{self.base_url}/organizations/{org_id}/metadata"
        payload = {"public_metadata": metadata_updates}

        response = request_with_retries(
            "patch",
            url,
            headers=self.headers,
            json=payload,
            proxies=get_proxies(),
            timeout=10,
            skip_retry_on_404=True,
        )
        response.raise_for_status()
        return response.json()

    def update_community_scrape_times(
        self,
        org_slug: str,
        org_id: str,
        last_full_scrape: Optional[datetime] = None,
        last_full_scrape_time_to_complete: Optional[str] = None,
        last_quick_scrape: Optional[datetime] = None,
        last_quick_scrape_time_to_complete: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Update scrape time information in an organization's metadata

        Args:
            org_slug: The organization slug
            last_full_scrape: Datetime of the last full scrape
            last_full_scrape_time_to_complete: Time it took to complete the last full scrape
            last_quick_scrape: Datetime of the last quick scrape
            last_quick_scrape_time_to_complete: Time it took to complete the last quick scrape

        Returns:
            Updated organization data
        """
        # Build updates dictionary with only the provided fields
        metadata_updates = {}

        if last_full_scrape is not None:
            metadata_updates["last_full_scrape"] = last_full_scrape.isoformat()

        if last_full_scrape_time_to_complete is not None:
            metadata_updates["last_full_scrape_time_to_complete"] = (
                last_full_scrape_time_to_complete
            )

        if last_quick_scrape is not None:
            metadata_updates["last_quick_scrape"] = last_quick_scrape.isoformat()

        if last_quick_scrape_time_to_complete is not None:
            metadata_updates["last_quick_scrape_time_to_complete"] = (
                last_quick_scrape_time_to_complete
            )

        # Only update if we have changes to make
        if metadata_updates:
            return self.update_organization_metadata(org_slug, metadata_updates, org_id)

        # Return current org data if no updates
        return self.get_organization_by_slug(org_slug)

    def get_all_organizations(self, limit: int = 500) -> list:
        """
        Fetch all organizations from Clerk, paginating through results 500 at a time

        Args:
            limit: Number of organizations to fetch per request (max 500)

        Returns:
            List of all organization objects
        """
        all_organizations = []
        offset = 0

        while True:
            # Make request with pagination
            url = f"{self.base_url}/organizations?limit={limit}&offset={offset}"
            response = request_with_retries(
                "get",
                url,
                headers=self.headers,
                proxies=get_proxies(),
                timeout=10,
                skip_retry_on_404=True,
            )
            response.raise_for_status()

            # Parse response data
            data = response.json()
            organizations = data.get("data", [])

            # Add organizations to our result list
            all_organizations.extend(organizations)

            # Check if we've fetched all organizations
            if len(organizations) < limit:
                # We've received fewer organizations than the limit, so we've reached the end
                break

            # Update offset for next page
            offset += limit

        return all_organizations

    def get_users_details_with_org(self, user_ids: list) -> Dict[str, Any]:
        """
        Fetch user details (avatar, first name, last name) and their organization/team name for a list of user_ids.
        Returns a dict mapping user_id to a dict with keys: first_name, last_name, profile_image_url, organizations (list of org/team names).
        Only includes user_ids that were requested. Fetches each user individually to avoid duplicate requests.
        """
        user_ids_set = set(user_ids)
        user_details = {}
        for user_id in user_ids_set:
            # Fetch user details
            url = f"{self.base_url}/users/{user_id}"
            response = request_with_retries(
                "get",
                url,
                headers=self.headers,
                proxies=get_proxies(),
                timeout=10,
                skip_retry_on_404=True,
            )
            if response.status_code == 404:
                continue  # Skip users not found
            response.raise_for_status()
            user = response.json()
            user_details[user_id] = {
                "first_name": user.get("first_name"),
                "last_name": user.get("last_name"),
                "profile_image_url": user.get("profile_image_url"),
                "organizations": [],  # Will fill below
            }
        # Now fetch org memberships for each user
        for user_id in user_details.keys():
            url = f"{self.base_url}/users/{user_id}/organization_memberships"
            response = request_with_retries(
                "get",
                url,
                headers=self.headers,
                proxies=get_proxies(),
                timeout=10,
                skip_retry_on_404=True,
            )
            if response.status_code == 404:
                continue  # Skip if memberships not found
            response.raise_for_status()
            orgs = response.json().get("data", [])
            org_names = [
                org["organization"].get("name")
                for org in orgs
                if org.get("organization")
            ]
            user_details[user_id]["organizations"] = org_names
        return user_details

    def get_organization_by_id(self, org_id: str) -> dict:
        """
        Find an organization by its ID and include its public_metadata.
        Args:
            org_id: The organization ID
        Returns:
            Organization data as a dictionary, with 'public_metadata' key, or None if not found
        """
        organizations = self.get_all_organizations()
        # Create a dictionary for O(1) lookup instead of O(n) iteration
        org_dict = {org.get("id"): org for org in organizations}

        org = org_dict.get(org_id)
        if org is None:
            return None

        # Try to get metadata from org object, fallback to fetching by slug if needed
        metadata = org.get("public_metadata")
        if metadata is None:
            # Try to fetch by slug if available
            slug = org.get("slug")
            if slug:
                metadata = self.get_organization_metadata(slug)
        org["public_metadata"] = metadata or {}
        return org


if __name__ == "__main__":
    pass
