"""
community.py

last updated: 2025-07-12

todo
- add google style docs for doc generation
- add unit tests
- decouple supabase
- define scope for this file
- plan database better for community objects
- (store all data and use skool ids for primary keys)
"""

# external
import json
import time
from datetime import date, datetime

# internal
from deps.settings import get_proxies, delay
from deps.utils import request_with_retries


def get_billing_dashboard(group_id: str, auth_token: str):
    print(f"\n=== Fetching billing dashboard data for group {group_id[:8]} ===")
    url = f"https://api.skool.com/groups/{group_id}/billing-dashboard?vs=1"
    headers = {
        "accept": "*/*",
        "accept-language": "en-US,en;q=0.9",
        "priority": "u=1, i",
        "sec-ch-ua": '"Chromium";v="134", "Not:A-Brand";v="24", "Google Chrome";v="134"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
        "x-nextjs-data": "1",
    }
    cookies = {"auth_token": auth_token}
    print(f"Sending API request to: {url}")
    time.sleep(delay)
    response = request_with_retries(
        "get",
        url,
        headers=headers,
        cookies=cookies,
        proxies=get_proxies(),
        timeout=10,
        # skip because it means we dont have admin in this community yet
        skip_retry_on_401=True,
    )

    if response and response.status_code == 200:
        data = response.json()
        print(f"Success! Received dashboard data")
        print(f"Saving response data to get_billing_dashboard.json")
        # with open("get_billing_dashboard.json", "w") as f:
        #     json.dump(data, f)
        print(f"Dashboard data successfully saved")
        return data
    else:
        if response:
            print(f"ERROR: API request failed with status code {response.status_code}")
            print(f"Response: {response.text}")
        else:
            print(f"ERROR: API request failed after retries.")
        raise Exception(
            f"API request failed with status code {response.status_code if response else 'N/A'}: {response.text if response else 'No response'}"
        )


def get_community_details(group_slug: str, build_id: str):
    print(f"\n=== Fetching community details for group '{group_slug}' ===")
    url = f"https://www.skool.com/_next/data/{build_id}/{group_slug}/about.json?group={group_slug}"

    headers = {
        "accept": "*/*",
        "accept-language": "en-US,en;q=0.9",
        "priority": "u=1, i",
        "sec-ch-ua": '"Chromium";v="134", "Not:A-Brand";v="24", "Google Chrome";v="134"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
        "x-nextjs-data": "1",
    }

    print(f"Sending API request to: {url}")
    print(f"Waiting {delay} seconds before request...")
    time.sleep(delay)
    response = request_with_retries(
        "get", url, headers=headers, proxies=get_proxies(), timeout=10
    )

    if response and response.status_code == 200:
        data = response.json()
        print(f"Success! Received community details data")

        # Extract group ID for logging
        try:
            group_id = data["pageProps"]["currentGroup"]["id"]
            print(f"Group ID: {group_id}")
        except KeyError:
            print("Warning: Could not extract group ID from response")
            # Write group_slug to a file for manual update, only if not already present
            filename = "group_slugs_needing_manual_update.txt"
            try:
                with open(filename, "r") as f:
                    existing_slugs = set(line.strip() for line in f)
            except FileNotFoundError:
                existing_slugs = set()
            if group_slug not in existing_slugs:
                with open(filename, "a") as f:
                    f.write(group_slug + "\n")

        # print(f"Saving response data to get_community_details.json")
        # with open("get_community_details.json", "w") as f:
        #     json.dump(data, f)
        print(f"Community details successfully saved")
        return data
    else:
        if response:
            print(f"ERROR: API request failed with status code {response.status_code}")
            print(f"Response: {response.text}")
        else:
            print(f"ERROR: API request failed after retries.")
        raise Exception(
            f"API request failed with status code {response.status_code if response else 'N/A'}: {response.text if response else 'No response'}"
        )


def update_dashboard_data_in_db(
    supabase, group_id, group_slug, auth_token, members_count
):
    """
    Fetch dashboard data from the API and update it in the database
    """
    print(f"\n=== Updating dashboard data in database for group '{group_slug}' ===")
    print(f"Fetching billing dashboard data from API...")
    dashboard_data = get_billing_dashboard(group_id, auth_token)

    # Extract metrics
    print(f"Extracting metrics from dashboard data...")
    churn_rate = dashboard_data.get("churn_rate", 0)
    total_about_page_visitors = dashboard_data.get("total_about_page_visitors", 0)
    total_sign_ups = dashboard_data.get("total_sign_ups", 0)
    conversion_rate = (
        total_sign_ups / total_about_page_visitors if total_about_page_visitors else 0
    )

    # Convert decimal rates to full percentage values (0.2 -> 20)
    churn_rate_percentage = churn_rate * 100
    conversion_rate_percentage = conversion_rate * 100

    print(f"  - Churn rate: {churn_rate:.2%} ({churn_rate_percentage:.2f}%)")
    print(f"  - About page visitors: {total_about_page_visitors}")
    print(f"  - Total sign ups: {total_sign_ups}")
    print(
        f"  - Conversion rate: {conversion_rate:.2%} ({conversion_rate_percentage:.2f}%)"
    )
    print(f"  - Members count: {members_count}")

    # Get current date for the record
    today = date.today().isoformat()
    print(f"Using date: {today}")

    # Prepare data for database
    print(f"Preparing dashboard record for database insertion...")
    dashboard_record = {
        "group_slug": group_slug,
        "date": today,
        "churn_rate": churn_rate_percentage,
        "conversion_rate": conversion_rate_percentage,
        "member_count": members_count,
        # updated_at will use default
    }

    # Insert or update dashboard data in database
    print(f"Executing upsert to 'scraped_dashboard' table...")
    try:
        result = (
            supabase.table("scraped_dashboard")
            .upsert(dashboard_record, on_conflict=("group_slug, date"))
            .execute()
        )
        print(f"Successfully updated dashboard data in database")
        print(f"=== Dashboard update complete ===\n")
        return dashboard_record
    except Exception as e:
        print(f"ERROR: Failed to update dashboard data in database: {e}")
        raise


def get_community_owner_slug(group_slug: str, build_id: str):
    """
    Fetch the community details and extract the owner's slug (name) from the metadata.
    Returns the owner's slug (name) as a string, or None if not found.
    """
    data = get_community_details(group_slug, build_id)
    try:
        owner_str = data["pageProps"]["currentGroup"]["metadata"]["owner"]
        owner_obj = json.loads(owner_str)
        owner_slug = owner_obj.get("name")
        print(f"Owner slug for group '{group_slug}': {owner_slug}")
        return owner_slug
    except (KeyError, json.JSONDecodeError, TypeError) as e:
        print(f"ERROR: Could not extract owner slug: {e}")
        return None


if __name__ == "__main__":
    pass
