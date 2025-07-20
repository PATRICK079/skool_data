"""
members.py

last updated: 2025-07-12

todo
- add unit tests
- add google style docs for doc generation
- decouple supabase logic
- define scope for this file
- database modeling for members
"""

# external
import json
from datetime import datetime
import time
from requests.exceptions import ProxyError, Timeout, ConnectionError

# internal
from deps.utils import ensure_iso, request_with_retries
from deps.settings import get_proxies, delay


def is_admin_in_group(community_slug, auth_token, build_id, admin_skool_handle):
    url = f"https://www.skool.com/_next/data/{build_id}/{community_slug}/-/search.json?q={admin_skool_handle}&t=members"
    print(f"Preparing request to: {url}")

    headers = {
        "accept": "*/*",
        "accept-language": "en-US,en;q=0.9",
        "priority": "u=1, i",
        "referer": "",
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

    print(f"(is_admin_in_group) Sending API request...")
    time.sleep(delay)
    response = request_with_retries(
        "get", url, headers=headers, cookies=cookies, proxies=get_proxies(), timeout=10
    )

    if response is None or response.status_code != 200:
        print(f"Error: API request failed after retries.")
        if response is not None:
            print(f"Status code: {response.status_code}")
            print(f"Response: {response.text}")
        return False

    print(f"Success! Request returned status code {response.status_code}")
    data = response.json()
    # pritn the data with indent
    print(f"Here is the data:")
    print(json.dumps(data, indent=4))

    search_results = (
        data.get("pageProps", {})
        .get("renderData", {})
        .get("members", {})
        .get("members", [])
    )
    print(f"[debug] Found {len(search_results)} members in the search results")
    for member in search_results:
        role = member.get("role")
        if role == "group-admin":
            if member.get("user", {}).get("name") == admin_skool_handle:
                print(
                    f"We are an admin because the role is {role} for @{admin_skool_handle} in group {community_slug}"
                )
                return True
    print(
        f"We are not an admin: @{admin_skool_handle} does not have a role of `group-admin` in group {community_slug}"
    )
    return False


def add_any_new_members_from_scraped_to_crm(supabase, group_slug):
    """
    Adds new members from the scraped_members table to the crm_members table
    Checks for members that exist in the scraped table but not in the CRM table

    Args:
        supabase: Supabase client object
        group_slug (str): Group slug identifier

    Returns:
        dict: Result with success status, message, and count of added members
    """
    try:
        print(
            f"\n=== Adding new members from scraped table to CRM for group {group_slug} ==="
        )

        # Get all scraped members for the group
        scraped_members = get_all_members_from_db_scraped(supabase, group_slug)

        if not scraped_members:
            print("No scraped members found for this group")
            return {
                "isSuccess": False,
                "message": "No scraped members found for this group",
                "data": {"added": 0},
            }

        # Get all existing CRM members for this group
        print(f"Fetching existing CRM members for group {group_slug}...")
        existing_crm_members = get_all_members_from_db_crm(supabase, group_slug)
        print(f"Found {len(existing_crm_members)} existing CRM members")

        # Get the IDs of existing CRM members
        existing_user_ids = [member["user_id"] for member in existing_crm_members]

        # Filter scraped members to only include those not already in CRM
        new_members = [
            member
            for member in scraped_members
            if member["id"] not in existing_user_ids
        ]

        if not new_members:
            print("No new members to add to CRM")
            return {
                "isSuccess": True,
                "message": "No new members to add",
                "data": {"added": 0},
            }

        print(f"Found {len(new_members)} new members to add to CRM")

        # Transform scraped members to CRM members format
        crm_members_to_insert = []
        for member in new_members:
            crm_member = {
                "user_id": member["id"],
                "team_slug": group_slug,
                "user_slug": member["name"],
                "first_name": member.get("first_name"),
                "last_name": member.get("last_name"),
                "profile_picture": member.get("profile_picture"),
                "tags": [],
                "assigned_team_members": [],
                "is_locked": False,
                "approved_at": member.get("approved_at"),
                "invited_by": member.get("invited_by"),
                "created_at": datetime.now().isoformat(),
                "updated_at": datetime.now().isoformat(),
            }
            crm_members_to_insert.append(crm_member)

        # Insert members in batches of 100
        batch_size = 100
        total_inserted = 0

        for i in range(0, len(crm_members_to_insert), batch_size):
            batch = crm_members_to_insert[i : i + batch_size]
            print(
                f"Inserting batch {i//batch_size + 1} of {(len(crm_members_to_insert) + batch_size - 1)//batch_size} ({len(batch)} members)..."
            )

            try:
                response = supabase.table("crm_members").insert(batch).execute()
                total_inserted += len(batch)
                print(f"Successfully inserted batch of {len(batch)} members")
            except Exception as e:
                print(f"Error inserting batch: {e}")
                # Continue with next batch even if this one fails
                continue

        print(f"Successfully added {total_inserted} members to CRM")
        return {
            "isSuccess": True,
            "message": f"Successfully added {total_inserted} members to CRM",
            "data": {"added": total_inserted},
        }

    except Exception as e:
        print(f"Error adding members from scrape: {e}")
        raise e


def get_members_on_page(
    group_slug: str,
    build_id: str,
    page: int = 1,
    auth_token: str = None,
    tab: str = "active",
):
    print(f"\n=== Fetching members for group {group_slug} on page {page} ===")
    url = f"https://www.skool.com/_next/data/{build_id}/{group_slug}/-/members.json?group={group_slug}&p={page}&t={tab}"
    print(f"Preparing request to: {url}")

    headers = {
        "accept": "*/*",
        "accept-language": "en-US,en;q=0.9",
        "priority": "u=1, i",
        "referer": "https://www.skool.com/garretts-group-7439/-/members",
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

    print(f"(get_members_on_page) Sending API request...")
    time.sleep(delay)
    response = request_with_retries(
        "get", url, headers=headers, cookies=cookies, proxies=get_proxies(), timeout=10
    )
    if response is None or response.status_code != 200:
        print(f"Error: API request failed after retries.")
        if response is not None:
            print(f"Status code: {response.status_code}")
            print(f"Response: {response.text}")
        return None
    print(f"Success! Request returned status code {response.status_code}")
    data = response.json()
    member_count = len(data.get("pageProps", {}).get("users", []))
    print(f"Found {member_count} members on page {page}")
    return data


def get_member_details(member_id: str, group_id: str, auth_token: str):
    print(
        f"\n=== Fetching details for member {member_id[:8]} in group {group_id[:8]} ==="
    )
    url = f"https://api.skool.com/users/{member_id}/preview?g={group_id}"

    print(f"Preparing request to: {url}")
    headers = {
        "accept": "*/*",
        "accept-language": "en-US,en;q=0.9",
        "content-type": "application/json",
        "origin": "https://www.skool.com",
        "priority": "u=1, i",
        "referer": "https://www.skool.com/",
        "sec-ch-ua": '"Chromium";v="134", "Not:A-Brand";v="24", "Google Chrome";v="134"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    }

    cookies = {"auth_token": auth_token}

    print(f"(get_member_details) Sending API request...")
    time.sleep(delay)
    response = request_with_retries(
        "get", url, headers=headers, cookies=cookies, proxies=get_proxies(), timeout=10
    )

    if response is None or response.status_code != 200:
        print(f"Error: API request failed after retries.")
        if response is not None:
            print(f"Status code: {response.status_code}")
            print(f"Response: {response.text}")
        return None

    print(f"Success! Request returned status code {response.status_code}")
    data = response.json()

    # # Save response for debugging
    # with open(f"get_member_details.json", "w") as f:
    #     json.dump(data, f)
    # print(f"Saved response data to get_member_details.json")

    return data


def get_all_members_from_db_crm(supabase, group_slug=None):
    """
    Retrieve all members from the database with pagination

    Args:
        supabase: Supabase client object
        group_slug (str, optional): Group slug to filter by

    Returns:
        List of member records
    """
    try:
        print(
            f"Fetching members from database{' for group '+group_slug if group_slug else ''}..."
        )
        all_members = []
        page = 0
        page_size = 1000

        while True:
            query = (
                supabase.table("crm_members")
                .select("*")
                .range(page * page_size, (page + 1) * page_size)
            )
            if group_slug:
                query = query.eq("team_slug", group_slug)
            response = query.execute()

            if not response.data:
                break

            all_members.extend(response.data)
            print(f"Fetched page {page + 1}, total members so far: {len(all_members)}")

            if len(response.data) < page_size:
                break

            page += 1

        print(
            f"Found {len(all_members)} total members in database under the crm_members table"
        )
        return all_members
    except Exception as e:
        print(f"(crm_members) Error fetching members from database: {e}")
        return []


def get_all_members_from_db_scraped(supabase, group_slug=None):
    """
    Retrieve all members from the database with pagination

    Args:
        supabase: Supabase client object
        group_slug (str, optional): Group slug to filter by

    Returns:
        List of member records
    """
    try:
        print(
            f"Fetching members from database{' for group '+group_slug if group_slug else ''}..."
        )
        all_members = []
        page = 0
        page_size = 1000

        while True:
            query = (
                supabase.table("scraped_members")
                .select("*")
                .range(page * page_size, (page + 1) * page_size)
            )
            if group_slug:
                query = query.eq("group_slug", group_slug)
            response = query.execute()

            if not response.data:
                break

            all_members.extend(response.data)
            print(f"Fetched page {page + 1}, total members so far: {len(all_members)}")

            if len(response.data) < page_size:
                break

            page += 1

        print(
            f"Found {len(all_members)} total members in database under the scraped_members table"
        )
        return all_members
    except Exception as e:
        print(f"(scraped_members) Error fetching members from database: {e}")
        return []


def add_member_to_db(
    supabase,
    id,
    name,
    group_slug,
    last_offline=None,
    first_name=None,
    last_name=None,
    requested_at=None,
    group_id=None,
    role=None,
    approved_at=None,
    profile_picture=None,
    invited_by=None,
):
    """
    Add a member to the database

    Args:
        supabase: Supabase client object
        id (str): Member ID (primary key)
        name (str): Member's full name
        group_slug (str): Group slug identifier
        created_at (datetime): Creation timestamp
        updated_at (datetime): Last update timestamp
        last_offline (datetime, optional): Last time member was offline
        first_name (str, optional): Member's first name
        last_name (str, optional): Member's last name
        requested_at (datetime, optional): When member requested to join
        group_id (str, optional): Group ID
        role (str, optional): Member's role in group
        approved_at (datetime, optional): When member was approved
        profile_picture (str, optional): URL to profile picture

    Returns:
        The inserted member data
    """
    try:
        print(f"Adding member {id[:8]} ({name}) to database...")

        member_data = {
            "id": id,
            "name": name,
            "group_slug": group_slug,
            "last_offline": last_offline,
            "first_name": first_name,
            "last_name": last_name,
            "requested_at": requested_at,
            "group_id": group_id,
            "role": role,
            "approved_at": approved_at,
            "profile_picture": profile_picture,
            "invited_by": invited_by,
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat(),
        }

        print(f"Preparing member data with fields: {', '.join(member_data.keys())}")

        # Remove None values to avoid inserting null for columns that should be omitted
        member_data = {k: v for k, v in member_data.items() if v is not None}
        print(f"Removed null fields, final field count: {len(member_data)}")

        print(f"Executing insert into 'members' table...")
        response = supabase.table("scraped_members").insert(member_data).execute()
        print(f"Successfully added member {id[:8]} ({name}) to database")
        return response.data
    except Exception as e:
        print(f"Error adding member to database: {e}")
        return None


def add_members_from_api_to_db(supabase, group_slug, build_id, auth_token):
    """
    Sync members from the Skool API to the database

    Args:
        supabase: Supabase client object
        group_slug (str): Group slug identifier
        build_id (str): Build ID for the Skool API
        auth_token (str): Authentication token for the Skool API

    Returns:
        int: Number of new members added to the database
    """
    print(f"\n=== Starting member sync for group {group_slug} ===")

    # Get all members from our DB (scraped_members)
    # we are checking agaist the last member scrape, not crm list.
    members_in_db = get_all_members_from_db_scraped(
        supabase=supabase, group_slug=group_slug
    )
    print(
        f"Found {len(members_in_db)} existing members in database for group {group_slug}"
    )

    new_members_count = 0
    page = 1
    total_members_processed = 0

    while True:
        print(f"\n--- Processing page {page} of members ---")
        members_on_page_data = get_members_on_page(
            group_slug=group_slug, build_id=build_id, page=page, auth_token=auth_token
        )

        if not members_on_page_data:
            print(f"Error fetching page {page} or no data returned")
            break

        members_on_page = members_on_page_data.get("pageProps", {}).get("users", [])
        print(f"Found {len(members_on_page)} members on page {page}")

        if not members_on_page:
            print(f"No members found on page {page}, ending sync")
            break

        new_members_found = False
        for i, member in enumerate(members_on_page):
            total_members_processed += 1
            member_id = member.get("id")
            member_name = member.get("name", "Unknown")

            print(
                f"[{i+1}/{len(members_on_page)}] Processing member {member_id[:8]} ({member_name})..."
            )

            if member_id not in [m["id"] for m in members_in_db]:
                new_members_found = True
                new_members_count += 1
                print(f"  - Member {member_id[:8]} not found in database, adding...")

                last_offline = None
                if "metadata" in member and "lastOffline" in member["metadata"]:
                    last_offline = ensure_iso(member["metadata"]["lastOffline"])
                    print(f"  - Last offline: {last_offline}")

                requested_at = None
                if (
                    "member" in member
                    and "metadata" in member.get("member", {})
                    and "requestedAt" in member.get("member", {}).get("metadata", {})
                ):
                    requested_at = ensure_iso(
                        member["member"]["metadata"]["requestedAt"]
                    )
                    print(f"  - Requested at: {requested_at}")

                invited_by = None
                # NOTE: do not reference the afl via the member > metadata. It is not correct!
                # ...instead, use aflUserData > userId
                if "aflUserData" in member and "userId" in member.get(
                    "aflUserData", {}
                ):
                    invited_by = member.get("aflUserData", {}).get("userId")
                    print(f"  - Invited by: {invited_by}")

                role = member.get("member", {}).get("role")
                print(f"  - Role: {role}")

                profile_picture = member.get("metadata", {}).get("pictureProfile")
                print(f"  - Has profile picture: {'Yes' if profile_picture else 'No'}")

                add_member_to_db(
                    supabase=supabase,
                    id=member_id,
                    name=member_name,
                    group_slug=group_slug,
                    # more reliable than member.member.lastOffline (because sometimes its not there)
                    last_offline=last_offline,
                    first_name=member.get("firstName"),
                    last_name=member.get("lastName"),
                    requested_at=requested_at,
                    role=role,
                    approved_at=member.get("member", {}).get("approvedAt"),
                    profile_picture=profile_picture,
                    invited_by=invited_by,
                )
            else:
                print(
                    f"  - Member {member_id[:8]} already exists in database, skipping"
                )

        # If no more members on this page, or no new members found, break out of the loop
        if not members_on_page or not new_members_found:
            print(
                f"{'No more members' if not members_on_page else 'No new members'} on page {page}, breaking out of the loop"
            )
            break

        # Increment page number for next iteration
        page += 1

    print(f"\n=== Finished member sync ===")
    print(f"Processed {total_members_processed} members across {page} pages")
    print(f"Added {new_members_count} new members to database")
    return new_members_count


def sync_churned_status(supabase, group_slug: str, build_id: str, auth_token: str):
    """
    Syncs the churned status of all members in the CRM by scraping the churned tab
    and updating the database accordingly.

    Args:
        supabase: Supabase client object
        group_slug (str): Group slug identifier
        build_id (str): Build ID for the Skool API
        auth_token (str): Authentication token for the Skool API

    Returns:
        dict: Stats about the sync operation
    """
    print(f"\n=== Syncing churned status for group {group_slug} ===")

    # Get all CRM members for this group
    crm_members = get_all_members_from_db_crm(supabase, group_slug)
    crm_member_ids = {member["user_id"] for member in crm_members}

    if not crm_members:
        print("No CRM members found for this group")
        return {
            "total_crm_members": 0,
            "members_marked_churned": 0,
            "members_marked_active": 0,
            "new_churned_members_added": 0,
        }

    # Get first page to determine total churned members
    print("\n--- Fetching first page to determine total churned members ---")
    first_page_data = get_members_on_page(
        group_slug=group_slug,
        build_id=build_id,
        page=1,
        auth_token=auth_token,
        tab="churned",
    )

    if not first_page_data:
        print("Could not fetch first page of churned members")
        return {
            "total_crm_members": len(crm_members),
            "members_marked_churned": 0,
            "members_marked_active": 0,
            "new_churned_members_added": 0,
        }

    total_churned_members = (
        first_page_data.get("pageProps", {})
        .get("renderData", {})
        .get("totalChurnedMembers", 0)
    )
    members_per_page = 30  # Skool's standard page size
    total_pages = (total_churned_members + members_per_page - 1) // members_per_page

    print(f"Total churned members: {total_churned_members}")
    print(f"Expected total pages: {total_pages}")

    # Get all churned members from the API
    churned_member_ids = set()
    churned_members_data = []  # Store full member data for new additions
    members_from_first_page = first_page_data.get("pageProps", {}).get("users", [])
    for member in members_from_first_page:
        churned_member_ids.add(member["id"])
        if member["id"] not in crm_member_ids:
            churned_members_data.append(member)

    # Fetch remaining pages
    for page in range(2, total_pages + 1):
        print(f"\n--- Fetching churned members page {page}/{total_pages} ---")
        members_data = get_members_on_page(
            group_slug=group_slug,
            build_id=build_id,
            page=page,
            auth_token=auth_token,
            tab="churned",
        )

        if not members_data:
            print(f"No more churned members data found on page {page}")
            break

        members = members_data.get("pageProps", {}).get("users", [])
        if not members:
            print(f"No more churned members found on page {page}")
            break

        # Add member IDs to our set and store full data if needed
        for member in members:
            churned_member_ids.add(member["id"])
            if member["id"] not in crm_member_ids:
                churned_members_data.append(member)

        print(f"Found {len(members)} churned members on page {page}")

        # Safety check - if we've found all churned members, we can stop
        if len(churned_member_ids) >= total_churned_members:
            print(
                f"Found all {total_churned_members} churned members, stopping pagination"
            )
            break

    print(f"\nTotal churned members found: {len(churned_member_ids)}")

    # Add new churned members to CRM
    new_churned_members_added = 0
    for member in churned_members_data:
        try:
            crm_member = {
                "user_id": member["id"],
                "team_slug": group_slug,
                "user_slug": member["name"],
                "first_name": member.get("firstName"),
                "last_name": member.get("lastName"),
                "profile_picture": member.get("metadata", {}).get("pictureProfile"),
                "tags": [],
                "status": "churn_risk",
                "assigned_team_members": [],
                "is_locked": False,
                "is_churned": True,
                "approved_at": member.get("member", {}).get("approvedAt"),
                "invited_by": member.get("aflUserData", {}).get("userId"),
                "created_at": datetime.now().isoformat(),
                "updated_at": datetime.now().isoformat(),
            }

            print(f"Adding new churned member {member['id'][:8]} to CRM")
            supabase.table("crm_members").insert(crm_member).execute()
            new_churned_members_added += 1

        except Exception as e:
            print(f"Error adding new churned member {member['id'][:8]}: {e}")
            continue

    # Update existing CRM members' churned status
    members_marked_churned = 0
    members_marked_active = 0

    for member in crm_members:
        user_id = member["user_id"]
        current_churned_status = member.get("is_churned", False)
        should_be_churned = user_id in churned_member_ids

        if current_churned_status != should_be_churned:
            try:
                print(
                    f"Updating churned status for member {user_id[:8]} to {should_be_churned}"
                )
                supabase.table("crm_members").update(
                    {
                        "is_churned": should_be_churned,
                        "updated_at": datetime.now().isoformat(),
                    }
                ).eq("user_id", user_id).eq("team_slug", group_slug).execute()

                if should_be_churned:
                    members_marked_churned += 1
                else:
                    members_marked_active += 1

            except Exception as e:
                print(f"Error updating churned status for member {user_id[:8]}: {e}")
                continue

    stats = {
        "total_crm_members": len(crm_members),
        "members_marked_churned": members_marked_churned,
        "members_marked_active": members_marked_active,
        "new_churned_members_added": new_churned_members_added,
    }

    print("\n=== Churned Status Sync Complete ===")
    print(f"Total CRM members processed: {stats['total_crm_members']}")
    print(f"Members marked as churned: {stats['members_marked_churned']}")
    print(f"Members marked as active: {stats['members_marked_active']}")
    print(f"New churned members added: {stats['new_churned_members_added']}")

    return stats


def get_all_members_for_community(
    group_slug, build_id, auth_token=None, tab=None, return_billing_products=False
):
    """
    Fetch all members for a community (active or churned) by paginating through all pages.
    Args:
        group_slug (str): Group slug identifier
        build_id (str): Build ID for the Skool API
        auth_token (str, optional): Authentication token for the Skool API
        tab (str, optional): Tab to fetch ('active', 'churned', or None for default)
    Returns:
        List[dict]: List of member dicts
    """
    members = []
    page = 1
    billing_products = []
    while True:
        data = get_members_on_page(
            group_slug=group_slug,
            build_id=build_id,
            page=page,
            auth_token=auth_token,
            tab=tab if tab else "active",
        )
        if not data:
            break
        users = data.get("pageProps", {}).get("users", [])

        try:
            membership_billing_products = data.get("pageProps", {}).get(
                "membershipBillingProducts"
            )
            if isinstance(membership_billing_products, list):
                billing_products.extend(membership_billing_products)
        except Exception as e:
            print(f"Error getting billing products: {e}")

        if not users:
            break
        members.extend(users)
        # Pagination: check if we've reached the last page
        if len(users) < 30:
            break
        page += 1

    if return_billing_products:
        return members, billing_products
    return members


def get_all_community_members_and_save(
    group_slug, build_id, auth_token=None, return_billing_products=False
):
    """
    Fetch all active and churned members for a community and save to a JSON file.
    Args:
        group_slug (str): Group slug identifier
        build_id (str): Build ID for the Skool API
        auth_token (str, optional): Authentication token for the Skool API
        output_file (str): Path to output JSON file
    Returns:
        List[dict]: Combined list of all members
    """
    print(f"Fetching all active members for {group_slug}")
    if return_billing_products:
        active_members, billing_products = get_all_members_for_community(
            group_slug,
            build_id,
            auth_token,
            tab=None,
            return_billing_products=return_billing_products,
        )
    else:
        active_members = get_all_members_for_community(
            group_slug,
            build_id,
            auth_token,
            tab=None,
            return_billing_products=return_billing_products,
        )
        billing_products = None

    print(f"Found {len(active_members)} active members")

    print(f"Fetching all churned members for {group_slug}")
    churned_members = get_all_members_for_community(
        group_slug, build_id, auth_token, tab="churned"
    )
    print(f"Found {len(churned_members)} churned members")

    all_members = active_members + churned_members
    print(f"Total members (active + churned): {len(all_members)}")

    # # save active and churned members to separate files
    # active_output_file = f"active_members_{group_slug}.json"
    # churned_output_file = f"churned_members_{group_slug}.json"

    # with open(active_output_file, "w") as f:
    #     json.dump(active_members, f, indent=2)

    # with open(churned_output_file, "w") as f:
    #     json.dump(churned_members, f, indent=2)

    # print(f"Saved all members to {active_output_file} and {churned_output_file}")

    if return_billing_products:
        return active_members, churned_members, billing_products

    return active_members, churned_members


def billing_products_to_dict(billing_products):
    """
    Takes a list of billing product objects and returns a dict mapping billing product IDs to
    {'interval': ..., 'price': ...} objects. Handles monthly, annual, and one-time products.
    Skips nulls or empty IDs.
    Args:
        billing_products (list): List of objects, each with keys like 'monthlyBillingProduct', 'annualBillingProduct', 'oneTimeBillingProduct',
                                and their corresponding IDs ('monthlyBpId', 'annualBpId', 'oneTimeBpId').
    Returns:
        dict: {billing_product_id: {'interval': ..., 'price': ...}}
    """
    result = {}
    for obj in billing_products:
        # Monthly
        monthly = obj.get("monthlyBillingProduct")
        monthly_id = obj.get("monthlyBpId")
        if monthly and monthly_id:
            result[monthly_id] = {
                "interval": monthly.get(
                    "recurringInterval", monthly.get("recurring_interval", "month")
                ),
                "price": monthly.get("amount"),
            }
        # Annual
        annual = obj.get("annualBillingProduct")
        annual_id = obj.get("annualBpId")
        if annual and annual_id:
            result[annual_id] = {
                "interval": annual.get(
                    "recurringInterval", annual.get("recurring_interval", "year")
                ),
                "price": annual.get("amount"),
            }
        # One-time
        one_time = obj.get("oneTimeBillingProduct")
        one_time_id = obj.get("oneTimeBpId")
        if one_time and one_time_id:
            result[one_time_id] = {
                "interval": one_time.get(
                    "recurringInterval", one_time.get("recurring_interval", "one_time")
                ),
                "price": one_time.get("amount"),
            }
    return result


def count_new_members_since_efficient(group_slug, build_id, auth_token, start_date):
    """
    Efficiently count and return new members who joined a community since a given start date (active tab only).
    Stops paginating once a member with .approvedAt before the start date is found.
    Args:
        group_slug (str): Group slug identifier
        build_id (str): Build ID for the Skool API
        auth_token (str): Authentication token for the Skool API
        start_date (str or datetime.date or datetime.datetime): Start date (inclusive) in 'YYYY-MM-DD' or datetime/date
    Returns:
        Tuple[int, List[dict]]: (count, list of new member dicts)
    """
    from datetime import datetime, date

    if isinstance(start_date, str):
        start_date_dt = datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%SZ").date()
    elif isinstance(start_date, datetime):
        start_date_dt = start_date.date()
    elif isinstance(start_date, date):
        start_date_dt = start_date
    else:
        raise ValueError(
            "start_date must be a string in YYYY-MM-DD format, datetime, or date object"
        )

    def parse_joined_at(member):
        approved_at = member.get("member", {}).get("approvedAt")
        if not approved_at:
            return None
        try:
            return datetime.fromisoformat(approved_at.replace("Z", "+00:00")).date()
        except Exception:
            pass
        try:
            ts = int(approved_at)
            if ts > 1e12:
                ts = ts / 1e9
            return datetime.utcfromtimestamp(ts).date()
        except Exception:
            return None

    new_members = []
    page = 1
    while True:
        data = get_members_on_page(
            group_slug=group_slug,
            build_id=build_id,
            page=page,
            auth_token=auth_token,
            tab="active",
        )
        if not data:
            break
        users = data.get("pageProps", {}).get("users", [])
        if not users:
            break
        stop = False
        for m in users:
            joined_at_dt = parse_joined_at(m)
            if joined_at_dt and joined_at_dt >= start_date_dt:
                new_members.append(m)
            else:
                stop = True
                break
        if stop or len(users) < 30:
            break
        page += 1
    return len(new_members), new_members


if __name__ == "__main__":
    pass
