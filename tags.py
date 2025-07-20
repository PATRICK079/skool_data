"""
tags.py

last updated: 2025-07-12

todo
- add unit tests
- add google style docs for doc generation
- decouple supabase logic
- define scope for this file
- database modeling for tags
"""

import datetime
import time
from collections import defaultdict


def get_all_members_for_group(supabase, group_slug):
    """
    Get all members for a specific group.

    Args:
        supabase: Supabase client
        group_slug: Group identifier

    Returns:
        List of all member records for the group
    """
    start_time = time.time()
    all_members = []
    page = 0
    page_size = 100

    while True:
        print(f"Fetching page {page+1} (offset: {page * page_size})")
        response = (
            supabase.table("crm_members")
            .select("*")
            .eq("team_slug", group_slug)
            .range(page * page_size, (page + 1) * page_size)
            .execute()
        )

        # Check if we have data
        if not response.data:
            break

        # Print the actual number of records retrieved
        curr_count = len(response.data)
        print(f"Retrieved {curr_count} records on page {page+1}")

        all_members.extend(response.data)
        page += 1

    print(
        f"TOTAL USERS CHECKED: {len(all_members)} in {time.time() - start_time:.2f} seconds"
    )
    return all_members


def get_lookback_date(lookback_days):
    """
    Calculate the ISO-formatted date for a lookback period.

    Args:
        lookback_days: Number of days to look back

    Returns:
        ISO-formatted date string for the lookback period
    """
    return (
        datetime.datetime.now() - datetime.timedelta(days=lookback_days)
    ).isoformat()


def get_users_with_post_count(
    supabase, group_slug, user_ids, lookback_days, min_posts=None, max_posts=None
):
    """
    Get users who have made a specific number of posts within a lookback period.

    Args:
        supabase: Supabase client
        group_slug: Group identifier
        user_ids: List of user IDs to check
        lookback_days: Number of days to analyze
        min_posts: Minimum number of posts required (default: None)
        max_posts: Maximum number of posts allowed (default: None)

    Returns:
        Dictionary with user_id as key and post count as value
    """
    start_time = time.time()
    lookback_date = get_lookback_date(lookback_days)

    posts_by_user = {}
    chunk_size = 20

    for i in range(0, len(user_ids), chunk_size):
        user_chunk = user_ids[i : i + chunk_size]
        page = 0
        while True:
            posts_response = (
                supabase.table("scraped_posts")
                .select("id, created_at, created_by")
                .in_("created_by", user_chunk)
                .eq("group_slug", group_slug)
                .gte("created_at", lookback_date)
                .range(page * 1000, (page + 1) * 1000)
                .execute()
            )

            if not posts_response.data:
                break

            # Count posts per user
            for post in posts_response.data:
                user = post["created_by"]
                if user not in posts_by_user:
                    posts_by_user[user] = 0
                posts_by_user[user] += 1

            page += 1
            if len(posts_response.data) < 1000:
                break

    # Apply filters if specified
    filtered_users = {}
    for user_id, count in posts_by_user.items():
        if (min_posts is None or count >= min_posts) and (
            max_posts is None or count <= max_posts
        ):
            filtered_users[user_id] = count

    print(f"Post count query completed in {time.time() - start_time:.2f} seconds")
    print(f"Found {len(filtered_users)} users matching post criteria")

    return filtered_users


def get_users_with_comment_count(
    supabase, group_slug, user_ids, lookback_days, min_comments=None, max_comments=None
):
    """
    Get users who have made a specific number of comments within a lookback period.

    Args:
        supabase: Supabase client
        group_slug: Group identifier
        user_ids: List of user IDs to check
        lookback_days: Number of days to analyze
        min_comments: Minimum number of comments required (default: None)
        max_comments: Maximum number of comments allowed (default: None)

    Returns:
        Dictionary with user_id as key and comment count as value
    """
    start_time = time.time()
    lookback_date = get_lookback_date(lookback_days)

    comments_by_user = {}
    chunk_size = 20

    for i in range(0, len(user_ids), chunk_size):
        user_chunk = user_ids[i : i + chunk_size]
        page = 0
        while True:
            comments_response = (
                supabase.table("scraped_comments")
                .select("id, created_at, created_by")
                .in_("created_by", user_chunk)
                .eq("group_slug", group_slug)
                .gte("created_at", lookback_date)
                .range(page * 1000, (page + 1) * 1000)
                .execute()
            )

            if not comments_response.data:
                break

            # Count comments per user
            for comment in comments_response.data:
                user = comment["created_by"]
                if user not in comments_by_user:
                    comments_by_user[user] = 0
                comments_by_user[user] += 1

            page += 1
            if len(comments_response.data) < 1000:
                break

    # Apply filters if specified
    filtered_users = {}
    for user_id, count in comments_by_user.items():
        if (min_comments is None or count >= min_comments) and (
            max_comments is None or count <= max_comments
        ):
            filtered_users[user_id] = count

    print(f"Comment count query completed in {time.time() - start_time:.2f} seconds")
    print(f"Found {len(filtered_users)} users matching comment criteria")

    return filtered_users


def get_users_with_activity_count(
    supabase, group_slug, user_ids, lookback_days, min_activity=None, max_activity=None
):
    """
    Get users with a specific level of combined activity (posts + comments) within a lookback period.

    Args:
        supabase: Supabase client
        group_slug: Group identifier
        user_ids: List of user IDs to check
        lookback_days: Number of days to analyze
        min_activity: Minimum total activity required (default: None)
        max_activity: Maximum total activity allowed (default: None)

    Returns:
        Dictionary with user_id as key and total activity count as value
    """
    # Get post counts
    posts_by_user = get_users_with_post_count(
        supabase, group_slug, user_ids, lookback_days
    )

    # Get comment counts
    comments_by_user = get_users_with_comment_count(
        supabase, group_slug, user_ids, lookback_days
    )

    # Combine activity
    activity_by_user = {}

    # If min_activity is 0, we need to consider all users, not just those with activity
    users_to_check = (
        user_ids
        if min_activity == 0
        else set(list(posts_by_user.keys()) + list(comments_by_user.keys()))
    )

    for user_id in users_to_check:
        post_count = posts_by_user.get(user_id, 0)
        comment_count = comments_by_user.get(user_id, 0)
        total_activity = post_count + comment_count

        if (min_activity is None or total_activity >= min_activity) and (
            max_activity is None or total_activity <= max_activity
        ):
            activity_by_user[user_id] = {
                "post_count": post_count,
                "comment_count": comment_count,
                "total_activity": total_activity,
            }

    print(f"Found {len(activity_by_user)} users matching combined activity criteria")
    return activity_by_user


def get_users_with_active_days(
    supabase,
    group_slug,
    user_ids,
    lookback_days,
    min_active_days=None,
    max_active_days=None,
):
    """
    Get users with a specific number of unique active days within a lookback period.

    Args:
        supabase: Supabase client
        group_slug: Group identifier
        user_ids: List of user IDs to check
        lookback_days: Number of days to analyze
        min_active_days: Minimum unique active days required (default: None)
        max_active_days: Maximum unique active days allowed (default: None)

    Returns:
        Dictionary with user_id as key and active days count as value
    """
    start_time = time.time()
    lookback_date = get_lookback_date(lookback_days)

    active_days_by_user = defaultdict(set)
    chunk_size = 20

    # Get active days from posts
    for i in range(0, len(user_ids), chunk_size):
        user_chunk = user_ids[i : i + chunk_size]
        page = 0
        while True:
            posts_response = (
                supabase.table("scraped_posts")
                .select("id, created_at, created_by")
                .in_("created_by", user_chunk)
                .eq("group_slug", group_slug)
                .gte("created_at", lookback_date)
                .range(page * 1000, (page + 1) * 1000)
                .execute()
            )

            if not posts_response.data:
                break

            # Track active days per user from posts
            for post in posts_response.data:
                user = post["created_by"]
                # Extract just the date portion (YYYY-MM-DD) for counting unique days
                post_date = post["created_at"].split("T")[0]
                active_days_by_user[user].add(post_date)

            page += 1
            if len(posts_response.data) < 1000:
                break

    # Get active days from comments
    for i in range(0, len(user_ids), chunk_size):
        user_chunk = user_ids[i : i + chunk_size]
        page = 0
        while True:
            comments_response = (
                supabase.table("scraped_comments")
                .select("id, created_at, created_by")
                .in_("created_by", user_chunk)
                .eq("group_slug", group_slug)
                .gte("created_at", lookback_date)
                .range(page * 1000, (page + 1) * 1000)
                .execute()
            )

            if not comments_response.data:
                break

            # Track active days per user from comments
            for comment in comments_response.data:
                user = comment["created_by"]
                # Extract just the date portion (YYYY-MM-DD) for counting unique days
                comment_date = comment["created_at"].split("T")[0]
                active_days_by_user[user].add(comment_date)

            page += 1
            if len(comments_response.data) < 1000:
                break

    # Apply filters if specified
    filtered_users = {}
    for user_id, active_days in active_days_by_user.items():
        days_count = len(active_days)
        if (min_active_days is None or days_count >= min_active_days) and (
            max_active_days is None or days_count <= max_active_days
        ):
            filtered_users[user_id] = days_count

    print(f"Active days query completed in {time.time() - start_time:.2f} seconds")
    print(f"Found {len(filtered_users)} users matching active days criteria")

    return filtered_users


def get_users_by_offline_days(
    supabase, group_slug, user_ids, min_days_offline=None, max_days_offline=None
):
    """
    Get users who have been offline for a specific number of days.

    Args:
        supabase: Supabase client
        group_slug: Group identifier
        user_ids: List of user IDs to check
        min_days_offline: Minimum days offline required (default: None)
        max_days_offline: Maximum days offline allowed (default: None)

    Returns:
        Dictionary with user_id as key and days offline as value
    """
    start_time = time.time()

    offline_by_user = {}
    chunk_size = 20

    for i in range(0, len(user_ids), chunk_size):
        user_chunk = user_ids[i : i + chunk_size]
        page = 0
        while True:
            members_response = (
                supabase.table("scraped_members")
                .select("id, last_offline")
                .in_("id", user_chunk)
                .eq("group_slug", group_slug)
                .range(page * 1000, (page + 1) * 1000)
                .execute()
            )

            if not members_response.data:
                break

            # Calculate days offline for each user
            for member_data in members_response.data:
                user = member_data["id"]
                if member_data.get("last_offline"):
                    last_offline_str = member_data["last_offline"]
                    last_offline = datetime.datetime.fromisoformat(
                        last_offline_str.replace("Z", "+00:00")
                    )
                    days_offline = (datetime.datetime.now() - last_offline).days
                    offline_by_user[user] = days_offline

            page += 1
            if len(members_response.data) < 1000:
                break

    # Apply filters if specified
    filtered_users = {}
    for user_id, days_offline in offline_by_user.items():
        if (min_days_offline is None or days_offline >= min_days_offline) and (
            max_days_offline is None or days_offline <= max_days_offline
        ):
            filtered_users[user_id] = days_offline

    print(f"Offline days query completed in {time.time() - start_time:.2f} seconds")
    print(f"Found {len(filtered_users)} users matching offline days criteria")

    return filtered_users


def get_users_by_membership_days(
    supabase, group_slug, user_ids, min_days=None, max_days=None
):
    """
    Get users who have been members for a specific number of days.

    Args:
        supabase: Supabase client
        group_slug: Group identifier
        user_ids: List of user IDs to check
        min_days: Minimum days of membership required (default: None)
        max_days: Maximum days of membership allowed (default: None)

    Returns:
        Dictionary with user_id as key and days of membership as value
    """
    start_time = time.time()

    membership_days_by_user = {}
    chunk_size = 20

    for i in range(0, len(user_ids), chunk_size):
        user_chunk = user_ids[i : i + chunk_size]
        page = 0
        while True:
            members_response = (
                supabase.table("scraped_members")
                .select("id, approved_at")
                .in_("id", user_chunk)
                .eq("group_slug", group_slug)
                .range(page * 1000, (page + 1) * 1000)
                .execute()
            )

            if not members_response.data:
                break

            # Calculate membership days for each user
            for member_data in members_response.data:
                user = member_data["id"]
                if member_data.get("approved_at"):
                    approved_at_str = member_data["approved_at"]
                    approved_at = datetime.datetime.fromisoformat(
                        approved_at_str.replace("Z", "+00:00")
                    )
                    days_member = (datetime.datetime.now() - approved_at).days
                    membership_days_by_user[user] = days_member

            page += 1
            if len(members_response.data) < 1000:
                break

    # Apply filters if specified
    filtered_users = {}
    for user_id, days_member in membership_days_by_user.items():
        if (min_days is None or days_member >= min_days) and (
            max_days is None or days_member <= max_days
        ):
            filtered_users[user_id] = days_member

    print(f"Membership days query completed in {time.time() - start_time:.2f} seconds")
    print(f"Found {len(filtered_users)} users matching membership days criteria")

    return filtered_users


def get_user_id_from_slug(supabase, user_slug, group_slug):
    """
    Get a user's ID from their slug by looking up in the scraped_members table.

    Args:
        supabase: Supabase client
        user_slug: User slug to look up
        group_slug: Group identifier

    Returns:
        The user ID if found, None otherwise
    """
    start_time = time.time()
    print(f"Looking up user ID for slug '{user_slug}' in group '{group_slug}'")

    # Query the scraped_members table where name equals user_slug
    response = (
        supabase.table("scraped_members")
        .select("id")
        .eq("name", user_slug)
        .eq("group_slug", group_slug)
        .execute()
    )

    execution_time = time.time() - start_time
    print(f"Query completed in {execution_time:.2f} seconds")

    if not response.data:
        print(f"No user found with slug '{user_slug}' in group '{group_slug}'")
        return None

    user_id = response.data[0]["id"]
    print(f"User ID found: {user_id}")

    return user_id


def apply_tag_to_users(supabase, group_slug, user_ids, tag_name):
    """
    Apply a tag to specified users while preserving their existing tags.

    Args:
        supabase: Supabase client
        group_slug: Group identifier
        user_ids: List of user IDs to apply the tag to
        tag_name: The tag to apply

    Returns:
        Dictionary with counts of users updated and skipped
    """
    if len(user_ids) == 0:
        print("No users to apply tag to")
        return {
            "tag_name": tag_name,
            "total_users": 0,
            "successful_updates": 0,
            "already_tagged": 0,
            "failed_updates": 0,
            "locked_users": 0,
            "execution_time": 0,
        }

    # NOTE: tags are all lowercase, and use _ as spaces.
    # ie: "Need_Activation" -> "need activation"
    tag_name = tag_name.replace(" ", "_").lower()
    print(f"Formatted tag name: '{tag_name}'")

    start_time = time.time()
    print(f"Starting tag application process at {time.strftime('%Y-%m-%d %H:%M:%S')}")

    # Track statistics
    successful_updates = 0
    already_tagged = 0
    failed_updates = 0
    locked_users = 0

    # Process users in batches to avoid overloading the API
    batch_size = 20
    total_batches = (len(user_ids) + batch_size - 1) // batch_size

    print(f"Applying tag '{tag_name}' to {len(user_ids)} users in group '{group_slug}'")
    print(f"Processing in {total_batches} batches of up to {batch_size} users each")

    for i in range(0, len(user_ids), batch_size):
        batch_num = i // batch_size + 1
        batch_user_ids = user_ids[i : i + batch_size]
        batch_start_time = time.time()

        print(
            f"\nBatch {batch_num}/{total_batches}: Processing {len(batch_user_ids)} users"
        )
        print(
            f"Batch user IDs: {', '.join(batch_user_ids[:5])}{' ...' if len(batch_user_ids) > 5 else ''}"
        )

        # Get current tags and isLocked status for each user in batch
        print(f"Fetching current tags and lock status for batch {batch_num}")
        members_response = (
            supabase.table("crm_members")
            .select("user_id, tags, is_locked")
            .eq("team_slug", group_slug)
            .in_("user_id", batch_user_ids)
            .execute()
        )

        print(
            f"Retrieved data for {len(members_response.data)} users in batch {batch_num}"
        )

        for member in members_response.data:
            user_id = member["user_id"]
            current_tags = member.get("tags", [])
            is_locked = member.get("is_locked", False)

            print(
                f"User {user_id}: Current tags = {current_tags}, is_locked = {is_locked}"
            )

            # Skip locked users
            if is_locked:
                locked_users += 1
                print(f"User {user_id}: User is locked - skipping")
                continue

            # Check if tag already exists for this user
            if tag_name in current_tags:
                already_tagged += 1
                print(f"User {user_id}: Already has tag '{tag_name}' - skipping")
                continue

            # Add new tag to existing tags
            updated_tags = current_tags + [tag_name]
            print(
                f"User {user_id}: Adding tag '{tag_name}' - New tags will be {updated_tags}"
            )

            # Update the user's tags
            try:
                print(f"User {user_id}: Updating tags in database")
                update_response = (
                    supabase.table("crm_members")
                    .update({"tags": updated_tags})
                    .eq("team_slug", group_slug)
                    .eq("user_id", user_id)
                    .execute()
                )

                if update_response.data:
                    successful_updates += 1
                    print(f"User {user_id}: Successfully updated tags")
                else:
                    failed_updates += 1
                    print(f"User {user_id}: Failed to update tags - No data returned")
            except Exception as e:
                failed_updates += 1
                print(f"User {user_id}: Error updating tags - {str(e)}")

        batch_time = time.time() - batch_start_time
        print(
            f"Batch {batch_num}/{total_batches} completed in {batch_time:.2f} seconds"
        )
        print(
            f"Current progress: {successful_updates} updated, {already_tagged} already tagged, {locked_users} locked, {failed_updates} failed"
        )

    execution_time = time.time() - start_time
    print(f"\n--- TAG APPLICATION SUMMARY ---")
    print(f"Tag: {tag_name}")
    print(f"Group: {group_slug}")
    print(f"Total users processed: {len(user_ids)}")
    print(f"Successfully tagged: {successful_updates}")
    print(f"Already tagged: {already_tagged}")
    print(f"Locked users skipped: {locked_users}")
    print(f"Failed to tag: {failed_updates}")
    print(
        f"Started at: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))}"
    )
    print(f"Finished at: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Execution time: {execution_time:.2f} seconds")
    print(f"Average time per user: {execution_time/len(user_ids):.4f} seconds")

    return {
        "tag_name": tag_name,
        "total_users": len(user_ids),
        "successful_updates": successful_updates,
        "already_tagged": already_tagged,
        "locked_users": locked_users,
        "failed_updates": failed_updates,
        "execution_time": execution_time,
    }


def get_user_details(supabase, user_ids, group_slug):
    """
    Get detailed user information for the specified user IDs.

    Args:
        supabase: Supabase client
        user_ids: List of user IDs to get details for
        group_slug: Group identifier

    Returns:
        Dictionary with user_id as key and user details as value
    """
    start_time = time.time()

    user_details = {}
    chunk_size = 20

    # Get CRM member data
    for i in range(0, len(user_ids), chunk_size):
        user_chunk = user_ids[i : i + chunk_size]
        page = 0
        while True:
            crm_response = (
                supabase.table("crm_members")
                .select("*")
                .eq("team_slug", group_slug)
                .in_("user_id", user_chunk)
                .range(page * 1000, (page + 1) * 1000)
                .execute()
            )

            if not crm_response.data:
                break

            for member in crm_response.data:
                user_id = member["user_id"]
                user_details[user_id] = {
                    "user_id": user_id,
                    "first_name": member.get("first_name", ""),
                    "last_name": member.get("last_name", ""),
                    "email": member.get("email", ""),
                    "tags": member.get("tags", []),
                }

            page += 1
            if len(crm_response.data) < 1000:
                break

    # Get scraped member data to enrich the details
    for i in range(0, len(user_ids), chunk_size):
        user_chunk = user_ids[i : i + chunk_size]
        page = 0
        while True:
            scraped_response = (
                supabase.table("scraped_members")
                .select("id, name, last_offline, approved_at")
                .in_("id", user_chunk)
                .eq("group_slug", group_slug)
                .range(page * 1000, (page + 1) * 1000)
                .execute()
            )

            if not scraped_response.data:
                break

            for member in scraped_response.data:
                user_id = member["id"]
                if user_id in user_details:
                    user_details[user_id]["username"] = member.get("name", "")

                    # Calculate days offline if available
                    if member.get("last_offline"):
                        last_offline_str = member["last_offline"]
                        last_offline = datetime.datetime.fromisoformat(
                            last_offline_str.replace("Z", "+00:00")
                        )
                        days_offline = (datetime.datetime.now() - last_offline).days
                        user_details[user_id]["days_offline"] = days_offline

                    # Calculate membership days if available
                    if member.get("approved_at"):
                        approved_at_str = member["approved_at"]
                        approved_at = datetime.datetime.fromisoformat(
                            approved_at_str.replace("Z", "+00:00")
                        )
                        days_member = (datetime.datetime.now() - approved_at).days
                        user_details[user_id]["days_in_community"] = days_member

            page += 1
            if len(scraped_response.data) < 1000:
                break

    print(f"User details query completed in {time.time() - start_time:.2f} seconds")
    print(f"Retrieved details for {len(user_details)} users")

    return user_details


# Example composite function recreating original functionality
def find_churn_risk_users(
    supabase,
    group_slug,
    lookback_days=7,
    min_activity=1,
    max_activity=14,
    min_days_offline=0,
):
    """
    Identify users at risk of churning using the new modular functions.

    Args:
        supabase: Supabase client
        group_slug: Group identifier
        lookback_days: Number of days to analyze (default: 7)
        min_activity: Minimum activity count (default: 1)
        max_activity: Maximum activity count (default: 14)
        min_days_offline: Minimum days offline (default: 0)

    Returns:
        A tuple containing:
        - List of user records who are at risk of churning
        - List of user IDs who are at risk of churning
    """
    # Get all members
    all_members = get_all_members_for_group(supabase, group_slug)
    user_ids = [member["user_id"] for member in all_members]

    # Get users with specified activity levels
    activity_users = get_users_with_activity_count(
        supabase,
        group_slug,
        user_ids,
        lookback_days,
        min_activity=min_activity,
        max_activity=max_activity,
    )

    # Get users with specified offline days
    offline_users = get_users_by_offline_days(
        supabase, group_slug, user_ids, min_days_offline=min_days_offline
    )

    # Find users who match both criteria
    matching_user_ids = set(activity_users.keys()) & set(offline_users.keys())

    # Get detailed info for the matching users
    user_details = get_user_details(supabase, list(matching_user_ids), group_slug)

    # Enrich the user details with the activity and offline data
    churn_risk_users = []
    for user_id, details in user_details.items():
        user_data = details.copy()
        user_data.update(
            {
                "post_count": activity_users[user_id]["post_count"],
                "comment_count": activity_users[user_id]["comment_count"],
                "total_activity": activity_users[user_id]["total_activity"],
                "days_logged_out": offline_users[user_id],
            }
        )
        churn_risk_users.append(user_data)

    # Print summary
    print(f"\n--- CHURN RISK SUMMARY ({len(churn_risk_users)} users) ---")
    print(f"Activity period: Last {lookback_days} days")
    print(
        f"{'Name':<30} {'Posts':<8} {'Comments':<10} {'Days Offline':<15} {'Total Activity':<15}"
    )
    print("-" * 80)

    for user in churn_risk_users:
        name = f"{user.get('first_name', '')} {user.get('last_name', '')}".strip()
        if not name:
            name = f"User {user['user_id']}"

        print(
            f"{name[:30]:<30} {user['post_count']:<8} {user['comment_count']:<10} {user['days_logged_out']:<15} {user['total_activity']:<15}"
        )

    if not churn_risk_users:
        print("No users identified as at risk of churning with current parameters.")

    # Extract user IDs into a separate list
    churn_risk_user_ids = [user["user_id"] for user in churn_risk_users]

    return (churn_risk_users, churn_risk_user_ids)


def find_users_need_ascension(
    supabase, group_slug, lookback_days=70, max_days_offline=1, min_days_of_activity=7
):
    """
    Identify users who need ascension based on their activity levels and time logged in.

    Args:
        supabase: Supabase client
        group_slug: Group identifier
        lookback_days: Number of days to analyze (default: 70)
        max_days_offline: Maximum days the user can be logged out (default: 1)
        min_days_of_activity: Minimum days with activity required (default: 7)

    Returns:
        A tuple containing:
        - List of user records who are identified as needing ascension
        - List of user IDs who are identified as needing ascension
    """
    # Get all members
    all_members = get_all_members_for_group(supabase, group_slug)
    user_ids = [member["user_id"] for member in all_members]

    # test: does this user id exist? "b4d08466454a49499a63ca3b06434b24"
    target_user_id = "b4d08466454a49499a63ca3b06434b24"
    if target_user_id in user_ids:
        print(f"User ID {target_user_id} found in user IDs")
    else:
        print(f"User ID {target_user_id} not found in user IDs")

    # Get users with specified minimum active days
    active_days_users = get_users_with_active_days(
        supabase,
        group_slug,
        user_ids,
        lookback_days,
        min_active_days=min_days_of_activity,
    )

    # Get users who have been offline for at most max_days_offline
    offline_users = get_users_by_offline_days(
        supabase, group_slug, user_ids, max_days_offline=max_days_offline
    )

    # Find users who match both criteria
    matching_user_ids = set(active_days_users.keys()) & set(offline_users.keys())

    # Get detailed info for the matching users
    user_details = get_user_details(supabase, list(matching_user_ids), group_slug)

    # Get activity counts for reporting purposes
    activity_counts = get_users_with_activity_count(
        supabase, group_slug, list(matching_user_ids), lookback_days
    )

    # Enrich the user details with the activity data
    users_need_ascension = []
    for user_id, details in user_details.items():
        user_data = details.copy()
        user_data.update(
            {
                "days_of_activity": active_days_users[user_id],
                "days_logged_out": offline_users[user_id],
            }
        )

        # Add activity counts if available
        if user_id in activity_counts:
            user_data.update(
                {
                    "post_count": activity_counts[user_id]["post_count"],
                    "comment_count": activity_counts[user_id]["comment_count"],
                    "total_activity": activity_counts[user_id]["total_activity"],
                }
            )

        users_need_ascension.append(user_data)

    # Calculate date range for display
    import datetime

    end_date = datetime.datetime.now()
    start_date = end_date - datetime.timedelta(days=lookback_days)
    date_format = "%b %d, %Y"
    date_range = (
        f"{start_date.strftime(date_format)} to {end_date.strftime(date_format)}"
    )

    # Print summary
    print(
        f"\n--- USERS WHO NEED ASCENSION SUMMARY ({len(users_need_ascension)} users) ---"
    )
    print(f"Activity period: {date_range} ({lookback_days} days)")
    print(f"{'Name':<30} {'Days of Activity':<20} {'Days Logged Out':<20}")
    print("-" * 80)

    for user in users_need_ascension:
        name = f"{user.get('first_name', '')} {user.get('last_name', '')}".strip()
        if not name:
            name = f"User {user['user_id']}"

        print(
            f"{name[:30]:<30} {user['days_of_activity']:<20} {user['days_logged_out']:<20}"
        )

    if not users_need_ascension:
        print("No users identified as needing ascension with current parameters.")

    # Extract user IDs into a separate list
    ascension_user_ids = [user["user_id"] for user in users_need_ascension]

    return (users_need_ascension, ascension_user_ids)


def find_users_need_onboarding(
    supabase,
    group_slug,
    lookback_days=7,
    max_days_in_community=3,
    min_comments=1,
    min_posts=1,
):
    """
    Identify users who are ready to onboard based on their activity and time in the community.

    Args:
        supabase: Supabase client
        group_slug: Group identifier
        lookback_days: Number of days to analyze (default: 7)
        max_days_in_community: Maximum days the user should be in the community (default: 3)
        min_comments: Minimum number of comments the user should have made (default: 1)
        min_posts: Minimum number of posts the user should have made (default: 1)

    Returns:
        A tuple containing:
        - List of user records who are identified as ready to onboard
        - List of user IDs who are identified as ready to onboard
    """
    # Get all members
    all_members = get_all_members_for_group(supabase, group_slug)
    user_ids = [member["user_id"] for member in all_members]

    # Get users who have been members for at most max_days_in_community
    membership_users = get_users_by_membership_days(
        supabase, group_slug, user_ids, max_days=max_days_in_community
    )

    # Get users with min post count
    post_users = get_users_with_post_count(
        supabase, group_slug, user_ids, lookback_days, min_posts=min_posts
    )

    # Get users with min comment count
    comment_users = get_users_with_comment_count(
        supabase, group_slug, user_ids, lookback_days, min_comments=min_comments
    )

    # Find users who match all three criteria
    matching_user_ids = (
        set(membership_users.keys())
        & set(post_users.keys())
        & set(comment_users.keys())
    )

    # Get detailed info for the matching users
    user_details = get_user_details(supabase, list(matching_user_ids), group_slug)

    # Enrich the user details with the activity data
    users_need_onboarding = []
    for user_id, details in user_details.items():
        user_data = details.copy()
        user_data.update(
            {
                "post_count": post_users[user_id],
                "comment_count": comment_users[user_id],
                "days_in_community": membership_users[user_id],
            }
        )
        users_need_onboarding.append(user_data)

    # Calculate date range for display
    import datetime

    end_date = datetime.datetime.now()
    start_date = end_date - datetime.timedelta(days=lookback_days)
    date_format = "%b %d, %Y"
    date_range = (
        f"{start_date.strftime(date_format)} to {end_date.strftime(date_format)}"
    )

    # Print summary
    print(
        f"\n--- USERS READY TO ONBOARD SUMMARY ({len(users_need_onboarding)} users) ---"
    )
    print(f"Activity period: {date_range} ({lookback_days} days)")
    print(f"{'Name':<30} {'Posts':<8} {'Comments':<10} {'Days in Community':<20}")
    print("-" * 80)

    for user in users_need_onboarding:
        name = f"{user.get('first_name', '')} {user.get('last_name', '')}".strip()
        if not name:
            name = f"User {user['user_id']}"

        print(
            f"{name[:30]:<30} {user['post_count']:<8} {user['comment_count']:<10} {user['days_in_community']:<20}"
        )

    if not users_need_onboarding:
        print("No users identified as ready to onboard with current parameters.")

    # Extract user IDs into a separate list
    onboarding_user_ids = [user["user_id"] for user in users_need_onboarding]

    return (users_need_onboarding, onboarding_user_ids)


def tag_crm_users_for_churn(
    supabase,
    group_slug,
    lookback_days=9,
    min_activity=1,
    max_activity=14,
    min_days_offline=0,
):
    """
    Identify and tag users at risk of churning.

    Args:
        supabase: Supabase client
        group_slug: Group identifier
        lookback_days: Number of days to analyze (default: 9)
        min_activity: Minimum activity count (default: 1)
        max_activity: Maximum activity count (default: 14)
        min_days_offline: Minimum days offline (default: 0)

    Returns:
        Result dictionary from the tag application process
    """
    users_at_risk_of_churning, churn_risk_user_ids = find_churn_risk_users(
        supabase,
        group_slug,
        lookback_days=lookback_days,
        min_activity=min_activity,
        max_activity=max_activity,
        min_days_offline=min_days_offline,
    )

    # print the first few names of who we are churning if possible
    if users_at_risk_of_churning:
        print(f"Users at risk of churning:")
        for user in users_at_risk_of_churning:
            print(f"{user['first_name']} {user['last_name']}")

    if len(churn_risk_user_ids) > 0:
        return apply_tag_to_users(
            supabase, group_slug, churn_risk_user_ids, "churn_risk"
        )
    else:
        print("No users identified for churn risk tagging")
        return {
            "tag_name": "churn_risk",
            "total_users": 0,
            "successful_updates": 0,
            "already_tagged": 0,
            "failed_updates": 0,
            "locked_users": 0,
            "execution_time": 0,
        }


def tag_crm_users_for_onboarding(
    supabase,
    group_slug,
    lookback_days=7,
    max_days_in_community=3,
    min_comments=1,
    min_posts=1,
):
    """
    Identify and tag users who need onboarding.

    Args:
        supabase: Supabase client
        group_slug: Group identifier
        lookback_days: Number of days to analyze (default: 7)
        max_days_in_community: Maximum days the user should be in the community (default: 3)
        min_comments: Minimum number of comments the user should have made (default: 1)
        min_posts: Minimum number of posts the user should have made (default: 1)

    Returns:
        Result dictionary from the tag application process
    """
    users_need_onboarding, onboarding_user_ids = find_users_need_onboarding(
        supabase,
        group_slug,
        lookback_days=lookback_days,
        max_days_in_community=max_days_in_community,
        min_comments=min_comments,
        min_posts=min_posts,
    )

    if len(onboarding_user_ids) > 0:
        return apply_tag_to_users(
            supabase, group_slug, onboarding_user_ids, "onboarding"
        )
    else:
        print("No users identified for onboarding tagging")
        return {
            "tag_name": "onboarding",
            "total_users": 0,
            "successful_updates": 0,
            "already_tagged": 0,
            "failed_updates": 0,
            "locked_users": 0,
            "execution_time": 0,
        }


def tag_crm_users_for_ascension(
    supabase, group_slug, lookback_days=70, max_days_offline=3, min_days_of_activity=3
):
    """
    Identify and tag users who need ascension.

    Args:
        supabase: Supabase client
        group_slug: Group identifier
        lookback_days: Number of days to analyze (default: 70)
        max_days_offline: Maximum days the user can be logged out (default: 3)
        min_days_of_activity: Minimum days with activity required (default: 3)

    Returns:
        Result dictionary from the tag application process
    """
    users_need_ascension, ascension_user_ids = find_users_need_ascension(
        supabase,
        group_slug,
        lookback_days=lookback_days,
        max_days_offline=max_days_offline,
        min_days_of_activity=min_days_of_activity,
    )

    if len(ascension_user_ids) > 0:
        return apply_tag_to_users(supabase, group_slug, ascension_user_ids, "hot")
    else:
        print("No users identified for ascension tagging")
        return {
            "tag_name": "hot",
            "total_users": 0,
            "successful_updates": 0,
            "already_tagged": 0,
            "failed_updates": 0,
            "locked_users": 0,
            "execution_time": 0,
        }


def generate_user_activity_report(
    supabase,
    group_slug,
    lookback_days=30,
    min_activity=0,
    max_activity=None,
    min_days_offline=None,
    max_days_offline=None,
    min_days_in_community=None,
    sort_by="total_activity",
    sort_order="desc",
    limit=None,
):
    """
    Generate a comprehensive report of user activity based on specified criteria.

    Args:
        supabase: Supabase client
        group_slug: Group identifier
        lookback_days: Number of days to analyze (default: 30)
        min_activity: Minimum total activity required (default: 0)
        max_activity: Maximum total activity allowed (default: None)
        min_days_offline: Minimum days offline required (default: None)
        max_days_offline: Maximum days offline allowed (default: None)
        min_days_in_community: Minimum days user must have been in the community (default: None)
        sort_by: Field to sort results by (default: "total_activity")
                 Options: "post_count", "comment_count", "total_activity", "days_offline", "name"
        sort_order: Order to sort results (default: "desc")
                   Options: "asc", "desc"
        limit: Maximum number of users to include in the report (default: None)

    Returns:
        A tuple containing:
        - List of user records matching the criteria with activity metrics
        - Summary statistics about the report
    """
    print(f"\n--- GENERATING USER ACTIVITY REPORT ---")
    print(f"Group: {group_slug}")
    print(f"Lookback period: {lookback_days} days")
    print(f"Activity filter: {min_activity or 0} to {max_activity or 'unlimited'}")

    # Check if offline days filtering is enabled
    apply_offline_filter = min_days_offline is not None or max_days_offline is not None
    include_offline_days = apply_offline_filter or sort_by == "days_offline"

    if apply_offline_filter:
        print(
            f"Offline days filter: {min_days_offline or 0} to {max_days_offline or 'unlimited'}"
        )
    else:
        print("Offline days filter: Disabled (not applying offline days criteria)")

    # Check if membership days filtering is enabled
    apply_membership_filter = min_days_in_community is not None

    if apply_membership_filter:
        print(
            f"Membership filter: Excluding users in community for less than {min_days_in_community} days"
        )

    start_time = time.time()

    # Get all members
    all_members = get_all_members_for_group(supabase, group_slug)
    user_ids = [member["user_id"] for member in all_members]
    print(f"Retrieved {len(user_ids)} total users in group")

    # Filter users by membership duration if requested
    if apply_membership_filter:
        membership_users = get_users_by_membership_days(
            supabase, group_slug, user_ids, min_days=min_days_in_community
        )
        user_ids = list(membership_users.keys())
        print(
            f"Filtered to {len(user_ids)} users with at least {min_days_in_community} days in community"
        )

        if not user_ids:
            print("No users match the membership criteria.")
            return [], {
                "total_users_checked": len(all_members),
                "matching_users": 0,
                "execution_time": time.time() - start_time,
            }

    # Get users with specified activity levels
    activity_users = get_users_with_activity_count(
        supabase,
        group_slug,
        user_ids,
        lookback_days,
        min_activity=min_activity,
        max_activity=max_activity,
    )

    # If we're applying offline days filter, get those users
    if apply_offline_filter:
        # Get users with specified offline days
        offline_users = get_users_by_offline_days(
            supabase,
            group_slug,
            user_ids,
            min_days_offline=min_days_offline,
            max_days_offline=max_days_offline,
        )

        # Find users who match both criteria
        matching_user_ids = set(activity_users.keys()) & set(offline_users.keys())
        print(
            f"Found {len(matching_user_ids)}/{len(user_ids)} users matching all criteria ({len(matching_user_ids)/len(user_ids)*100:.1f}% of filtered set)"
        )
    else:
        # Just use the activity-filtered users
        matching_user_ids = set(activity_users.keys())
        print(
            f"Found {len(matching_user_ids)}/{len(user_ids)} users matching activity criteria ({len(matching_user_ids)/len(user_ids)*100:.1f}% of filtered set)"
        )

    if not matching_user_ids:
        print("No users match the specified criteria.")
        return [], {
            "total_users_checked": len(all_members),
            "matching_users": 0,
            "execution_time": time.time() - start_time,
        }

    # Get detailed info for the matching users
    user_details = get_user_details(supabase, list(matching_user_ids), group_slug)

    # Get offline days in a single batch if needed but not already fetched
    if include_offline_days and not apply_offline_filter:
        offline_users = get_users_by_offline_days(
            supabase, group_slug, list(matching_user_ids)
        )

    # If we filtered by membership, preserve that data for reporting
    membership_data = {}
    if apply_membership_filter:
        membership_data = membership_users

    # Enrich the user details with the activity and offline data
    matching_users = []
    for user_id, details in user_details.items():
        user_data = details.copy()
        user_data.update(
            {
                "post_count": activity_users[user_id]["post_count"],
                "comment_count": activity_users[user_id]["comment_count"],
                "total_activity": activity_users[user_id]["total_activity"],
                "user_id": user_id,
            }
        )

        # Add offline days data if we have it or need it
        if include_offline_days:
            user_data["days_offline"] = offline_users.get(user_id, 0)
        else:
            user_data["days_offline"] = 0  # Default value if not querying offline days

        # Add membership days data if we have it
        if apply_membership_filter:
            user_data["days_in_community"] = membership_data.get(user_id, 0)

        matching_users.append(user_data)

    # Sort the results
    reverse_sort = sort_order.lower() == "desc"

    if sort_by == "name":
        matching_users.sort(
            key=lambda u: (
                f"{u.get('first_name', '')} {u.get('last_name', '')}"
                if u.get("first_name") or u.get("last_name")
                else u.get("username", "")
            ),
            reverse=reverse_sort,
        )
    else:
        matching_users.sort(key=lambda u: u.get(sort_by, 0), reverse=reverse_sort)

    # Limit the results if specified
    if limit and limit < len(matching_users):
        matching_users = matching_users[:limit]
        print(f"Limited results to {limit} users")

    # Print the report
    print(f"\n--- USER ACTIVITY REPORT RESULTS ({len(matching_users)} users) ---")
    print(f"Activity period: Last {lookback_days} days")

    # Determine what columns to display
    if apply_membership_filter:
        print(
            f"{'Name':<30} {'Posts':<8} {'Comments':<10} {'Days Offline':<15} {'Days in Community':<20} {'Total Activity':<15}"
        )
        print("-" * 95)
    else:
        print(
            f"{'Name':<30} {'Posts':<8} {'Comments':<10} {'Days Offline':<15} {'Total Activity':<15}"
        )
        print("-" * 80)

    for user in matching_users:
        name = f"{user.get('first_name', '')} {user.get('last_name', '')}".strip()
        if not name:
            name = user.get("username", f"User {user['user_id']}")

        if apply_membership_filter:
            print(
                f"{name[:30]:<30} {user['post_count']:<8} {user['comment_count']:<10} {user['days_offline']:<15} {user.get('days_in_community', 0):<20} {user['total_activity']:<15}"
            )
        else:
            print(
                f"{name[:30]:<30} {user['post_count']:<8} {user['comment_count']:<10} {user['days_offline']:<15} {user['total_activity']:<15}"
            )

    execution_time = time.time() - start_time
    print(f"\nReport generated in {execution_time:.2f} seconds\n")

    # Return the data and summary statistics
    summary = {
        "total_users_checked": len(all_members),
        "matching_users": len(matching_users),
        "execution_time": execution_time,
    }

    return matching_users, summary


def display_single_user_activity(supabase, group_slug, user_slug, lookback_days=30):
    """
    Display activity metrics for a specific user in a table format.

    Args:
        supabase: Supabase client
        group_slug: Group identifier
        user_slug: User's slug/username to look up
        lookback_days: Number of days to analyze (default: 30)

    Returns:
        Dictionary containing the user's activity metrics if found, None otherwise
    """
    print(f"\n--- RETRIEVING ACTIVITY DATA FOR USER '{user_slug}' ---")
    print(f"Group: {group_slug}")
    print(f"Lookback period: {lookback_days} days")

    start_time = time.time()

    # Get user ID from slug
    user_id = get_user_id_from_slug(supabase, user_slug, group_slug)

    if not user_id:
        print(f"User with slug '{user_slug}' not found in group '{group_slug}'")
        return None

    print(f"Found user ID: {user_id}")

    # Get user's post count
    posts_data = get_users_with_post_count(
        supabase, group_slug, [user_id], lookback_days
    )
    post_count = posts_data.get(user_id, 0)

    # Get user's comment count
    comments_data = get_users_with_comment_count(
        supabase, group_slug, [user_id], lookback_days
    )
    comment_count = comments_data.get(user_id, 0)

    # Calculate total activity
    total_activity = post_count + comment_count

    # Get user's offline days
    offline_data = get_users_by_offline_days(supabase, group_slug, [user_id])
    days_offline = offline_data.get(user_id, 0)

    # Get user details
    user_details = get_user_details(supabase, [user_id], group_slug)
    user_data = user_details.get(user_id, {})

    # Calculate date range for display
    end_date = datetime.datetime.now()
    start_date = end_date - datetime.timedelta(days=lookback_days)
    date_format = "%b %d, %Y"
    date_range = (
        f"{start_date.strftime(date_format)} to {end_date.strftime(date_format)}"
    )

    # Prepare user activity data
    activity_data = {
        "user_id": user_id,
        "user_slug": user_slug,
        "first_name": user_data.get("first_name", ""),
        "last_name": user_data.get("last_name", ""),
        "email": user_data.get("email", ""),
        "post_count": post_count,
        "comment_count": comment_count,
        "total_activity": total_activity,
        "days_offline": days_offline,
        "tags": user_data.get("tags", []),
        "days_in_community": user_data.get("days_in_community", 0),
    }

    # Print the report
    print(f"\n--- USER ACTIVITY REPORT FOR {user_slug} ---")
    print(f"Activity period: {date_range} ({lookback_days} days)")
    print(
        f"{'Name':<30} {'Posts':<8} {'Comments':<10} {'Days Offline':<15} {'Total Activity':<15}"
    )
    print("-" * 80)

    name = f"{activity_data.get('first_name', '')} {activity_data.get('last_name', '')}".strip()
    if not name:
        name = user_slug

    print(
        f"{name[:30]:<30} {activity_data['post_count']:<8} {activity_data['comment_count']:<10} {activity_data['days_offline']:<15} {activity_data['total_activity']:<15}"
    )

    execution_time = time.time() - start_time
    print(f"\nReport generated in {execution_time:.2f} seconds\n")

    return activity_data


if __name__ == "__main__":
    pass
