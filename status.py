"""
status.py

last updated: 2025-07-12

todo
- add unit tests
- add google style docs for doc generation
- decouple supabase logic
- define scope for this file
- decouple large functions into smaller functions
- change logic to elt
"""

import time


def assign_status_to_users(supabase, group_slug, user_ids, status):
    """
    Generic function to assign a status to users.

    Args:
        supabase: Supabase client
        group_slug: Group identifier
        user_ids: List of user IDs to update
        status: Status value to assign ("chillin", "churn_risk", "hot")

    Returns:
        Dictionary with counts of users updated and skipped
    """
    # Validate status parameter
    valid_statuses = ["chillin", "churn_risk", "hot"]
    if status not in valid_statuses:
        print(
            f"Error: Invalid status '{status}'. Must be one of: {', '.join(valid_statuses)}"
        )
        return {
            "status": status,
            "total_users": len(user_ids),
            "successful_updates": 0,
            "already_set": 0,
            "failed_updates": len(user_ids),
            "disabled_users": 0,
            "invalid_status": True,
            "execution_time": 0,
        }

    if len(user_ids) == 0:
        print(f"No users to assign status '{status}'")
        return {
            "status": status,
            "total_users": 0,
            "successful_updates": 0,
            "already_set": 0,
            "failed_updates": 0,
            "disabled_users": 0,
            "execution_time": 0,
        }

    start_time = time.time()
    print(f"Starting status update process at {time.strftime('%Y-%m-%d %H:%M:%S')}")

    # Track statistics
    successful_updates = 0
    already_set = 0
    failed_updates = 0
    disabled_users = 0
    pinned_users = 0
    not_found = 0

    # Standardize batch size to 50 (matching reset_all_members_to_chillin)
    batch_size = 50
    # Use a smaller batch size for queries to avoid URI too large errors
    query_batch_size = 50

    print(
        f"Setting status '{status}' for {len(user_ids)} users in group '{group_slug}'"
    )

    # Fetch users in batches to avoid Request-URI Too Large errors
    members_data = {}
    print(f"Fetching current status and disabled status for all users in batches")

    for i in range(0, len(user_ids), query_batch_size):
        batch_user_ids = user_ids[i : i + query_batch_size]
        batch_query_num = i // query_batch_size + 1
        total_query_batches = (len(user_ids) + query_batch_size - 1) // query_batch_size

        print(
            f"Fetching data batch {batch_query_num}/{total_query_batches} ({len(batch_user_ids)} users)"
        )

        try:
            batch_members_response = (
                supabase.table("crm_members")
                .select("user_id, status, is_disabled, is_pinned")
                .eq("team_slug", group_slug)
                .in_("user_id", batch_user_ids)
                .execute()
            )

            # Add batch data to the complete dictionary
            for member in batch_members_response.data:
                members_data[member["user_id"]] = member

            print(
                f"Retrieved data for {len(batch_members_response.data)}/{len(batch_user_ids)} users in this batch"
            )

        except Exception as e:
            print(f"Error fetching user data batch {batch_query_num}: {str(e)}")
            # Continue with next batch rather than failing completely

    print(f"Total retrieved data for {len(members_data)} users")

    # Check for users not found in the database
    not_found_users = [user_id for user_id in user_ids if user_id not in members_data]
    if not_found_users:
        not_found = len(not_found_users)
        print(f"Warning: {not_found} users not found in the database:")
        print(
            f"Not found user IDs: {', '.join(not_found_users[:5])}{' ...' if len(not_found_users) > 5 else ''}"
        )

    # Identify users that need updating (not disabled and status != target status)
    users_to_update = []

    for user_id in user_ids:
        if user_id not in members_data:
            continue

        member = members_data[user_id]
        current_status = member.get("status", "chillin")
        is_disabled = member.get("is_disabled", False)
        is_pinned = member.get("is_pinned", False)

        # Skip disabled users
        if is_disabled:
            disabled_users += 1
            print(f"User {user_id}: User is disabled - skipping")
            continue

        # Skip pinned users
        if is_pinned:
            pinned_users += 1
            print(f"User {user_id}: User is pinned - skipping")
            continue

        # Skip users that already have the target status
        if current_status == status:
            already_set += 1
            continue

        # Add user to the update list
        users_to_update.append(user_id)

    print(f"Found {len(users_to_update)} users that need status updates to '{status}'")
    print(f"{already_set} users already have the status '{status}'")
    print(f"{disabled_users} disabled users skipped")
    print(f"{pinned_users} pinned users skipped")

    # Process users in batches to avoid overloading the API
    total_batches = (len(users_to_update) + batch_size - 1) // batch_size
    print(f"Processing in {total_batches} batches of up to {batch_size} users each")

    for i in range(0, len(users_to_update), batch_size):
        batch_num = i // batch_size + 1
        batch_user_ids = users_to_update[i : i + batch_size]
        batch_start_time = time.time()

        if not batch_user_ids:
            continue

        print(
            f"\nBatch {batch_num}/{total_batches}: Processing {len(batch_user_ids)} users"
        )
        print(
            f"Batch user IDs: {', '.join(batch_user_ids[:5])}{' ...' if len(batch_user_ids) > 5 else ''}"
        )

        try:
            # Update all users in the batch to the specified status
            update_response = (
                supabase.table("crm_members")
                .update({"status": status})
                .eq("team_slug", group_slug)
                .in_("user_id", batch_user_ids)
                .execute()
            )

            num_updated = len(update_response.data)
            successful_updates += num_updated

            if num_updated != len(batch_user_ids):
                this_batch_failed = len(batch_user_ids) - num_updated
                failed_updates += this_batch_failed
                print(
                    f"Warning: Expected to update {len(batch_user_ids)} users but only updated {num_updated}"
                )

            print(f"Successfully updated {num_updated} users to '{status}' status")

        except Exception as e:
            failed_updates += len(batch_user_ids)
            print(f"Error updating batch {batch_num}: {str(e)}")

        batch_time = time.time() - batch_start_time
        print(
            f"Batch {batch_num}/{total_batches} completed in {batch_time:.2f} seconds"
        )
        print(
            f"Current progress: {successful_updates} updated, {already_set} already set, {disabled_users} disabled, {pinned_users} pinned, {failed_updates} failed"
        )

    execution_time = time.time() - start_time
    print(f"\n--- STATUS UPDATE SUMMARY ---")
    print(f"Status: {status}")
    print(f"Group: {group_slug}")
    print(f"Total users processed: {len(user_ids)}")
    print(f"Successfully updated: {successful_updates}")
    print(f"Already set: {already_set}")
    print(f"Disabled users skipped: {disabled_users}")
    print(f"Pinned users skipped: {pinned_users}")
    print(f"Users not found: {not_found}")
    print(f"Failed to update: {failed_updates}")
    print(
        f"Started at: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))}"
    )
    print(f"Finished at: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Execution time: {execution_time:.2f} seconds")
    print(f"Average time per user: {execution_time/len(user_ids):.4f} seconds")

    return {
        "status": status,
        "total_users": len(user_ids),
        "successful_updates": successful_updates,
        "already_set": already_set,
        "disabled_users": disabled_users,
        "pinned_users": pinned_users,
        "not_found": not_found,
        "failed_updates": failed_updates,
        "execution_time": execution_time,
    }


def reset_all_members_to_chillin(supabase, group_slug):
    """
    Sets all non-disabled and non-pinned members in a group to 'chillin' status.

    Args:
        supabase: Supabase client
        group_slug: Group identifier

    Returns:
        Dictionary with counts of users updated and skipped
    """
    start_time = time.time()
    print(f"Starting status reset process at {time.strftime('%Y-%m-%d %H:%M:%S')}")

    # Track statistics
    successful_updates = 0
    already_set = 0
    failed_updates = 0
    pinned_users = 0

    # Get all non-disabled and non-pinned members for the group
    print(f"Fetching all non-disabled and non-pinned members for group '{group_slug}'")
    members_response = (
        supabase.table("crm_members")
        .select("user_id, status")
        .eq("team_slug", group_slug)
        .eq("is_disabled", False)
        .eq("is_pinned", False)
        .execute()
    )

    total_members = len(members_response.data)
    print(
        f"Found {total_members} non-disabled and non-pinned members in group '{group_slug}'"
    )

    if total_members == 0:
        print(f"No members to update for group '{group_slug}'")
        return {
            "status": "chillin",
            "total_members": 0,
            "successful_updates": 0,
            "already_set": 0,
            "pinned_users": 0,
            "failed_updates": 0,
            "execution_time": 0,
        }

    # Collect all user_ids that need updating (status != "chillin")
    members_to_update = []
    for member in members_response.data:
        if member.get("status") != "chillin":
            members_to_update.append(member["user_id"])
        else:
            already_set += 1

    print(
        f"{len(members_to_update)} members need updating, {already_set} already set to 'chillin'"
    )

    # Process members in batches to avoid overloading the API
    batch_size = 50
    total_batches = (total_members + batch_size - 1) // batch_size

    print(
        f"Setting status 'chillin' for all {total_members} non-disabled and non-pinned members"
    )
    print(f"Processing in {total_batches} batches of up to {batch_size} members each")

    # Update members in batches
    for i in range(0, len(members_to_update), batch_size):
        batch_num = i // batch_size + 1
        batch_user_ids = members_to_update[i : i + batch_size]
        batch_start_time = time.time()

        if not batch_user_ids:
            continue

        print(
            f"\nBatch {batch_num}/{total_batches}: Processing {len(batch_user_ids)} members"
        )
        print(
            f"Batch user IDs: {', '.join(batch_user_ids[:5])}{' ...' if len(batch_user_ids) > 5 else ''}"
        )

        try:
            # Update all members in batch to "chillin" status
            update_response = (
                supabase.table("crm_members")
                .update({"status": "chillin"})
                .eq("team_slug", group_slug)
                .in_("user_id", batch_user_ids)
                .execute()
            )

            num_updated = len(update_response.data)
            successful_updates += num_updated

            if num_updated != len(batch_user_ids):
                failed_updates += len(batch_user_ids) - num_updated
                print(
                    f"Warning: Expected to update {len(batch_user_ids)} members but only updated {num_updated}"
                )

            print(f"Successfully updated {num_updated} members to 'chillin' status")

        except Exception as e:
            failed_updates += len(batch_user_ids)
            print(f"Error updating batch {batch_num}: {str(e)}")

        batch_time = time.time() - batch_start_time
        print(
            f"Batch {batch_num}/{total_batches} completed in {batch_time:.2f} seconds"
        )
        print(
            f"Current progress: {successful_updates} updated, {already_set} already set, {pinned_users} pinned, {failed_updates} failed"
        )

    execution_time = time.time() - start_time
    print(f"\n--- STATUS RESET SUMMARY ---")
    print(f"Group: {group_slug}")
    print(f"Total members: {total_members}")
    print(f"Successfully updated: {successful_updates}")
    print(f"Already set to 'chillin': {already_set}")
    print(f"Pinned users skipped: {pinned_users}")
    print(f"Failed to update: {failed_updates}")
    print(
        f"Started at: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))}"
    )
    print(f"Finished at: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Execution time: {execution_time:.2f} seconds")
    if total_members > 0:
        print(f"Average time per member: {execution_time/total_members:.4f} seconds")

    return {
        "status": "chillin",
        "total_members": total_members,
        "successful_updates": successful_updates,
        "already_set": already_set,
        "pinned_users": pinned_users,
        "failed_updates": failed_updates,
        "execution_time": execution_time,
    }
