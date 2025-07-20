"""
likes.py

last updated: 2025-07-12

todo
- add unit tests
- add google style docs for doc generation
- decouple supabase logic
- define scope for this file better
- database modeling for likes
- full data storage with keys with skools keys for primary keys
- decouple large functions into smaller functions
"""

# internal
import json
import time

# external
from deps.settings import get_proxies, delay
from deps.utils import request_with_retries


def get_likes_page_1(post_id: str, auth_token: str, limit: int = 10):
    # Q: can i pass in a comment id?
    # a: use the has_more field, if its true, reference the .cursor
    print(
        f"Fetching first page of likes for post/comment {post_id[:8]}... (limit: {limit})"
    )
    url = f"https://api.skool.com/posts/{post_id}/vote-users"
    params = {
        "limit": limit,
        "tab": "upvotes",
    }
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
        "Cookie": f"auth_token={auth_token}",
    }

    print(f"Sending API request to: {url}")
    time.sleep(delay)
    response = request_with_retries(
        "get", url, headers=headers, params=params, proxies=get_proxies(), timeout=10
    )

    if response and response.status_code == 200:
        data = response.json()
        print(f"Success! Received data with {len(data.get('users', []))} users")
        # save to get_likes_for_page.json
        # with open("get_likes_page_1.json", "w") as f:
        #     json.dump(data, f)
        # print(f"Saved response data to get_likes_page_1.json")
        return data
    else:
        if response:
            print(f"Error: API request failed with status code {response.status_code}")
            print(f"Response: {response.text}")
        else:
            print(f"Error: API request failed after retries.")
        return None


def get_likes_for_page(
    post_id: str, auth_token: str, limit: int = 10, cursor: str = ""
):
    # Q: where do i get the crusor?
    print(
        f"Fetching page of likes for post/comment {post_id[:8]}... (limit: {limit}, cursor: {cursor[:15]}{'...' if len(cursor) > 15 else ''})"
    )
    url = f"https://api.skool.com/posts/{post_id}/vote-users"
    params = {"limit": limit, "tab": "upvotes", "cursor": cursor}
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
        "Cookie": f"auth_token={auth_token}",
    }

    print(f"Sending API request to: {url}")
    time.sleep(delay)
    response = request_with_retries(
        "get", url, headers=headers, params=params, proxies=get_proxies(), timeout=10
    )

    if response and response.status_code == 200:
        data = response.json()
        print(f"Success! Received data with {len(data.get('users', []))} users")
        return data
    else:
        if response:
            print(f"Error: API request failed with status code {response.status_code}")
            print(f"Response: {response.text}")
        else:
            print(f"Error: API request failed after retries.")
        return None


def add_likes_to_db(
    supabase,
    post_id: str,
    user_ids: list,
    group_slug: str,
    created_for: str,
    is_post_like: bool = False,
    post_url: str = None,
):
    """
    Add multiple likes to the database in a batch operation

    Args:
        supabase: Supabase client
        post_id: ID of the post or comment being liked
        user_ids: List of user IDs who liked the post/comment
        group_slug: Slug of the group where the post/comment exists
        created_for: ID of the user who created the post/comment
        is_post_like: Whether this is a like on a post (True) or comment (False)
        post_url: Optional URL to the post
    """
    try:
        print(
            f"Adding {len(user_ids)} likes to database for {'post' if is_post_like else 'comment'} {post_id[:8]}..."
        )

        # Create a list of like records to insert
        like_records = []
        for user_id in user_ids:
            like_record = {
                "id": f"{user_id}_{post_id}",  # Create a unique ID
                "group_slug": group_slug,
                "created_by": user_id,
                "created_for": created_for,
                "post_id": post_id,
                "is_post_like": is_post_like,
            }

            # Add optional post_url if provided
            if post_url:
                like_record["post_url"] = post_url

            like_records.append(like_record)

        print(f"Prepared {len(like_records)} like records for database insertion")

        # Batch insert all records
        if like_records:
            print(f"Executing batch insert into 'likes' table...")
            supabase.table("scraped_likes").insert(like_records).execute()
            print(f"Successfully added {len(like_records)} likes to database")
            return len(like_records)
        print("No like records to insert")
        return 0
    except Exception as e:
        print(f"Error adding likes to database: {e}")
        return 0


def get_likes_from_db(supabase, post_id: str):
    """
    Get list of user IDs who liked a post or comment

    Args:
        supabase: Supabase client
        post_id: ID of the post or comment

    Returns:
        list: List of user IDs who liked the post/comment
    """
    try:
        print(f"Fetching likes from database for {post_id[:8]}...")
        all_likes = []
        page = 0
        while True:
            query = (
                supabase.table("scraped_likes")
                .select("created_by")
                .eq("post_id", post_id)
                .range(page * 1000, (page + 1) * 1000)
            )
            response = query.execute()
            if not response.data:
                break
            all_likes.extend([like["created_by"] for like in response.data])
            page += 1
            if len(response.data) < 1000:
                break
        print(f"Found {len(all_likes)} total likes in database for {post_id[:8]}")
        return all_likes
    except Exception as e:
        print(f"Error fetching likes from database: {e}")
        return []


def add_comment_likes_to_db(
    supabase,
    all_comments_for_post,
    group_slug,
    auth_token,
    post_url,
    post_created_by_user_id,
):
    """
    Synchronize likes for all comments and subcomments with the database

    Args:
        supabase: Supabase client
        all_comments_for_post: List of comment objects for a post
        group_slug: Slug of the group
        auth_token: Authentication token
        post_url: Optional URL to the post
        post_created_by_user_id: ID of the user who created the post
    Returns:
        int: Total number of new likes added
    """
    print(
        f"\n=== Starting comment likes sync for {len(all_comments_for_post)} comments ==="
    )
    total_new_likes = 0
    processed_comments = 0

    # For each comment, add new likes from the API to the database
    for comment in all_comments_for_post:
        processed_comments += 1
        comment_id = comment["id"]
        likes = comment.get("metadata", {}).get("upvotes", 0)

        print(
            f"[{processed_comments}/{len(all_comments_for_post)}] Processing comment {comment_id[:8]}... ({likes} likes)"
        )

        if likes == 0:
            print(f"  - Skipping comment with 0 likes")
            continue

        # Get existing likes from database
        likes_in_db = get_likes_from_db(supabase, comment_id)
        print(f"  - Found {len(likes_in_db)} existing likes in database")

        if len(likes_in_db) < likes:
            print(
                f"  - Fetching {likes} likes from API (missing {likes - len(likes_in_db)})"
            )
            # Fetch all of the likes
            all_users_who_liked = []

            # Start with page 1
            print(f"  - Fetching page 1 of likes")
            likes_page_1_data = get_likes_page_1(comment_id, auth_token)

            has_more = likes_page_1_data["has_more"]
            cursor = likes_page_1_data.get("cursor", None)
            all_users_who_liked.extend(likes_page_1_data["users"])
            print(
                f"  - Page 1: Found {len(likes_page_1_data['users'])} likes, has_more={has_more}"
            )

            page_num = 2
            while has_more:

                print(f"  - Fetching page {page_num} of likes")
                likes_page_data = get_likes_for_page(
                    comment_id, auth_token, cursor=cursor
                )
                has_more = likes_page_data["has_more"]
                cursor = likes_page_data.get("cursor", None)
                # NOTEL cursor is none of theres no more.
                all_users_who_liked.extend(likes_page_data["users"])
                print(
                    f"  - Page {page_num}: Found {len(likes_page_data['users'])} likes, has_more={has_more}"
                )
                page_num += 1

            # Find out which likes are new
            new_likes = [
                user["id"]
                for user in all_users_who_liked
                if user["id"] not in likes_in_db
            ]
            print(f"  - Found {len(new_likes)} new likes to add")

            # Insert new likes to database
            if new_likes:
                print(f"  - Adding {len(new_likes)} new likes to database")
                added_likes = add_likes_to_db(
                    supabase,
                    post_id=comment_id,
                    user_ids=new_likes,
                    group_slug=group_slug,
                    created_for=post_created_by_user_id,
                    is_post_like=False,
                    post_url=post_url,
                )
                total_new_likes += added_likes
                print(f"  - Successfully added {added_likes} likes")
            else:
                print("  - No new likes to add")
        else:
            print(f"  - All {likes} likes already in database, skipping")

    print(f"\n=== Finished comment likes sync ===")
    print(f"Processed {processed_comments} comments")
    print(f"Added {total_new_likes} new likes to database")
    return total_new_likes


def add_post_likes_to_db(
    supabase,
    post_id: str,
    post_url: str,
    post_metadata: dict,
    group_slug: str,
    post_created_by_user_id: str,
    auth_token: str,
):
    """
    Sync likes for a post between API and database

    Args:
        supabase: Supabase client
        post_id: ID of the post
        post_url: URL of the post
        post_metadata: Metadata dictionary of the post
        group_slug: Slug of the group
        post_created_by_user_id: User ID who created the post
        auth_token: Authentication token for API requests

    Returns:
        int: Number of new likes added to the database
    """
    # First, get the likes for this post from our db
    likes_in_db = get_likes_from_db(supabase, post_id)

    # Second, compare the count of our likes vs the post likes
    like_count_on_post = post_metadata.get("updates", 0)
    total_new_likes = 0

    if like_count_on_post > len(likes_in_db):
        print(
            f"  - Fetching {like_count_on_post} likes from API (missing {like_count_on_post - len(likes_in_db)})"
        )
        # Fetch all of the likes
        all_users_who_liked = []

        # Start with page 1
        print(f"  - Fetching page 1 of likes")
        likes_page_1_data = get_likes_page_1(post_id, auth_token)

        has_more = likes_page_1_data["has_more"]
        cursor = likes_page_1_data.get("cursor", None)
        all_users_who_liked.extend(likes_page_1_data["users"])
        print(
            f"  - Page 1: Found {len(likes_page_1_data['users'])} likes, has_more={has_more}"
        )

        page_num = 2
        while has_more:

            print(f"  - Fetching page {page_num} of likes")
            likes_page_data = get_likes_for_page(post_id, auth_token, cursor=cursor)
            has_more = likes_page_data["has_more"]
            cursor = likes_page_data["cursor"]
            all_users_who_liked.extend(likes_page_data["users"])
            print(
                f"  - Page {page_num}: Found {len(likes_page_data['users'])} likes, has_more={has_more}"
            )
            page_num += 1

        # Find out which likes are new
        new_likes = [
            user["id"] for user in all_users_who_liked if user["id"] not in likes_in_db
        ]
        print(f"  - Found {len(new_likes)} new likes to add")

        # Insert new likes to database
        if new_likes:
            print(f"  - Adding {len(new_likes)} new likes to database")
            added_likes = add_likes_to_db(
                supabase,
                post_id=post_id,
                user_ids=new_likes,
                group_slug=group_slug,
                created_for=post_created_by_user_id,
                is_post_like=True,
                post_url=post_url,
            )
            total_new_likes = added_likes
            print(f"  - Successfully added {added_likes} likes")
        else:
            print("  - No new likes to add")
    else:
        print(f"  - All {like_count_on_post} likes already in database, skipping")

    return total_new_likes


if __name__ == "__main__":
    pass
