"""
comments.py

last updated: 2025-07-12

todo
- seperate out get_build_id for decoupling
- decouple supabase logic
- define scope better / map functionality 
- add unit tests
- add google style docs for doc generation
- plan database better for comment objects 
- (store all data and use skool ids for primary keys)
"""

# external
import json
import time
from bs4 import BeautifulSoup

# internal
from deps.utils import request_with_retries
from deps.settings import get_proxies, delay


def get_build_id():
    """
    Get the build id for the skool.com website
    """
    url = f"https://www.skool.com"
    time.sleep(delay)
    response = request_with_retries(
        "get",
        url,
        timeout=5,
        headers={
            "accept": "*/*",
            "accept-language": "en-US,en;q=0.9",
            "priority": "u=1, i",
            "sec-ch-ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"macOS"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "x-nextjs-data": "1",
        },
        proxies=get_proxies(),
    )
    if response is None or response.status_code != 200:
        print(f"Error: API request failed after retries.")
        if response is not None:
            print(f"Status code: {response.status_code}")
            print(f"Response: {response.text}")
        raise Exception(
            f"API request failed with status code {response.status_code if response else 'N/A'}: {response.text if response else 'No response'}"
        )
    soup = BeautifulSoup(response.text, "html.parser")
    script_tag = soup.find("script", {"id": "__NEXT_DATA__"})
    json_text = script_tag.string
    json_data = json.loads(json_text)
    build_id = json_data["buildId"]
    print(f"Build ID: {build_id}")
    return build_id


def get_all_comments(group_id: str, post_id: str, auth_token: str, post_url: str):
    """
    Get all comments and subcomments for a post.
    Handles pagination by fetching all pages of comments.
    Returns a flat array of all post objects.
    """
    print(
        f"\n=== Starting to fetch all comments for post {post_id[:8]} in group {group_id[:8]} ==="
    )

    # Get first page of comments
    print(f"Fetching first page of comments...")
    comments_page_1_data = get_comments_page_1(group_id, post_id, auth_token)
    if not comments_page_1_data:
        print("No comments found or error fetching first page")
        return []

    children = comments_page_1_data.get("post_tree", {}).get("children", [])
    print(f"Found {len(children)} top-level comments on first page")

    if len(children) == 0:
        print("No comments found on first page")
        return []

    all_posts = flatten_post_tree(children)
    print(f"Flattened to {len(all_posts)} total comments and replies")

    # Check if there are more comments to fetch
    last = comments_page_1_data["last"]
    has_more = last != 0
    print(f"Pagination check: last={last}, has_more={has_more}")

    page_num = 2
    # Track the previous 'last' value to detect when it's not changing
    prev_last = 0
    # If there are more comments, fetch them using pagination
    while has_more:
        print(f"Fetching page {page_num} of comments (created_gt={last})...")
        # Get more comments using the 'last' timestamp as created_gt
        more_comments_data = get_more_comments(
            group_id, post_id, auth_token, created_gt=last, post_url=post_url
        )
        if not more_comments_data:
            print(f"Error or no more comments found on page {page_num}")
            break

        # Add these new comments to our flat list
        if "children" in more_comments_data and more_comments_data["children"]:
            new_comments = flatten_post_tree(more_comments_data["children"])
            print(
                f"Found {len(more_comments_data['children'])} top-level comments on page {page_num}"
            )
            print(
                f"Flattened to {len(new_comments)} total comments and replies on page {page_num}"
            )
            all_posts.extend(new_comments)
        else:
            print(f"No children found on page {page_num}")

        # Update last and check if we're done
        prev_last = last
        last = more_comments_data["last"]
        # Stop if last is 0 or if last hasn't changed (same as previous)
        has_more = last != 0 and last != prev_last
        print(
            f"Pagination check: last={last}, prev_last={prev_last}, has_more={has_more} | post_url: {post_url}"
        )
        page_num += 1

    print(f"Total comments and replies found: {len(all_posts)}")
    return all_posts


def get_comments_page_1(group_id: str, post_id: str, auth_token: str):
    print(
        f"Fetching first page of comments for post {post_id[:8]} in group {group_id[:8]}..."
    )
    url = f"https://api.skool.com/posts/{post_id}/comments"

    params = {"group-id": group_id, "limit": 25, "pinned": "true"}

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

    print(f"Sending API request to: {url}")
    time.sleep(delay)
    response = request_with_retries(
        "get",
        url,
        params=params,
        headers=headers,
        cookies=cookies,
        proxies=get_proxies(),
        timeout=None,  # Will use proxy-aware timeout
    )
    status_code = response.status_code if response else None
    print(f"Status code: {status_code}")

    if response and response.status_code == 200:
        data = response.json()
        print(f"Success! Received data with post tree")
        return data
    else:
        if response:
            print(f"Error: API request failed with status code {response.status_code}")
            print(f"Response: {response.text}")
        else:
            print(f"Error: API request failed after retries.")
        return None


def get_more_comments(
    group_id: str, post_id: str, auth_token: str, created_gt: str, post_url: str
):
    # determine if theres more based on the count of comments fetched
    # if more, then pass in the "last" field as created_gt
    # Q: if last is 0, does that mean theres no more
    # Q: if last is not 0, does that mean theres more?
    print(
        f"Fetching more comments for post {post_id[:8]} with created_gt={created_gt}... | post_url: {post_url}"
    )
    url = f"https://api.skool.com/posts/{post_id}/comments"

    params = {
        "group-id": group_id,
        "limit": 25,
        "pinned": "true",
        "created_gt": created_gt,
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
    }

    cookies = {"auth_token": auth_token}

    print(f"Sending API request to: {url}")
    time.sleep(delay)
    response = request_with_retries(
        "get",
        url,
        params=params,
        headers=headers,
        cookies=cookies,
        proxies=get_proxies(),
        timeout=None,  # Will use proxy-aware timeout
    )
    status_code = response.status_code if response else None
    print(f"Status code: {status_code}")

    if response and response.status_code == 200:
        data = response.json()
        print(f"Success! Received more comments data")
        return data
    else:
        if response:
            print(f"Error: API request failed with status code {response.status_code}")
            print(f"Response: {response.text}")
        else:
            print(f"Error: API request failed after retries.")
        return None


def flatten_post_tree(children_array):
    """
    Recursively flatten the nested post tree structure.
    Returns a flat array of post objects.
    """
    print(f"Flattening post tree with {len(children_array)} top-level items...")
    flat_posts = []

    for i, child in enumerate(children_array):
        # Add the current post
        if "post" in child:
            flat_posts.append(child["post"])
            post_id = child["post"].get("id", "unknown")[:8]
            print(f"  - Added post {i+1}/{len(children_array)}: {post_id}")

        # Recursively process children if they exist
        if "children" in child and child["children"]:
            print(
                f"  - Processing {len(child['children'])} child comments for post {i+1}/{len(children_array)}"
            )
            child_posts = flatten_post_tree(child["children"])
            flat_posts.extend(child_posts)
            print(
                f"  - Added {len(child_posts)} child comments for post {i+1}/{len(children_array)}"
            )

    print(f"Flattening complete: {len(flat_posts)} total items")
    return flat_posts


def get_comments_from_db(supabase, post_url: str):
    try:
        print(f"Fetching existing comments from database for post URL: {post_url}")
        all_comments = []
        page = 0
        while True:
            query = (
                supabase.table("scraped_comments")
                .select("id")
                .eq("post_url", post_url)
                .range(page * 1000, (page + 1) * 1000)
            )
            response = query.execute()
            if not response.data:
                break
            all_comments.extend([comment["id"] for comment in response.data])
            page += 1
            if len(response.data) < 1000:
                break
        print(f"Found {len(all_comments)} existing comments in database")
        return all_comments
    except Exception as e:
        print(f"Error fetching comments from database: {e}")
        return []


def add_comment_to_db(
    supabase,
    comment_id,
    group_slug,
    created_by,
    created_for,
    post_url=None,
    created_at=None,
):
    """
    Add a comment to the database

    Args:
        supabase: Supabase client
        comment_data: Comment data from the API (must have 'id' field)
        group_slug: Slug of the group where the comment exists
        created_by: ID of the user who created the comment
        created_for: ID of the user whose post/comment is being responded to
        post_url: Optional URL to the post

    Returns:
        bool: Whether the comment was successfully added
    """
    try:
        print(f"Adding comment {comment_id[:8]} to database")

        # Create comment record
        comment_record = {
            "id": comment_id,
            "group_slug": group_slug,
            "created_by": created_by,
            "created_for": created_for,
            "created_at": created_at,
        }

        # Add optional post_url if provided
        if post_url:
            comment_record["post_url"] = post_url
            print(f"  - Including post URL: {post_url[:30]}...")

        # Insert the record
        print(f"  - Executing insert into 'comments' table...")
        result = supabase.table("scraped_comments").insert(comment_record).execute()

        if result.data:
            print(f"  - Successfully added comment {comment_id[:8]}")
            return True
        else:
            print(f"  - Failed to add comment {comment_id[:8]}")
            return False

    except Exception as e:
        print(f"Error adding comment to database: {e}")
        return False


def find_created_for_in_flat_comments(parent_id: str, flat_comments: list):
    # Useful for finding the subcomments parent comment owner user id.
    print(
        f"Looking for creator of parent comment {parent_id[:8]} in {len(flat_comments)} comments..."
    )
    for comment in flat_comments:
        if comment["id"] == parent_id:
            print(f"  - Found parent comment creator: {comment['user_id'][:8]}")
            return comment["user_id"]
    print(f"  - Could not find parent comment creator for {parent_id[:8]}")
    return None


def add_new_comments_to_db(
    supabase, all_comments_for_post, group_slug, post_url, post_created_by_user_id
):
    """
    Synchronize comments from API with the database

    Args:
        supabase: Supabase client
        all_comments_for_post: List of comment objects for a post
        group_slug: Slug of the group
        post_url: URL of the post

    Returns:
        int: Number of new comments added to the database
    """
    print(f"\n=== Starting comments sync for {len(all_comments_for_post)} comments ===")
    comments_in_db = get_comments_from_db(supabase, post_url)
    new_comments_added = 0
    processed_comments = 0

    for comment in all_comments_for_post:
        processed_comments += 1
        comment_id = comment["id"]

        print(
            f"[{processed_comments}/{len(all_comments_for_post)}] Processing comment {comment_id[:8]}..."
        )

        if comment_id not in comments_in_db:
            print(f"  - Comment {comment_id[:8]} not found in database, adding...")
            # Find the parent (who this comment is responding to)
            parent_id = comment.get("parent_id", None)

            if parent_id:
                print(f"  - Comment has parent_id {parent_id[:8]}, finding creator...")
                created_for = find_created_for_in_flat_comments(
                    parent_id, all_comments_for_post
                )

            if not created_for:
                print(
                    f"Unable to find creator, this means the created for user id is the post creator."
                )
                created_for = post_created_by_user_id

            # Add the comment to the database
            print(f"  - Adding comment to database...")
            success = add_comment_to_db(
                supabase,
                comment_id,
                group_slug,
                comment["user_id"],
                created_for,
                post_url,
                created_at=comment["created_at"],
            )
            if success:
                new_comments_added += 1
                print(
                    f"  - Successfully added comment {comment_id[:8]} ({new_comments_added} total added)"
                )
        else:
            print(f"  - Comment {comment_id[:8]} already exists in database, skipping")

    print(f"\n=== Finished comments sync ===")
    print(f"Processed {processed_comments} comments")
    print(
        f"Found {len(all_comments_for_post)} comments, {len(comments_in_db)} already in database"
    )
    print(f"Added {new_comments_added} new comments to database")
    return new_comments_added


if __name__ == "__main__":
    pass
