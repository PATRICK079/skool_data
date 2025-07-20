"""
posts.py
last updated: 2025-07-12

todo
- add unit tests
- add google style docs for doc generation
- decouple supabase logic
- define scope for this file
- database modeling for posts
- decouple get build id into a separate file
"""

# external
import json
import os
import time
from dotenv import load_dotenv
from bs4 import BeautifulSoup

# internal
from deps.settings import get_proxies, delay
from deps.utils import request_with_retries

load_dotenv(override=True)

auth_token = os.getenv("AUTH_TOKEN_ME")


def get_build_id():
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


def get_posts_on_page(
    group_slug: str, auth_token: str, page: int, build_id: str, retry_count=0
):
    print(f"\n=== Fetching posts for group {group_slug} on page {page} ===")
    url = f"https://www.skool.com/_next/data/{build_id}/{group_slug}.json?group={group_slug}&p={page}"

    print(f"Preparing request to: {url}")
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

    print(f"(get_posts_on_page) Sending API request...")
    time.sleep(delay)
    response = request_with_retries(
        "get", url, headers=headers, cookies=cookies, proxies=get_proxies(), timeout=10
    )

    if response is None:
        print(f"Error: API request failed after retries.")
        return None
    if response.status_code == 404 and retry_count < 3:
        time.sleep(10)
        print(f"Received 404 error. Retrying with a new build ID...")
        new_build_id = get_build_id()
        print(f"Got new build ID: {new_build_id}")
        return get_posts_on_page(
            group_slug, auth_token, page, new_build_id, retry_count=retry_count + 1
        )
    elif response.status_code != 200:
        print(f"Error: API request failed with status code {response.status_code}")
        print(f"Response: {response.text}")
        raise Exception(
            f"API request failed with status code {response.status_code}: {response.text}"
        )
    print(f"Success! Request returned status code {response.status_code}")
    data = response.json()
    return data


def get_total_post_count(group_slug: str, build_id: str, retry_count=0):
    print(f"\n=== Fetching total post count for group {group_slug} ===")
    url = f"https://www.skool.com/_next/data/{build_id}/{group_slug}/about.json?group={group_slug}"

    print(f"Preparing request to: {url}")
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

    print(f"(get_total_post_count) Sending API request...")
    time.sleep(delay)
    response = request_with_retries(
        "get", url, headers=headers, proxies=get_proxies(), timeout=10
    )
    if response is None:
        print(f"Error: API request failed after retries.")
        return 0
    if response.status_code == 404 and retry_count < 3:
        print(f"Received 404 error. Retrying with a new build ID...")
        time.sleep(10)
        new_build_id = get_build_id()
        print(f"Got new build ID: {new_build_id}")
        return get_total_post_count(
            group_slug, new_build_id, retry_count=retry_count + 1
        )
    if response.status_code != 200:
        print(f"Error: API request failed with status code {response.status_code}")
        print(f"Response: {response.text}")
        raise Exception(
            f"API request failed with status code {response.status_code}: {response.text}"
        )
    print(f"Success! Request returned status code {response.status_code}")
    data = response.json()
    total_posts = 0
    try:
        total_posts = data["pageProps"]["currentGroup"]["metadata"]["totalPosts"]
        print(f"Total posts in {group_slug}: {total_posts}")
    except KeyError as e:
        print(f"Error: Failed to extract total post count from response: {e}")
        print(f"Available keys: {', '.join(data.keys())}")
    return total_posts


def get_posts_from_db(supabase, group_slug: str):
    print(f"Fetching posts from database for group {group_slug}...")
    all_posts = []
    page = 0
    while True:
        posts_in_db = (
            supabase.table("scraped_posts")
            .select("*")
            .eq("group_slug", group_slug)
            .range(page * 1000, (page + 1) * 1000)
            .execute()
        )
        if not posts_in_db.data:
            break
        all_posts.extend(posts_in_db.data)
        page += 1
        if len(posts_in_db.data) < 1000:
            break
    print(f"Found {len(all_posts)} total posts in database for group {group_slug}")
    return all_posts


def add_post_to_db(
    supabase, post_id: str, group_slug: str, created_by: str, created_at: str
):
    """
    Add a post to the database if it doesn't already exist

    Args:
        supabase: Supabase client
        post_id: ID of the post
        group_slug: Slug of the group
        created_by: User ID who created the post

    Returns:
        True if a new post was added, False if it already exists
    """
    print(f"Checking if post {post_id[:8]} already exists in database...")
    # Check if post already exists
    existing_post = (
        supabase.table("scraped_posts").select("id").eq("id", post_id).execute()
    )

    if len(existing_post.data) == 0:
        # Post doesn't exist, insert it
        print(f"Post {post_id[:8]} not found in database, adding...")
        post_data = {
            "id": post_id,
            "group_slug": group_slug,
            "created_by": created_by,
            "created_at": created_at,
        }

        print(f"Executing insert into 'posts' table...")
        result = supabase.table("scraped_posts").insert(post_data).execute()
        print(f"Successfully added post {post_id[:8]} to database")
        return True

    print(f"Post {post_id[:8]} already exists in database, skipping")
    return False


def check_and_add_post_to_db(
    supabase,
    post_id: str,
    group_slug: str,
    created_by: str,
    posts_in_db: list,
    created_at: str,
):
    """
    Check if post exists in the database and add it if it doesn't

    Args:
        supabase: Supabase client
        post_id: ID of the post
        group_slug: Slug of the group
        created_by: User ID who created the post
        posts_in_db: List of posts already in the database

    Returns:
        True if a new post was added, False if it already exists
    """
    print(f"Checking if post {post_id[:8]} exists in pre-fetched database list...")
    if post_id not in [post["id"] for post in posts_in_db]:
        print(
            f"Post {post_id[:8]} not found in pre-fetched list, adding to database..."
        )
        added = add_post_to_db(
            supabase=supabase,
            post_id=post_id,
            group_slug=group_slug,
            created_by=created_by,
            created_at=created_at,
        )
        if added:
            print(f"Added post {post_id[:8]} to database")
        return added

    print(f"Post {post_id[:8]} already exists in pre-fetched list, skipping")
    return False


if __name__ == "__main__":
    pass
