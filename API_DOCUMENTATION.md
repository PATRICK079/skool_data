# Skool Community Scraper - API Documentation

Detailed API reference for the Skool Community Scraper module.

## Table of Contents

- [Authentication & Authorization](#authentication--authorization)
- [Content Extraction](#content-extraction)
- [Member Management](#member-management)
- [Analytics & Metrics](#analytics--metrics)
- [User Segmentation](#user-segmentation)
- [Utilities](#utilities)
- [Database Operations](#database-operations)

## Authentication & Authorization

### `auth_token_utils.py`

#### `get_scrape_account_for_org(org_metadata, org_slug, org_id, clerk, is_admin_in_group_func)`

Selects the appropriate authentication account for an organization based on admin status and previous success.

**Parameters:**
- `org_metadata` (dict): Organization metadata containing `skool_slug` and `last_successful_scrape_account`
- `org_slug` (str): Organization slug identifier
- `org_id` (str): Organization ID
- `clerk` (ClerkClient): Clerk client instance
- `is_admin_in_group_func` (function): Function to check admin status in a group

**Returns:**
- `tuple`: (auth_token, admin_skool_handle, account_key) or (None, None, None) if no working account

**Example:**
```python
from deps.auth_token_utils import get_scrape_account_for_org
from deps.members import is_admin_in_group

auth_token, admin_handle, account_key = get_scrape_account_for_org(
    org_metadata={"skool_slug": "my-community"},
    org_slug="my-org",
    org_id="org_123",
    clerk=clerk_client,
    is_admin_in_group_func=is_admin_in_group
)
```

#### `check_and_update_goose_admin_access(org, metadata, skool_slug, is_admin, clerk)`

Updates the `has_goose_admin_access` field in Clerk metadata when admin status changes.

**Parameters:**
- `org` (dict): Organization data
- `metadata` (dict): Current organization metadata
- `skool_slug` (str): Skool community slug
- `is_admin` (bool): Current admin status
- `clerk` (ClerkClient): Clerk client instance

### `clerk.py`

#### `ClerkClient`

Main client for interacting with Clerk API.

**Constructor:**
```python
ClerkClient(api_key: Optional[str] = None, type: Optional[str] = None)
```

**Methods:**

##### `get_organization_by_slug(slug: str) -> Dict[str, Any]`

Retrieves organization data by slug.

##### `get_organization_metadata(slug: str) -> Dict[str, Any]`

Gets organization metadata by slug.

##### `update_organization_metadata(slug: str, metadata_updates: Dict[str, Any], org_id: str) -> Dict[str, Any]`

Updates specific fields in organization metadata.

##### `get_all_organizations(limit: int = 500) -> list`

Retrieves all organizations with pagination.

## Content Extraction

### `posts.py`

#### `get_build_id() -> str`

Retrieves the current build ID from Skool.com homepage.

**Returns:**
- `str`: Current build ID

**Example:**
```python
from deps.posts import get_build_id

build_id = get_build_id()
print(f"Current build ID: {build_id}")
```

#### `get_posts_on_page(group_slug: str, auth_token: str, page: int, build_id: str, retry_count=0) -> Optional[dict]`

Fetches posts from a specific page of a community.

**Parameters:**
- `group_slug` (str): Community slug
- `auth_token` (str): Authentication token
- `page` (int): Page number (1-based)
- `build_id` (str): Skool build ID
- `retry_count` (int): Retry attempt count

**Returns:**
- `dict`: Posts data with pagination info or None on error

**Example:**
```python
from deps.posts import get_posts_on_page

posts_data = get_posts_on_page(
    group_slug="my-community",
    auth_token="your_auth_token",
    page=1,
    build_id="build_123"
)

if posts_data:
    posts = posts_data.get("posts", [])
    print(f"Found {len(posts)} posts on page 1")
```

#### `get_total_post_count(group_slug: str, build_id: str, retry_count=0) -> Optional[int]`

Gets the total number of posts in a community.

**Parameters:**
- `group_slug` (str): Community slug
- `build_id` (str): Skool build ID
- `retry_count` (int): Retry attempt count

**Returns:**
- `int`: Total post count or None on error

### `comments.py`

#### `get_all_comments(group_id: str, post_id: str, auth_token: str, post_url: str) -> list`

Extracts all comments and replies for a post with pagination support.

**Parameters:**
- `group_id` (str): Group ID
- `post_id` (str): Post ID
- `auth_token` (str): Authentication token
- `post_url` (str): Post URL

**Returns:**
- `list`: Flat list of all comments and replies

**Example:**
```python
from deps.comments import get_all_comments

comments = get_all_comments(
    group_id="group_123",
    post_id="post_456",
    auth_token="your_auth_token",
    post_url="https://skool.com/group/post"
)

print(f"Found {len(comments)} total comments and replies")
```

#### `get_comments_page_1(group_id: str, post_id: str, auth_token: str) -> Optional[dict]`

Fetches the first page of comments for a post.

**Parameters:**
- `group_id` (str): Group ID
- `post_id` (str): Post ID
- `auth_token` (str): Authentication token

**Returns:**
- `dict`: First page of comments data or None on error

### `likes.py`

#### `get_likes_page_1(post_id: str, auth_token: str, limit: int = 10) -> Optional[dict]`

Retrieves the first page of likes for a post or comment.

**Parameters:**
- `post_id` (str): Post or comment ID
- `auth_token` (str): Authentication token
- `limit` (int): Number of likes to fetch per page

**Returns:**
- `dict`: Likes data with users and pagination info or None on error

#### `get_likes_for_page(post_id: str, auth_token: str, limit: int = 10, cursor: str = "") -> Optional[dict]`

Fetches subsequent pages of likes using cursor pagination.

**Parameters:**
- `post_id` (str): Post or comment ID
- `auth_token` (str): Authentication token
- `limit` (int): Number of likes to fetch per page
- `cursor` (str): Pagination cursor

**Returns:**
- `dict`: Likes data or None on error

#### `add_post_likes_to_db(supabase, post_id: str, post_url: str, post_metadata: dict, group_slug: str, post_created_by_user_id: str, auth_token: str) -> int`

Synchronizes likes for a post between API and database.

**Parameters:**
- `supabase`: Supabase client
- `post_id` (str): Post ID
- `post_url` (str): Post URL
- `post_metadata` (dict): Post metadata containing like count
- `group_slug` (str): Group slug
- `post_created_by_user_id` (str): User ID who created the post
- `auth_token` (str): Authentication token

**Returns:**
- `int`: Number of new likes added to database

## Member Management

### `members.py`

#### `is_admin_in_group(community_slug: str, auth_token: str, build_id: str, admin_skool_handle: str) -> bool`

Checks if a user has admin privileges in a community.

**Parameters:**
- `community_slug` (str): Community slug
- `auth_token` (str): Authentication token
- `build_id` (str): Skool build ID
- `admin_skool_handle` (str): User handle to check

**Returns:**
- `bool`: True if user is admin, False otherwise

#### `get_all_members_for_community(group_slug: str, build_id: str, auth_token: str = None, tab: str = "active", return_billing_products: bool = False) -> tuple`

Fetches all members for a community with optional billing data.

**Parameters:**
- `group_slug` (str): Community slug
- `build_id` (str): Skool build ID
- `auth_token` (str): Authentication token (optional)
- `tab` (str): Member tab ("active", "churned", "cancelling")
- `return_billing_products` (bool): Whether to return billing products

**Returns:**
- `tuple`: (members_list, billing_products) or (members_list, None)

#### `get_all_community_members_and_save(group_slug: str, build_id: str, auth_token: str = None, return_billing_products: bool = False) -> tuple`

Fetches and saves all community members to database.

**Parameters:**
- `group_slug` (str): Community slug
- `build_id` (str): Skool build ID
- `auth_token` (str): Authentication token (optional)
- `return_billing_products` (bool): Whether to return billing products

**Returns:**
- `tuple`: (active_members, churned_members, billing_products)

**Example:**
```python
from deps.members import get_all_community_members_and_save

active_members, churned_members, billing_products = get_all_community_members_and_save(
    group_slug="my-community",
    build_id="build_123",
    auth_token="your_auth_token",
    return_billing_products=True
)

print(f"Active members: {len(active_members)}")
print(f"Churned members: {len(churned_members)}")
```

#### `sync_churned_status(supabase, group_slug: str, build_id: str, auth_token: str) -> dict`

Synchronizes churned member status between API and database.

**Parameters:**
- `supabase`: Supabase client
- `group_slug` (str): Community slug
- `build_id` (str): Skool build ID
- `auth_token` (str): Authentication token

**Returns:**
- `dict`: Sync results with counts

## Analytics & Metrics

### `hud2.py`

#### `update_hud2(community_slug: str, build_id: str, auth_token: str, supabase) -> dict`

Updates all HUD2 metrics for a community including MRR, churn, and growth metrics.

**Parameters:**
- `community_slug` (str): Community slug
- `build_id` (str): Skool build ID
- `auth_token` (str): Authentication token
- `supabase`: Supabase client

**Returns:**
- `dict`: Updated metrics data

**Example:**
```python
from deps.hud2 import update_hud2

metrics = update_hud2(
    community_slug="my-community",
    build_id="build_123",
    auth_token="your_auth_token",
    supabase=supabase_client
)

print(f"Current MRR: ${metrics.get('current_mrr', 0)}")
print(f"Churn rate: {metrics.get('churn_rate', 0):.2%}")
```

#### `calculate_mrr(members: list) -> float`

Calculates Monthly Recurring Revenue from member data.

**Parameters:**
- `members` (list): List of member objects with MRR data

**Returns:**
- `float`: Total MRR

#### `calculate_churn_past_30(formatted_members: list) -> float`

Calculates 30-day churn rate.

**Parameters:**
- `formatted_members` (list): List of formatted member objects

**Returns:**
- `float`: Churn rate as decimal (e.g., 0.05 for 5%)

### `hud2_charts.py`

#### `calculate_monthly_metrics(members: list, community_slug: str) -> list`

Calculates monthly metrics for chart generation.

**Parameters:**
- `members` (list): List of member objects
- `community_slug` (str): Community slug

**Returns:**
- `list`: Monthly metrics data

## User Segmentation

### `tags.py`

#### `find_churn_risk_users(supabase, group_slug: str, lookback_days: int = 7, min_activity: int = 1, max_activity: int = 14, min_days_offline: int = 0) -> list`

Identifies users at risk of churning based on activity patterns.

**Parameters:**
- `supabase`: Supabase client
- `group_slug` (str): Community slug
- `lookback_days` (int): Days to look back for activity
- `min_activity` (int): Minimum activity threshold
- `max_activity` (int): Maximum activity threshold
- `min_days_offline` (int): Minimum days offline

**Returns:**
- `list`: List of user IDs at risk of churning

**Example:**
```python
from deps.tags import find_churn_risk_users

churn_risk_users = find_churn_risk_users(
    supabase=supabase_client,
    group_slug="my-community",
    lookback_days=7,
    min_activity=1,
    max_activity=14
)

print(f"Found {len(churn_risk_users)} users at risk of churning")
```

#### `find_users_need_onboarding(supabase, group_slug: str, lookback_days: int = 7, max_days_in_community: int = 3, min_comments: int = 1, min_posts: int = 1) -> list`

Identifies users who need onboarding assistance.

**Parameters:**
- `supabase`: Supabase client
- `group_slug` (str): Community slug
- `lookback_days` (int): Days to look back for activity
- `max_days_in_community` (int): Maximum days in community
- `min_comments` (int): Minimum comments required
- `min_posts` (int): Minimum posts required

**Returns:**
- `list`: List of user IDs needing onboarding

#### `apply_tag_to_users(supabase, group_slug: str, user_ids: list, tag_name: str) -> dict`

Applies tags to users in the database.

**Parameters:**
- `supabase`: Supabase client
- `group_slug` (str): Community slug
- `user_ids` (list): List of user IDs to tag
- `tag_name` (str): Tag name to apply

**Returns:**
- `dict`: Results of tagging operation

### `status.py`

#### `assign_status_to_users(supabase, group_slug: str, user_ids: list, status: str) -> dict`

Assigns status to users (chillin, churn_risk, hot).

**Parameters:**
- `supabase`: Supabase client
- `group_slug` (str): Community slug
- `user_ids` (list): List of user IDs
- `status` (str): Status to assign ("chillin", "churn_risk", "hot")

**Returns:**
- `dict`: Results of status assignment

**Example:**
```python
from deps.status import assign_status_to_users

result = assign_status_to_users(
    supabase=supabase_client,
    group_slug="my-community",
    user_ids=["user1", "user2", "user3"],
    status="churn_risk"
)

print(f"Successfully updated {result['successful_updates']} users")
```

#### `reset_all_members_to_chillin(supabase, group_slug: str) -> dict`

Resets all members' status to "chillin".

**Parameters:**
- `supabase`: Supabase client
- `group_slug` (str): Community slug

**Returns:**
- `dict`: Results of reset operation

## Utilities

### `utils.py`

#### `request_with_retries(method: str, url: str, max_retries: int = 5, backoff: int = 2, timeout: int = None, skip_retry_on_404: bool = False, skip_retry_on_401: bool = False, **kwargs) -> Optional[Response]`

Makes HTTP requests with automatic retry logic and proxy support.

**Parameters:**
- `method` (str): HTTP method ("GET", "POST", etc.)
- `url` (str): Request URL
- `max_retries` (int): Maximum retry attempts
- `backoff` (int): Backoff delay in seconds
- `timeout` (int): Request timeout
- `skip_retry_on_404` (bool): Skip retry on 404 errors
- `skip_retry_on_401` (bool): Skip retry on 401 errors
- `**kwargs`: Additional request parameters

**Returns:**
- `Response`: HTTP response object or None on error

#### `ensure_iso(timestamp) -> str`

Ensures a timestamp is in ISO format.

**Parameters:**
- `timestamp`: Timestamp (string, int, or datetime)

**Returns:**
- `str`: ISO formatted timestamp

#### `get_human_readable_duration(start_time: float) -> str`

Converts time difference to human-readable format.

**Parameters:**
- `start_time` (float): Start time from time.time()

**Returns:**
- `str`: Human-readable duration (e.g., "2 mins", "1 hr and 30 mins")

### `settings.py`

#### `get_proxies() -> Optional[dict]`

Gets a random proxy from the proxy list.

**Returns:**
- `dict`: Proxy configuration or None if proxies disabled

#### `get_timeout_for_request(use_proxy: bool = True) -> int`

Gets appropriate timeout based on proxy usage.

**Parameters:**
- `use_proxy` (bool): Whether proxy is being used

**Returns:**
- `int`: Timeout in seconds

## Database Operations

### `database.py`

#### `connect_to_supabase() -> Client`

Creates a Supabase client connection.

**Returns:**
- `Client`: Supabase client instance

**Example:**
```python
from deps.database import connect_to_supabase

supabase = connect_to_supabase()
```

### `notifications.py`

#### `send_desktop_notification(title: str, message: str, sticky: bool = True) -> None`

Sends desktop notifications (macOS only).

**Parameters:**
- `title` (str): Notification title
- `message` (str): Notification message
- `sticky` (bool): Whether notification should stay until dismissed

## Error Handling

All functions include comprehensive error handling:

- **Retry Logic**: Automatic retry for transient failures
- **Proxy Fallback**: Graceful fallback when proxies fail
- **Input Validation**: Parameter validation and type checking
- **Graceful Degradation**: Functions return None/empty values on error
- **Logging**: Comprehensive error logging and debugging output

## Rate Limiting

The module implements rate limiting to respect Skool's API limits:

- **Built-in Delays**: Configurable delays between requests
- **Proxy Rotation**: Automatic proxy rotation for high-volume requests
- **Request Throttling**: Prevents overwhelming the API

## Best Practices

1. **Always use retry logic**: Use `request_with_retries()` for all API calls
2. **Handle authentication properly**: Use `get_scrape_account_for_org()` for token selection
3. **Respect rate limits**: Don't disable delays unless necessary
4. **Validate data**: Check return values before processing
5. **Use batch operations**: Group database operations when possible
6. **Monitor errors**: Log and handle errors appropriately 