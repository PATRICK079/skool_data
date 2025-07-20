"""
auth_token_utils.py

last updated: 2025-07-12

Defines the scrape accounts dictionary,
provides functionality to get which account is an admin
in the group.

Scope
Admin check

functions
- get_scrape_account_for_org: get the scrape account for an org
- check_and_update_goose_admin_access: updates goose admin status in clerk metadata

todo
- decouple is_admin_in_group_func to not be a higher order function
- standardize build_id (as an arg)
- standardize the logging to be for whole runs instead of mini prints
- add unit tests
- add google style docs for doc generation
"""

# external
import os
from dotenv import load_dotenv
from deps.comments import get_build_id

load_dotenv(override=True)

# --- Auth tokens and handles ---
SCRAPE_ACCOUNTS = {
    "goose": {
        "auth_token": os.getenv("AUTH_TOKEN_GOOSE"),
        "admin_skool_handle": "goose",
    },
    "goose_free": {
        "auth_token": os.getenv("AUTH_TOKEN_GOOSE_FREE"),
        "admin_skool_handle": "crm",
    },
}


# --- Main function for new logic ---
def get_scrape_account_for_org(
    org_metadata, org_slug, org_id, clerk, is_admin_in_group_func
):
    """
    Try to use the last successful scrape account from org metadata, fallback to the other, and update metadata if needed.
    Logs all steps. Accepts org_metadata, org_slug, org_id, ClerkClient instance, and is_admin_in_group_func.
    Returns (auth_token, admin_skool_handle, account_key)
    """
    skool_slug = org_metadata.get("skool_slug")

    # --- Preferred order: use last_successful_scrape_account if present ---
    preferred = org_metadata.get("last_successful_scrape_account", None)

    account_order = list(SCRAPE_ACCOUNTS.keys())

    if preferred in account_order:
        account_order.remove(preferred)
        account_order.insert(0, preferred)

    build_id = get_build_id()

    for account_key in account_order:
        account = SCRAPE_ACCOUNTS[account_key]
        print(
            f"[TOKEN] Trying account '{account_key}' for skool_slug '{skool_slug}'..."
        )
        is_admin = is_admin_in_group_func(
            skool_slug, account["auth_token"], build_id, account["admin_skool_handle"]
        )
        print(f"[TOKEN]   -> is_admin: {is_admin}")
        if is_admin:
            if org_metadata.get("last_successful_scrape_account") != account_key:
                print(
                    f"[TOKEN] Updating last_successful_scrape_account to '{account_key}' in Clerk metadata for '{skool_slug}'"
                )
                clerk.update_organization_metadata(
                    org_slug, {"last_successful_scrape_account": account_key}, org_id
                )
            return account["auth_token"], account["admin_skool_handle"], account_key
    # If neither works, log and return None
    print(
        f"[TOKEN] ERROR: No working scrape account found for skool_slug '{skool_slug}' (tried: {account_order})"
    )
    return None, None, None


def check_and_update_goose_admin_access(org, metadata, skool_slug, is_admin, clerk):
    has_goose_admin_access = metadata.get("has_goose_admin_access")
    print(
        f"is_admin: {is_admin} | skool_slug: {skool_slug} | has_goose_admin_access: {has_goose_admin_access}"
    )
    if has_goose_admin_access != is_admin:
        print(f"🔄 Updating Clerk metadata for {skool_slug}...")
        clerk.update_organization_metadata(
            org["slug"], {"has_goose_admin_access": is_admin}, org["id"]
        )


if __name__ == "__main__":
    pass
