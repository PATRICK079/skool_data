from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import json
import time
import os
from dotenv import load_dotenv



## Loading my  environment variables which are stored in my .env
load_dotenv()
group_slug = os.getenv("GROUP_SLUG")
auth_token = os.getenv("AUTH_TOKEN")


base_url = f"https://www.skool.com/{group_slug}?p="

# Setup  headless Driver
options = Options()
options.add_argument("--headless")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=options)

driver.get(f"https://www.skool.com/{group_slug}")
driver.add_cookie({
    "name": "auth_token",
    "value": auth_token,
    "domain": "www.skool.com"
})

all_posts = []
page = 88

while True:
    url = f"{base_url}{page}"
    print(f"Loading page {page}: {url}")
    driver.get(url)
    time.sleep(3)

    soup = BeautifulSoup(driver.page_source, "html.parser")
    script_tag = soup.find("script", id="__NEXT_DATA__")

    if not script_tag:
        print("No __NEXT_DATA__ found. Stopping.")
        break

    data = json.loads(script_tag.string)

    post_trees = data.get("props", {}).get("pageProps", {}).get("postTrees", [])
    if not post_trees:
        print("No posts found on this page. Finished scraping.")
        break

    # Find all post containers for category extraction
    cat_posts = soup.find_all("div", class_="styled__PostItemWrapper-sc-e4ns84-7")

    # Extract categories for each post, keep them in a list aligned with post_trees order
    categories = []
    for post_div in cat_posts:
        category_tag = post_div.find("div", class_="styled__GroupFeedLinkLabel-sc-vh0utx-10")
        category = category_tag.get_text(strip=True) if category_tag else "No category"
        categories.append(category)

    # Loop through posts and assign categories by index
    for idx, tree in enumerate(post_trees):
        post = tree.get("post", {})
        user = post.get("user", {})
        metadata = post.get("metadata", {})

        first_name = user.get("firstName", "")
        last_name = user.get("lastName", "")
        author_name = f"{first_name} {last_name}".strip()

        #  Parse contributors string (JSON) 
        commented_by = []
        contributors_raw = metadata.get("contributors", "[]")
        try:
            contributors = json.loads(contributors_raw)
            for c in contributors:
                fname = c.get("first_name", "")
                lname = c.get("last_name", "")
                full = f"{fname} {lname}".strip()
                if full:
                    commented_by.append(full)
        except json.JSONDecodeError:
            
            commented_by = []

        # Get category for this post by index, fallback if index mismatch
        category = categories[idx] if idx < len(categories) else "No category"

        post_data = {
            "post_id": post.get("id"),
            "post_title": post.get("name"),
            "user_id": user.get("id"),
            "username": user.get("name", ""),
            "author_name": author_name,
            "content": metadata.get("content", ""),
            "likes": metadata.get("upvotes", 0),
            "comments_count": metadata.get("comments", 0),
            "timestamp": post.get("createdAt", ""),
            "commented_by": "; ".join(commented_by),
            "category": category
        }

        all_posts.append(post_data)

    print(f"Scraped {len(post_trees)} posts on page {page}")
    page += 1
    time.sleep(1.2)

driver.quit()

os.makedirs("output", exist_ok=True)
## save as json file
with open(f"output/{group_slug}_post_full.json", "w", encoding="utf-8") as f:
    json.dump(all_posts, f, ensure_ascii=False, indent=2)
