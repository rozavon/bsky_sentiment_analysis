import os
import time
import json
import requests
import pandas as pd
from atproto import Client
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get Bluesky credentials from environment variables
USERNAME = os.getenv("BSKY_HANDLE")
PASSWORD = os.getenv("BSKY_PASSWORD")

# # test login
# try:
#     client = Client()
#     client.login(USERNAME, PASSWORD)
#     print("Test login successful!")
# except Exception as e:
#     print(f"Authentication failed: {str(e)}")
#     print(f"Username used: {USERNAME}")
#     print("Please verify your credentials in the .env file")

# Constants
EXCEL_FILENAME = "bsky_posts.xlsx"
JSON_FILENAME = "bsky_posts.json"
IMAGE_FOLDER = "bsky_images"
MAX_RETRIES = 5  # Max retries for API errors
INITIAL_WAIT_TIME = 45  # Start with 45 seconds wait time

# Ensure image folder exists
os.makedirs(IMAGE_FOLDER, exist_ok=True)

def fetch_and_save_posts(keyword, num_posts):
    """Fetches posts from Bluesky based on a keyword and saves them to Excel and JSON files."""
    retry_attempts = 0
    wait_time = INITIAL_WAIT_TIME

    while retry_attempts < MAX_RETRIES:
        try:
            # Initialize client and login ONCE, outside the pagination loop
            client = Client()
            client.login(USERNAME, PASSWORD)
            print("Login successful!")

            # Initialize variables
            posts_data = []
            json_data = {keyword: {}}
            image_count = len(os.listdir(IMAGE_FOLDER)) + 1
            total_fetched = 0
            cursor = None

            while total_fetched < num_posts:
                time.sleep(wait_time)  # Rate limiting
                print(f"\nFetching {num_posts} posts for keyword: {keyword}")

                # Fetch search results
                params = {"q": keyword}
                if cursor:
                    params["cursor"] = cursor

                response = client.app.bsky.feed.search_posts(params)

                # Check if response contains posts
                if hasattr(response, "posts") and response.posts:
                    posts = response.posts
                    for post in posts:
                        if total_fetched >= num_posts:
                            break  # Stop if required number of posts is fetched

                        post_id = f"post_{total_fetched + 1}"
                        post_text = post.record.text if hasattr(post.record, "text") else "No content"
                        image_filenames = []

                        # Check for images in post.embed.images
                        if hasattr(post, "embed") and post.embed:
                            if hasattr(post.embed, "images") and post.embed.images:
                                for img in post.embed.images:
                                    image_url = img.fullsize if hasattr(img, "fullsize") else None
                                    if image_url:
                                        # Generate image filename
                                        image_filename = f"{image_count}.jpg"
                                        image_path = os.path.join(IMAGE_FOLDER, image_filename)

                                        # Download and save image
                                        img_data = requests.get(image_url).content
                                        with open(image_path, "wb") as img_file:
                                            img_file.write(img_data)

                                        print(f"Downloaded: {image_filename}")
                                        image_filenames.append(image_filename)
                                        image_count += 1  # Increment image counter

                            # Check for external thumbnails in embeds
                            elif hasattr(post.embed, "external") and hasattr(post.embed.external, "thumb"):
                                image_url = post.embed.external.thumb
                                if image_url:
                                    image_filename = f"{image_count}.jpg"
                                    image_path = os.path.join(IMAGE_FOLDER, image_filename)

                                    # Download and save image
                                    img_data = requests.get(image_url).content
                                    with open(image_path, "wb") as img_file:
                                        img_file.write(img_data)

                                    print(f"Downloaded: {image_filename}")
                                    image_filenames.append(image_filename)
                                    image_count += 1  # Increment image counter

                        # Store post data in DataFrame format
                        posts_data.append([post_id, post_text, ", ".join(image_filenames), keyword])

                        # Store post data in JSON format
                        json_data[keyword][post_id] = {
                            "content": post_text,
                            "images": image_filenames
                        }

                        total_fetched += 1  # Increment count of fetched posts

                    # Check if there's a next page
                    cursor = getattr(response, "cursor", None)
                    if not cursor:
                        print("No more posts available.")
                        break  # Exit loop if there are no more posts

                else:
                    print("No posts found in response or end of results reached.")
                    break

            # Convert to DataFrame
            new_df = pd.DataFrame(posts_data, columns=["ID", "Text", "Image Filename", "Keyword"])

            # Check if Excel file exists, then append or create new file
            if os.path.exists(EXCEL_FILENAME):
                existing_df = pd.read_excel(EXCEL_FILENAME)
                combined_df = pd.concat([existing_df, new_df], ignore_index=True)
                combined_df.to_excel(EXCEL_FILENAME, index=False)
                print(f"Data appended to {EXCEL_FILENAME}")
            else:
                new_df.to_excel(EXCEL_FILENAME, index=False)
                print(f"New file created: {EXCEL_FILENAME}")

            # Save to JSON file
            if os.path.exists(JSON_FILENAME):
                with open(JSON_FILENAME, "r", encoding="utf-8") as json_file:
                    existing_json_data = json.load(json_file)
            else:
                existing_json_data = {}

            # Merge new data with existing JSON data
            existing_json_data.update(json_data)

            with open(JSON_FILENAME, "w", encoding="utf-8") as json_file:
                json.dump(existing_json_data, json_file, indent=4, ensure_ascii=False)

            print(f"Data saved to {JSON_FILENAME}")
            return  # Exit function if successful

        except Exception as e:
            print(f"Error fetching data for {keyword}: {e}. Retrying in {wait_time} seconds...")
            time.sleep(wait_time)
            retry_attempts += 1
            wait_time *= 2  # Increase wait time for next retry
            continue

    print(f"Failed to fetch data for {keyword} after {MAX_RETRIES} attempts. Skipping...")

# List of keywords to search for
lst = [
    "nuclear energy",
    "small modular reactor",
    "nuclear reactor",
    "nuclear power plant",
    "nuclear plant",
    "nuclear policy",
    "nuclear energy policy",

]

# Process each keyword
for keyword in lst:
        fetch_and_save_posts(keyword, 400)

# for testing purpose
#fetch_and_save_posts("small modular reactor", num_posts=100)
