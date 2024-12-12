import pandas as pd
import hashlib
import os
import logging
from typing import List
import sys
import json
from tweetfinder import Article
from downloader import download_to_dir, url_hash

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.propagate = False
logging.getLogger('readability.readability').setLevel(logging.WARNING)


def extract_embedded_tweets(urls: List[str], dest_dir: str):
    logger.info(f"Processing to {dest_dir}: {len(urls)}")
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)
    processed_count = 0
    cached_count = 0
    missing_count = 0
    embedded_tweet_count = 0
    mentioned_tweet_count = 0
    embedded_story_count = 0
    mentioned_story_count = 0
    for url in urls:
        hash = url_hash(url)
        html_file_path = f'data/html-cache/{hash}.html'
        if not os.path.exists(html_file_path):
            logger.debug(f"  File not found for {url}")
            missing_count += 1
            continue
        # load that cached HTML
        with open(html_file_path, 'r') as f:
            this_html = f.read()
        # generate or load the tweet data
        tweets_file_path = f'{dest_dir}/{hash}.json'
        if not os.path.exists(tweets_file_path):
            embedded_tweets = []
            mentioned_tweets = []
            try:
                this_article = Article(url=url, html=this_html)
                embedded_tweets = this_article.list_embedded_tweets()
                mentioned_tweets = this_article.list_mentioned_tweets()
            except Exception as e:
                logger.debug(f"  Error processing {url}: {e}")
                continue
            results = {
                'url': url,
                'embedded_tweets': embedded_tweets,
                'mentioned_tweets': mentioned_tweets
            }
            with open(tweets_file_path, 'w') as json_file:
                json.dump(results, json_file, indent=4)
                processed_count += 1
        else:
            with open(tweets_file_path, 'r') as f:
                results = json.load(f)
                cached_count += 1
        embedded_tweet_count += len(results['embedded_tweets'])
        mentioned_tweet_count += len(results['mentioned_tweets'])
        if len(results['embedded_tweets']) > 0:
            embedded_story_count += 1
        if len(results['mentioned_tweets']) > 0:
            mentioned_story_count += 1
    logger.info(f"  Processing complete: {processed_count} new, {cached_count} cached, {missing_count} fetch failed")
    logger.info(f"  embedded={embedded_tweet_count}, mentioned_tweet_count={mentioned_tweet_count}")
    return dict(embeds=embedded_tweet_count, mentions=mentioned_tweet_count,
                embed_docs=embedded_story_count, mention_docs=mentioned_story_count,
                pct_with_embeds=embedded_story_count/(processed_count+cached_count), 
                pct_with_mentions=mentioned_story_count/(processed_count+cached_count), 
                processed=processed_count, cached=cached_count, missing=missing_count)


MC_STORY_LIST_DIR = 'data/sample-story-urls'

# load list of files contianing URLs for each selected day
selected_date_file_paths = sorted([f"{MC_STORY_LIST_DIR}/{fn}" for fn in os.listdir(MC_STORY_LIST_DIR) if fn.endswith('.csv')])
logger.info(f"Found {len(selected_date_file_paths)} dates to study in {MC_STORY_LIST_DIR}")

# download all the HTML that you can (caching locally)
date_to_sample_df = {}
all_urls = []
for file_path in selected_date_file_paths:
    the_date = file_path.split('/')[-1][0:10]
    df = pd.read_csv(file_path)
    date_to_sample_df[the_date] = df
    day_urls = df['url'].tolist()
    all_urls += day_urls
logger.info(f"Picked {len(all_urls)} URLs to download")
download_to_dir(all_urls, f'data/html-cache/')

# process for embedded tweets (caching locally)
results = []
for file_path in selected_date_file_paths:
    the_date = file_path.split('/')[-1][0:10]
    df = date_to_sample_df[the_date]
    urls = df['url'].tolist()
    counts = extract_embedded_tweets(urls, f'data/tweet-info/{the_date}')
    counts['date'] = the_date
    results.append(counts)

df = pd.DataFrame(results)
df.to_csv('tweet-stats.csv', index=False)
