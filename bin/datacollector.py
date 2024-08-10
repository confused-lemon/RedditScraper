#python 3.9
import yaml
import praw, prawcore
import csv
from datetime import datetime, timezone
import logging, logging.config
from time import sleep, time
import os, sys
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '..'))
sys.path.append(project_root)
from cfg.logging_config import LOGGING_CONFIG
logging.config.dictConfig(LOGGING_CONFIG)

path = os.path.dirname(os.path.dirname(__file__))
logging.getLogger('collector')

def data_collection(username, password, client_id, client_secret, agent) -> list:
    reddit_session = praw.Reddit(
        username=username,
        password=password,
        client_id=client_id,
        client_secret=client_secret,
        user_agent=agent
    )
    
    #Collect top 100 posts on r/all
    post_data_list = []
    snapshot_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    rank=1
    for post in reddit_session.subreddit("all").hot(limit=100):
        post_data = [rank, post.id, post.subreddit, post.permalink, post.author, post.title.replace("'", "\\'").replace('"', '\\"'), post.score, post.upvote_ratio, post.num_comments,
        post.author_flair_text, post.created_utc, post.over_18, post.edited, post.stickied, post.locked, post.is_original_content, snapshot_time]
        post_data_list.append(post_data)
        rank+=1

    return post_data_list

with open(f'{path}/cfg/credentials.yaml','r') as cred_file:
    credentials = yaml.safe_load(cred_file)

    username = credentials["Reddit"]["username"]
    password = credentials["Reddit"]["password"]
    client_id = credentials["Reddit"]["client_id"]
    client_secret = credentials["Reddit"]["client_secret"]
    agent = credentials["Reddit"]["user_agent"]

try:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    post_data_list = data_collection(username, password, client_id, client_secret, agent)
except prawcore.exceptions.OAuthException as e:
    logging.exception(f"OAuthException occured : {e}")
    exit(1)
except Exception as e1:
    try:
        logging.warning(f"First attempt failed due to: {e1}")
        logging.info("Sleeping 5 minutes...")
        sleep(300) #5 min sleep
        logging.info("Starting second attempt..")
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        post_data_list = data_collection(username, password, client_id, client_secret, agent)
        logging.info("Second attempt completed successfully.")
    except Exception as e:
        logging.error(f"Aborting after two failed attempts. {e}")
        exit(1)

headers = ["rank", "id","subreddit","permalink","author","title","score","upvote_ratio","num_comments",
           "author_flair_text","created_utc","over_18","edited","stickied","locked","is_original_content", "snapshot_time(UTC)"]


with open(f"{path}/CSV_datasets/{timestamp}.csv", "w") as file:
    writer = csv.writer(file)
    writer.writerow(headers)
    writer.writerows(post_data_list)
