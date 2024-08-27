CREATE TABLE main_post_data(
    id VARCHAR(16),
    rank INT,
    subreddit VARCHAR(25),
    permalink VARCHAR(100),
    author VARCHAR(40),
    title VARCHAR(300),
    score INT,
    upvote_ratio FLOAT,
    num_comments INT,
    author_flair_text VARCHAR(64),
    created_utc FLOAT,
    over_18 BOOLEAN,
    edited VARCHAR(15),
    stickied BOOLEAN,
    locked BOOLEAN,
    is_original_content BOOLEAN,
    snapshot_time_utc TIMESTAMP
);
