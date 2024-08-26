from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, BooleanType, TimestampType

TABLE_SCHEMA = StructType([
        StructField("rank", IntegerType()),
        StructField("id", StringType(),False),
        StructField("subreddit", StringType()),
        StructField("permalink", StringType()),
        StructField("author", StringType()),
        StructField("title", StringType()),
        StructField("score", IntegerType()),
        StructField("upvote_ratio", FloatType()),
        StructField("num_comments", IntegerType()),
        StructField("author_flair_text", StringType()),
        StructField("created_utc", FloatType()),
        StructField("over_18", BooleanType()),
        StructField("edited", StringType()),
        StructField("stickied", BooleanType()),
        StructField("locked", BooleanType()),
        StructField("is_original_content", BooleanType()),
        StructField("snapshot_time(UTC)", TimestampType())
    ])