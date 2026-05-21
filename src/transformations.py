"""
Silver and Gold layer transformations.

Silver layer: Data cleansing and validation (deduplication, timestamp normalization, genre explosion)
Gold layer: Aggregated business-ready tables for analytics
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, from_unixtime, to_timestamp, explode, split, count, avg, countDistinct,
    when, lit, broadcast
)


# ==================== SILVER LAYER ====================

def deduplicate_ratings(ratings_df: DataFrame) -> DataFrame:
    """
    Remove duplicate ratings (same user, same movie, same timestamp).

    Duplicates can occur due to data collection errors or system bugs.
    We keep only one record per unique (userId, movieId, timestamp) combination.

    Args:
        ratings_df: Raw ratings DataFrame

    Returns:
        Deduplicated ratings DataFrame
    """
    return ratings_df.dropDuplicates(["userId", "movieId", "timestamp"])


def normalize_timestamps(ratings_df: DataFrame, tags_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Convert Unix timestamps (seconds since epoch) to Spark TimestampType.

    This enables time-based analysis (e.g., ratings by year, temporal anomalies).
    Using Spark's native timestamp type is more efficient than string manipulation.

    Args:
        ratings_df: Ratings DataFrame with timestamp as long
        tags_df: Tags DataFrame with timestamp as long

    Returns:
        Tuple of (ratings_df, tags_df) with timestamp columns converted
    """
    ratings_normalized = ratings_df.withColumn(
        "timestamp",
        to_timestamp(from_unixtime(col("timestamp")))
    )

    tags_normalized = tags_df.withColumn(
        "timestamp",
        to_timestamp(from_unixtime(col("timestamp")))
    )

    return ratings_normalized, tags_normalized


def explode_genres(movies_df: DataFrame) -> DataFrame:
    """
    Explode pipe-separated genres into individual rows.

    Movies can have multiple genres (e.g., "Action|Adventure|Sci-Fi").
    Exploding creates one row per movie-genre pair, enabling genre-level analytics.

    This is a common pattern in data warehousing for handling multi-valued attributes.

    Args:
        movies_df: Movies DataFrame with genres as pipe-separated string

    Returns:
        DataFrame with one row per movie-genre combination
    """
    # Split genres by pipe, then explode into separate rows
    movies_exploded = movies_df.withColumn(
        "genre",
        explode(split(col("genres"), r"\|"))
    )

    # Filter out "(no genres listed)" entries
    movies_exploded = movies_exploded.filter(col("genre") != "(no genres listed)")

    return movies_exploded


# ==================== GOLD LAYER ====================

def build_gold_table(
    ratings_df: DataFrame,
    tags_df: DataFrame,
    movies_exploded_df: DataFrame
) -> DataFrame:
    """
    Build gold-level aggregate table: metrics per movie per genre.

    Aggregations:
        - avg_rating: Average rating for this movie-genre combination
        - rating_count: Number of ratings for this movie-genre
        - tag_count: Number of tags for this movie-genre

    This table is optimized for analytical queries (e.g., top genres by rating).

    Performance note:
        - Broadcast join on movies because it's small (~9K rows vs 100K ratings)
        - Aggregate after join to reduce data volume early

    Args:
        ratings_df: Silver-level ratings (normalized timestamps, deduplicated)
        tags_df: Silver-level tags (normalized timestamps)
        movies_exploded_df: Silver-level movies with exploded genres

    Returns:
        Gold table with movie-genre level metrics
    """
    # Join ratings with exploded movies (broadcast hint for small dimension table)
    ratings_with_genre = ratings_df.join(
        broadcast(movies_exploded_df),
        on="movieId",
        how="inner"
    )

    # Aggregate ratings by movie and genre
    gold_ratings = ratings_with_genre.groupBy("movieId", "genre").agg(
        avg("rating").alias("avg_rating"),
        count("rating").alias("rating_count")
    )

    # Join tags with exploded movies
    tags_with_genre = tags_df.join(
        broadcast(movies_exploded_df),
        on="movieId",
        how="inner"
    )

    # Aggregate tags by movie and genre
    gold_tags = tags_with_genre.groupBy("movieId", "genre").agg(
        count("tag").alias("tag_count")
    )

    # Combine ratings and tags (left join to keep movies without tags)
    gold_table = gold_ratings.join(
        gold_tags,
        on=["movieId", "genre"],
        how="left"
    ).fillna(0, subset=["tag_count"])

    return gold_table
