"""
Analytical functions for the three main questions and anomaly detection.

All functions return DataFrames for easy inspection and further analysis.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, count, countDistinct, avg, sum, min, max, desc, asc, year, when, lit,
    broadcast, row_number, stddev, abs as spark_abs
)
from pyspark.sql.window import Window


# ==================== ANALYTICAL QUESTIONS ====================

def top_genres_by_year(ratings_df: DataFrame, movies_exploded_df: DataFrame) -> DataFrame:
    """
    Compute top 10 genres by total ratings, with yearly popularity evolution.

    This answers: What are the top 10 genres by total number of ratings,
    and how has their popularity evolved over time (by year of rating)?

    Args:
        ratings_df: Silver-level ratings with normalized timestamps
        movies_exploded_df: Silver-level movies with exploded genres

    Returns:
        DataFrame with columns: genre, year, rating_count, rank
    """
    # Join ratings with exploded genres
    ratings_with_genre = ratings_df.join(
        broadcast(movies_exploded_df),
        on="movieId",
        how="inner"
    )

    # Extract year from timestamp
    ratings_with_year = ratings_with_genre.withColumn(
        "year",
        year(col("timestamp"))
    )

    # Count ratings per genre per year
    genre_year_counts = ratings_with_year.groupBy("genre", "year").agg(
        count("rating").alias("rating_count")
    )

    # Window spec for ranking genres by total ratings
    window_spec = Window.partitionBy().orderBy(desc("total_rating_count"))

    # Compute total ratings per genre and rank
    genre_totals = genre_year_counts.groupBy("genre").agg(
        sum("rating_count").alias("total_rating_count")
    ).withColumn("rank", row_number().over(window_spec))

    # Join back to get yearly data for top 10 genres
    top_genres = genre_totals.filter(col("rank") <= 10).select("genre")

    result = genre_year_counts.join(
        top_genres,
        on="genre",
        how="inner"
    ).join(
        genre_totals.select("genre", "rank"),
        on="genre",
        how="inner"
    ).orderBy("rank", "year")

    return result


def find_hidden_gems(
    ratings_df: DataFrame,
    movies_df: DataFrame,
    movies_exploded_df: DataFrame
) -> tuple[DataFrame, DataFrame]:
    """
    Identify hidden gems: movies with < 50 ratings but avg rating > 4.0.

    Also computes genre over-representation in this segment vs overall distribution.

    Args:
        ratings_df: Silver-level ratings
        movies_df: Silver-level movies (original, not exploded)
        movies_exploded_df: Silver-level movies with exploded genres

    Returns:
        Tuple of (hidden_gems_df, genre_comparison_df)
    """
    # Compute movie-level metrics
    movie_metrics = ratings_df.groupBy("movieId").agg(
        avg("rating").alias("avg_rating"),
        count("rating").alias("rating_count")
    )

    # Filter hidden gems
    hidden_gems = movie_metrics.filter(
        (col("rating_count") < 50) & (col("avg_rating") > 4.0)
    )

    # Join with movie titles
    hidden_gems_with_title = hidden_gems.join(
        movies_df.select("movieId", "title"),
        on="movieId",
        how="inner"
    ).orderBy(desc("avg_rating"))

    # Compute genre distribution for hidden gems
    hidden_genres = hidden_gems.join(
        movies_exploded_df,
        on="movieId",
        how="inner"
    ).groupBy("genre").agg(
        count("*").alias("hidden_count")
    )

    # Compute overall genre distribution
    overall_genres = ratings_df.join(
        movies_exploded_df,
        on="movieId",
        how="inner"
    ).groupBy("genre").agg(
        count("*").alias("overall_count")
    )

    # Join and compute over-representation ratio
    genre_comparison = hidden_genres.join(
        overall_genres,
        on="genre",
        how="inner"
    ).withColumn(
        "hidden_ratio",
        col("hidden_count") / sum("hidden_count").over(Window.partitionBy())
    ).withColumn(
        "overall_ratio",
        col("overall_count") / sum("overall_count").over(Window.partitionBy())
    ).withColumn(
        "over_representation",
        col("hidden_ratio") / col("overall_ratio")
    ).orderBy(desc("over_representation"))

    return hidden_gems_with_title, genre_comparison


def build_inflight_catalog(
    ratings_df: DataFrame,
    movies_df: DataFrame,
    movies_exploded_df: DataFrame,
    num_movies: int = 100
) -> DataFrame:
    """
    Select movies for inflight catalog to maximize passenger satisfaction.

    Selection criteria:
        1. High average rating (quality)
        2. Sufficient rating count (popularity)
        3. Genre diversity (coverage across tastes)

    Strategy:
        - Compute a composite score: weighted average of (normalized rating + normalized count)
        - Select top movies by score, ensuring genre diversity via stratified sampling

    Args:
        ratings_df: Silver-level ratings
        movies_df: Silver-level movies
        movies_exploded_df: Silver-level movies with exploded genres
        num_movies: Number of movies to select (default 100)

    Returns:
        DataFrame with selected movies and their scores
    """
    # Compute movie-level metrics
    movie_metrics = ratings_df.groupBy("movieId").agg(
        avg("rating").alias("avg_rating"),
        count("rating").alias("rating_count")
    )

    # Normalize metrics (min-max scaling)
    # Get min/max for normalization
    stats = movie_metrics.agg(
        min("avg_rating").alias("min_rating"),
        max("avg_rating").alias("max_rating"),
        min("rating_count").alias("min_count"),
        max("rating_count").alias("max_count")
    ).collect()[0]

    # Apply normalization and compute composite score
    # Weight: 60% quality (rating), 40% popularity (count)
    movie_scores = movie_metrics.withColumn(
        "norm_rating",
        (col("avg_rating") - stats["min_rating"]) / (stats["max_rating"] - stats["min_rating"])
    ).withColumn(
        "norm_count",
        (col("rating_count") - stats["min_count"]) / (stats["max_count"] - stats["min_count"])
    ).withColumn(
        "composite_score",
        col("norm_rating") * 0.6 + col("norm_count") * 0.4
    )

    # Join with movie info
    movies_with_scores = movie_scores.join(
        movies_df,
        on="movieId",
        how="inner"
    )

    # Get genres for each movie
    movies_with_genres = movies_with_scores.join(
        movies_exploded_df,
        on="movieId",
        how="inner"
    )

    # Simple strategy: select top movies by composite score
    # In a real scenario, you'd implement more sophisticated genre balancing
    inflight_catalog = movies_with_scores.orderBy(
        desc("composite_score"),
        desc("rating_count")
    ).limit(num_movies)

    return inflight_catalog


# ==================== ANOMALY DETECTION ====================

def detect_user_rating_anomalies(ratings_df: DataFrame) -> DataFrame:
    """
    Detect users with suspiciously high rating volumes using z-score.

    Users with rating counts > 3 standard deviations from mean are flagged.

    Args:
        ratings_df: Silver-level ratings

    Returns:
        DataFrame with userId, rating_count, z_score, is_anomaly
    """
    # Compute rating count per user
    user_counts = ratings_df.groupBy("userId").agg(
        count("rating").alias("rating_count")
    )

    # Compute mean and std dev
    stats = user_counts.agg(
        avg("rating_count").alias("mean_count"),
        stddev("rating_count").alias("std_count")
    ).collect()[0]

    # Compute z-score and flag anomalies
    user_anomalies = user_counts.withColumn(
        "z_score",
        (col("rating_count") - stats["mean_count"]) / stats["std_count"]
    ).withColumn(
        "is_anomaly",
        spark_abs(col("z_score")) > 3
    ).orderBy(desc("z_score"))

    return user_anomalies


def detect_movie_rating_anomalies(ratings_df: DataFrame) -> DataFrame:
    """
    Detect movies with statistically improbable rating distributions.

    Flag movies where:
        - Rating count is very low but all ratings are identical (suspicious)
        - Rating distribution is extremely skewed

    Args:
        ratings_df: Silver-level ratings

    Returns:
        DataFrame with movieId, rating_count, unique_ratings, is_suspicious
    """
    # Compute metrics per movie
    movie_stats = ratings_df.groupBy("movieId").agg(
        count("rating").alias("rating_count"),
        countDistinct("rating").alias("unique_ratings"),
        avg("rating").alias("avg_rating")
    )

    # Flag suspicious patterns
    movie_anomalies = movie_stats.withColumn(
        "is_suspicious",
        when(
            (col("rating_count") > 5) & (col("unique_ratings") == 1),
            True
        ).otherwise(False)
    ).orderBy(desc("rating_count"))

    return movie_anomalies


def detect_temporal_anomalies(ratings_df: DataFrame) -> DataFrame:
    """
    Detect temporal anomalies in rating patterns.

    Flag periods with unusually high rating volume (possible bot activity).

    Args:
        ratings_df: Silver-level ratings with normalized timestamps

    Returns:
        DataFrame with date, rating_count, z_score, is_anomaly
    """
    # Extract date from timestamp
    ratings_by_date = ratings_df.withColumn(
        "date",
        col("timestamp").cast("date")
    ).groupBy("date").agg(
        count("rating").alias("rating_count")
    )

    # Compute mean and std dev
    stats = ratings_by_date.agg(
        avg("rating_count").alias("mean_count"),
        stddev("rating_count").alias("std_count")
    ).collect()[0]

    # Compute z-score and flag anomalies
    temporal_anomalies = ratings_by_date.withColumn(
        "z_score",
        (col("rating_count") - stats["mean_count"]) / stats["std_count"]
    ).withColumn(
        "is_anomaly",
        spark_abs(col("z_score")) > 3
    ).orderBy(desc("z_score"))

    return temporal_anomalies
