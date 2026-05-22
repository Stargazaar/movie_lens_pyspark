"""
Analytical functions for the three main questions and anomaly detection.

All functions return DataFrames for easy inspection and further analysis.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, count, avg, sum, min, max, desc, year,
    lit as spark_lit, broadcast, row_number, stddev, abs as spark_abs
)
from pyspark.sql.window import Window


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
        & (col("rating_count") >= 5)
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
    movies_with_year_df: DataFrame,
    movies_exploded_df: DataFrame,
    hidden_gems_df: DataFrame,
    cutoff_year: int
) -> DataFrame:
    """
    Select movies for inflight catalog to maximise passenger satisfaction.

    Three-section selection strategy:
        1. Recent Blockbusters (10) — top composite-scored movies released in the
           past year of the dataset, with at least 10 ratings.
        2. Hidden Gems (10) — top hidden gems (< 50 ratings, avg > 4.0).
        3. Genre-Weighted Fill (80) — remaining slots allocated proportionally
           across the top 10 genres by composite score, and 2 over-represented
           genres from earlier analyses.

    Movies are deduplicated between sections (earlier sections take priority).

    Args:
        ratings_df: Silver-level ratings
        movies_df: Silver-level movies (with genres column)
        movies_with_year_df: Silver-level movies with release_year column
        movies_exploded_df: Silver-level movies with exploded genres
        hidden_gems_df: Pre-computed hidden gems DataFrame (movieId, avg_rating, ...)
        cutoff_year: Minimum release year for recent blockbusters (e.g., latest_year - 1)

    Returns:
        DataFrame with movieId, title, genres, avg_rating, rating_count,
        composite_score, selection_reason
    """
    # --- Compute composite score per movie ---
    # Composite = 60% normalised avg_rating + 40% normalised rating_count
    movie_metrics = ratings_df.groupBy("movieId").agg(
        avg("rating").alias("avg_rating"),
        count("rating").alias("rating_count")
    )

    # Collect min/max for min-max normalisation
    stats = movie_metrics.agg(
        min("avg_rating").alias("min_rating"),
        max("avg_rating").alias("max_rating"),
        min("rating_count").alias("min_count"),
        max("rating_count").alias("max_count")
    ).collect()[0]

    # Apply normalisation using collected scalars as Python literals
    movies_with_scores = movie_metrics.withColumn(
        "composite_score",
        (col("avg_rating") - stats["min_rating"]) / (stats["max_rating"] - stats["min_rating"]) * 0.6
        + (col("rating_count") - stats["min_count"]) / (stats["max_count"] - stats["min_count"]) * 0.4
    ).join(
        movies_df.select("movieId", "title", "genres"),
        on="movieId", how="inner"
    )

    # --- Section 1: Recent Blockbusters (10) ---
    recent_blockbusters = (
        movies_with_scores
        .join(movies_with_year_df.select("movieId", "release_year"), on="movieId", how="inner")
        .filter((col("release_year") >= cutoff_year) & (col("rating_count") >= 10))
        .orderBy(desc("composite_score"), desc("rating_count"))
        .limit(10)
        .drop("release_year")
        .withColumn("selection_reason", spark_lit("recent_blockbuster"))
    )

    blockbuster_ids = [row["movieId"] for row in recent_blockbusters.select("movieId").collect()]

    # --- Section 2: Hidden Gems (10) ---
    # Take top 10 hidden gems not already in blockbusters
    gems_ids = [
        row["movieId"]
        for row in hidden_gems_df.select("movieId").collect()
        if row["movieId"] not in blockbuster_ids
    ][:10]

    hidden_gem_selection = (
        movies_with_scores
        .filter(col("movieId").isin(gems_ids))
        .withColumn("selection_reason", spark_lit("hidden_gem"))
    )

    # Combined exclusion set for section 3
    excluded_ids = list(set(blockbuster_ids + gems_ids))

    # --- Section 3: Genre-Weighted Fill (80) ---
    genre_allocation = {
        "Drama": 12, "Comedy": 12, "Action": 10, "Thriller": 8,
        "Adventure": 7, "Romance": 6, "Sci-Fi": 6, "Crime": 6,
        "Fantasy": 4, "Children": 5, "Documentary": 2, "Film-Noir": 2
    }

    # Rank movies per genre by composite score using a window function
    genre_window = Window.partitionBy("genre").orderBy(
        desc("composite_score"), desc("rating_count")
    )

    ranked_by_genre = (
        movies_with_scores
        .join(movies_exploded_df.select("movieId", "genre").distinct(), on="movieId", how="inner")
        .filter(~col("movieId").isin(excluded_ids))
        .withColumn("genre_rank", row_number().over(genre_window))
    )

    # For each genre, keep only movies up to the allocated slot count
    genre_fill_ids = set()
    for genre, slots in genre_allocation.items():
        picks = (
            ranked_by_genre
            .filter((col("genre") == genre) & (col("genre_rank") <= slots))
            .select("movieId")
            .collect()
        )
        for row in picks:
            genre_fill_ids.add(row["movieId"])

    # Backfill if cross-genre overlap reduced count below 80
    if len(genre_fill_ids) < 80:
        shortfall = 80 - len(genre_fill_ids)
        all_excluded = excluded_ids + list(genre_fill_ids)
        backfill = (
            movies_with_scores
            .filter(~col("movieId").isin(all_excluded))
            .orderBy(desc("composite_score"), desc("rating_count"))
            .limit(shortfall)
            .select("movieId")
            .collect()
        )
        for row in backfill:
            genre_fill_ids.add(row["movieId"])

    genre_fill_selection = (
        movies_with_scores
        .filter(col("movieId").isin(list(genre_fill_ids)))
        .withColumn("selection_reason", spark_lit("genre_fill"))
    )

    # --- Union all three sections ---
    output_cols = ["movieId", "title", "genres", "avg_rating", "rating_count",
                   "composite_score", "selection_reason"]

    catalog = (
        recent_blockbusters.select(*output_cols)
        .unionByName(hidden_gem_selection.select(*output_cols))
        .unionByName(genre_fill_selection.select(*output_cols))
    )

    return catalog

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

    Flag movies where rating variance is suspiciously low — i.e. the stddev
    of ratings is more than 3σ below the mean stddev across all movies.
    Only movies with at least 10 ratings are considered.

    Args:
        ratings_df: Silver-level ratings

    Returns:
        DataFrame with movieId, rating_count, avg_rating, rating_stddev,
        variance_z_score, is_suspicious
    """
    # Compute per-movie stats (only movies with enough ratings)
    movie_stats = ratings_df.groupBy("movieId").agg(
        count("rating").alias("rating_count"),
        avg("rating").alias("avg_rating"),
        stddev("rating").alias("rating_stddev")
    ).filter(col("rating_count") >= 10)

    # Compute mean and stddev of the per-movie stddevs
    variance_stats = movie_stats.agg(
        avg("rating_stddev").alias("mean_stddev"),
        stddev("rating_stddev").alias("std_stddev")
    ).collect()[0]

    # Flag movies whose rating stddev is > 3σ below the mean (unusually uniform)
    movie_anomalies = movie_stats.withColumn(
        "variance_z_score",
        (col("rating_stddev") - variance_stats["mean_stddev"]) / variance_stats["std_stddev"]
    ).withColumn(
        "is_suspicious",
        col("variance_z_score") < -3
    ).orderBy("variance_z_score")

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