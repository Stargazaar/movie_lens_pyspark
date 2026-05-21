"""
Data ingestion module for loading MovieLens CSV files.

This module provides functions to load each CSV file with explicit schemas.
Using explicit schemas instead of inferSchema avoids an extra pass over the data,
which is a common performance best practice in production pipelines.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, LongType, FloatType
)


def load_movies(spark: SparkSession, path: str = "data/raw/movies.csv") -> DataFrame:
    """
    Load movies.csv with explicit schema.

    Schema:
        movieId: int - Movie identifier
        title: string - Movie title with release year
        genres: string - Pipe-separated genre list (e.g., "Action|Adventure")

    Args:
        spark: SparkSession instance
        path: Path to movies.csv

    Returns:
        DataFrame with movies data
    """
    schema = StructType([
        StructField("movieId", IntegerType(), nullable=False),
        StructField("title", StringType(), nullable=False),
        StructField("genres", StringType(), nullable=False),
    ])

    df = spark.read.csv(path, header=True, schema=schema)
    return df


def load_ratings(spark: SparkSession, path: str = "data/raw/ratings.csv") -> DataFrame:
    """
    Load ratings.csv with explicit schema.

    Schema:
        userId: int - User identifier
        movieId: int - Movie identifier
        rating: float - Rating from 0.5 to 5.0 (half-star increments)
        timestamp: long - Unix timestamp (seconds since epoch)

    Args:
        spark: SparkSession instance
        path: Path to ratings.csv

    Returns:
        DataFrame with ratings data
    """
    schema = StructType([
        StructField("userId", IntegerType(), nullable=False),
        StructField("movieId", IntegerType(), nullable=False),
        StructField("rating", FloatType(), nullable=False),
        StructField("timestamp", LongType(), nullable=False),
    ])

    df = spark.read.csv(path, header=True, schema=schema)
    return df


def load_tags(spark: SparkSession, path: str = "data/raw/tags.csv") -> DataFrame:
    """
    Load tags.csv with explicit schema.

    Schema:
        userId: int - User identifier
        movieId: int - Movie identifier
        tag: string - User-generated tag
        timestamp: long - Unix timestamp (seconds since epoch)

    Args:
        spark: SparkSession instance
        path: Path to tags.csv

    Returns:
        DataFrame with tags data
    """
    schema = StructType([
        StructField("userId", IntegerType(), nullable=False),
        StructField("movieId", IntegerType(), nullable=False),
        StructField("tag", StringType(), nullable=False),
        StructField("timestamp", LongType(), nullable=False),
    ])

    df = spark.read.csv(path, header=True, schema=schema)
    return df


def load_links(spark: SparkSession, path: str = "data/raw/links.csv") -> DataFrame:
    """
    Load links.csv with explicit schema.

    Schema:
        movieId: int - MovieLens identifier
        imdbId: int - IMDB identifier
        tmdbId: int - TMDB identifier (may be null)

    Args:
        spark: SparkSession instance
        path: Path to links.csv

    Returns:
        DataFrame with external ID mappings
    """
    schema = StructType([
        StructField("movieId", IntegerType(), nullable=False),
        StructField("imdbId", IntegerType(), nullable=False),
        StructField("tmdbId", IntegerType(), nullable=True),  # tmdbId can be null
    ])

    df = spark.read.csv(path, header=True, schema=schema)
    return df
