"""Main module."""

from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.functions import count, mean, stddev, min, max

def remove_nulls(df: DataFrame, column_name: str):
    """
    Removes null values from a PySpark dataframe.
    
    :param df: PySpark dataframe
    :return: PySpark dataframe without null values
    """
    return df.filter(col(column_name).isNotNull())


def rename_columns(df, old_col_name, new_col_name):
    """
    Renames a column in a PySpark dataframe.
    
    :param df: PySpark dataframe
    :param old_col_name: Current column name
    :param new_col_name: New column name
    :return: PySpark dataframe with renamed column
    """
    return df.withColumnRenamed(old_col_name, new_col_name)


def calculate_summary_stats(df, numeric_cols):
    """
    Calculates summary statistics for numeric columns in a PySpark dataframe.
    
    :param df: PySpark dataframe
    :param numeric_cols: List of numeric column names
    :return: PySpark dataframe with summary statistics
    """
    summary_stats = df.select([count(col(c)).alias(c + "_count") for c in numeric_cols] +
                              [mean(col(c)).alias(c + "_mean") for c in numeric_cols] +
                              [stddev(col(c)).alias(c + "_stddev") for c in numeric_cols] +
                              [min(col(c)).alias(c + "_min") for c in numeric_cols] +
                              [max(col(c)).alias(c + "_max") for c in numeric_cols])
    return summary_stats