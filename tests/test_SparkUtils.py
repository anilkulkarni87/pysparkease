#!/usr/bin/env python

"""Tests for `pysparkease` package."""

from pyspark.sql import SparkSession

from pysparkease import SparkUtils 


def test_remove_nulls(spark):
    """Test that remove_nulls works fine"""
    accountsDf = spark.createDataFrame(
        data=[
            ("123456789", None),
            ("222222222", "202"),
            ("000000000", "302"),
        ],
        schema=["account_number", "business_line_id"],
    )
    resultDF = SparkUtils.remove_nulls(accountsDf, "business_line_id")
    assert resultDF.count() == 2

def test_rename_columns(spark):
    """Test for verifying that the column is renamed"""
    customerDF = spark.createDataFrame(
        data=[
        ("Henry","Jacob")
        ],
        schema = ["first_name","lastname"],
    )
    resultDF = SparkUtils.rename_columns(customerDF, "lastname", "last_name")
    assert "last_name:" in resultDF.schema.simpleString()