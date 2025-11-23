#!/usr/bin/env python3
"""
Problem 1: Log Level Distribution
Analyzing the distribution of log levels (INFO, WARN, ERROR, DEBUG) across all log files. 
The following will be used:  PySpark operations and simple aggregations.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import os


#Will first test on the sample data and then move onto the full dataset on the cluster. 