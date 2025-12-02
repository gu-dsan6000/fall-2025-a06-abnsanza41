#!/usr/bin/env python3
"""
Problem 1: Log-Level Distribution
Will Analyze the distribution of log levels from the data files. 
PySpark operations & simple aggregations.
"""

import logging
import sys
import time
from pyspark.sql.functions import regexp_extract, rand, input_file_name
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import os


#Will first test on the sample data and then move onto the full dataset on the cluster. 

# Configure logging with basicConfig

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s,p%(process)s,{%(filename)s:%(lineno)d},%(levelname)s,%(message)s",
)
logger = logging.getLogger(__name__)

# Creating the spark sess 
def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("Problem1")
        .config("spark.driver.memory", "4g")
        .config("spark.driver.maxResultSize", "2g")
        .config("spark.master", "local[*]")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )
    return spark


# Problem 1 soln function
def problem1_soln(spark):
    """Parsing the logs, extrating them & computing counts, sample, & the summary."""

    logger.info("Starting 1 log parsing...")

    # reading & loading the data from s3 bucket --> if this doesnt work will read straight from local.     
    local_path = "data/raw/"  
    logger.info(f" ---> Reading local log files from: {local_path}")    # Adding the path  

    df_log = spark.read.text(local_path)    

    # log level extract  using //  tried followed the output order.     
    logs = df_log.withColumn(        
        "log_level",        
        regexp_extract(col("value"), r"(INFO|WARN|ERROR|DEBUG)", 1)    
    )
    # Counts and samples of the log level lines     
    logs_filtered = logs.filter(col("log_level") != "")    
    counts_df = logs_filtered.groupBy("log_level").count()
    sample_df = logs_filtered.orderBy(rand()).limit(10)
    ## summary of hte text
    total_l = df_log.count()    
    total_levels = logs_filtered.count()    
    unique_levels = counts_df.count()
    # Converting the counts -->  pandas for the summary part.     
    counts_pd = counts_df.toPandas()

    os.makedirs("data/output", exist_ok=True)

    summary_path = f"data/output/problem1_summary.txt"
    with open(summary_path, "w") as f:
        f.write(f"Total log lines = : {total_l}\n")
        f.write(f"Total lines with the log levels = : {total_levels}\n")
        f.write(f"The Unique log levels found: {unique_levels}\n\n")
        f.write("Log level distribution:\n")
        for _, row in counts_pd.iterrows():
            lvl = row['log_level']
            cnt = row['count']
            perc = (cnt / total_levels) * 100
            f.write(f"  {lvl:<5}: {cnt:>10} ({perc:.2f}%)\n")
    logger.info("Summary file written.")
    # Saving the final outputs
    counts_df.coalesce(1).write.csv(
        "data/output/problem1_counts.csv",
        header=True,
        mode="overwrite"
    )
    sample_df.coalesce(1).write.csv(
        "data/output/problem1_sample.csv",
        header=True,
        mode="overwrite"
    )
    logger.info("The outputs for Problem 1 written to data/output/. succesfuly")
    return True



# Sampled from the nyc code problem1
def main():
    logger.info(" ========= Starting Problem 1: Log Level Distribution")
    # Parsing the CLI args
    bucket_name = None
    # no longer used
    logger.info("Running Problem 1 in LOCAL mode (no cluster, no S3).")
    #print(f"Using the S3 bucket created earlier: {bucket_name}")
    overall_start = time.time()
    # Creating the spark session
    spark = create_spark_session()
    # Running the soln Problem 1
    success = True
    try:
        problem1_soln(spark)
    except Exception as e:
        logger.exception(f"Error in Problem 1: {str(e)}")
        success = False
    spark.stop()
    elapsed = time.time() - overall_start
    print(f"\nElapsed time: {elapsed:.2f}s")
    print("===================================================")
    if success:
        print("✅ Problem 1 completed successfully!")
        print("Generated files:")
        print("  - problem1_counts.csv")
        print("  - problem1_sample.csv")
        print("  - problem1_summary.txt")
    else:
        print(" Problem 1 failed.")
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())