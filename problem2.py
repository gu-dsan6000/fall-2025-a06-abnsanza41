#!/usr/bin/env python3

"""
Analyze cluster usage patterns to understand which clusters are most heavily used over time. 
Extract cluster IDs, application IDs, and application start/end times to 
create a time-series dataset suitable for visualization with Seaborn.
"""
import logging
import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import os
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql.functions import to_timestamp, min as spark_min, max as spark_max, count as spark_count

from pyspark.sql.functions import (
    regexp_extract, rand, input_file_name,
    col, count,
    to_timestamp, min as spark_min, max as spark_max, count as spark_count,
    when
)



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
        .appName("Problem2")
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



 ## Function to plot apps per cluster
def plot_apps_clu(cluster_pdf, output_path):
    """Plot applications-per-cluster bar chart."""
    plt.figure(figsize=(10, 6))
    sns.barplot(x="cluster_id", y="num_applications", data=cluster_pdf)
    plt.title("Number of Apps per Clusters")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()


# Plotting the duration density of the clusters. 

def plot_dur_density(apps_pdf, cluster_id, output_path):
    """Plotting histogram of duration for the clusters."""
    subset = apps_pdf[apps_pdf["cluster_id"] == cluster_id]

    plt.figure(figsize=(10, 6))
    sns.histplot(subset["duration_sec"], kde=True)
    plt.xscale("log")                
    plt.xlabel("Duration (s) (logarithmic scale)")
    plt.title(f"The Jobs Duration Dist Per (Cluster {cluster_id})")
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()


## Problem 2 soln function 

def problem2_soln(spark):
    logger.info("Starting Problem 2 Soln ----------------->")

    os.makedirs("data/output", exist_ok=True)


    #Loading the logs from my local
    data_path = "data/raw/"
    logger.info(f"Loading logs from {data_path}")

    #Reading every file -> as text 
    df = spark.read.text(data_path).withColumn("file_path", input_file_name())

    #Extracting the application_id & the cluster_id, app_number

    logger.info("Extracting apps & cluster IDs...")

    df = df.withColumn(
        "application_id",
        regexp_extract(col("file_path"), r"(application_\d+_\d+)", 1)
    )

    df = df.withColumn(
        "cluster_id",
        regexp_extract(col("application_id"), r"application_(\d+)_", 1)
    )

    df = df.withColumn(
        "app_number",
        regexp_extract(col("application_id"), r"_(\d+)$", 1)
    )

    #Extracting the timestamps
    # So the timeseries can be done. 
    logger.info("Extracting timestamps...")

    df = df.withColumn(
        "timestamp_raw",
        regexp_extract(col("value"), r"(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})", 1)
    )

    #parsing only the rows where timestamp_raw looks valid (length == 17)
    df = df.withColumn(
        "timestamp",
        when(col("timestamp_raw") != "", to_timestamp(col("timestamp_raw"), "yy/MM/dd HH:mm:ss"))
    )

    # Dropping the rows without valid timestamps for clearity
    df = df.filter(col("timestamp").isNotNull())


    #Now comuputing the start and end per app. 
    logger.info("Computing start & end times for each application...")

    apps = (
        df.groupBy("cluster_id", "application_id", "app_number")
        .agg(
            spark_min("timestamp").alias("start_time"),
            spark_max("timestamp").alias("end_time")
        )
    )

    # And getting the duration cols in secs. & saving the timeline 
    apps = apps.withColumn(
        "duration_sec",
        col("end_time").cast("long") - col("start_time").cast("long")
    )

    apps_pdf = apps.toPandas()
    apps_pdf.to_csv("data/output/problem2_timeline.csv", index=False)

    # creating the  cluster summary
    logger.info("Creating cluster summary...")

    clusters = (
        apps.groupBy("cluster_id")
        .agg(
            spark_count("*").alias("num_applications"),
            spark_min("start_time").alias("cluster_first_app"),
            spark_max("end_time").alias("cluster_last_app")
        )
    )

    cluster_pdf = clusters.toPandas()
    cluster_pdf.to_csv("data/output/problem2_cluster_summary.csv", index=False)


    # Stats summary file and then saving the viz
    logger.info("Writing stats summary...")

    stats_path = "data/output/problem2_stats.txt"
    total_clusters = cluster_pdf["cluster_id"].nunique()
    total_apps = len(apps_pdf)
    avg_apps = cluster_pdf["num_applications"].mean()

    # Writing the stats to the file.
    with open(stats_path, "w") as f:
        f.write(f"Total clusters = : {total_clusters}\n")
        f.write(f"Total Apps: {total_apps}\n")
        f.write(f"Avg apps per cluster: {avg_apps:.2f}\n\n")
        f.write("Most active clusters:\n")

        for _, row in cluster_pdf.sort_values("num_applications", ascending=False).iterrows():
            f.write(f"  Cluster {row['cluster_id']} → {row['num_applications']} apps\n")

    # The Viz of the plots. 
    logger.info("Creating visualizations...")

    plot_apps_clu(cluster_pdf, "data/output/problem2_bar_chart.png")

    largest_cluster = (
        cluster_pdf.sort_values("num_applications", ascending=False)
        ["cluster_id"].iloc[0]
    )

    plot_dur_density(
        apps_pdf,
        largest_cluster,
        "data/output/problem2_density_plot.png"
    )

    logger.info("Problem 2 completed successfully. ✅")
    return True

## Main function 

def main():
    logger.info("Running Problem 2 ----------")

    start = time.time()
    spark = create_spark_session()

    try:
        success = problem2_soln(spark)
    except Exception as e:
        logger.exception(f"ERROR: {str(e)}")
        success = False

    # Checking the success and stopping spark
    spark.stop()
    print(f"\nElapsed time: {time.time() - start:.2f}s")

    if success:
        print("✅ Problem 2 completed successfully!")
    else:
        print("Problem 2 failed.")

    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())