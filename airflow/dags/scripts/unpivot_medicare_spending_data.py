from pyspark.sql import SparkSession
from airflow.hooks.base import BaseHook
from pyspark.sql.functions import explode, col, lit
import json
from typing import List, Set
import logging

def get_processed_files(spark: SparkSession, bucket: str) -> Set[str]:
    """Read manifest of processed files"""
    try:
        manifest_df = spark.read.json(f"s3a://{bucket}/medicare/manifest/processed_files.json")
        return set(manifest_df.select("processed_files").first()[0])
    except:
        return set()

# def update_manifest(spark: SparkSession, bucket: str, processed_files: List[str]):
#     """Update manifest with newly processed files"""
#     manifest_data = [{"processed_files": list(processed_files)}]
#     manifest_df = spark.createDataFrame(manifest_data)
#     manifest_df.write.mode("overwrite").json(f"s3a://{bucket}/medicare/manifest/processed_files.json")


def update_manifest(spark: SparkSession, bucket: str, processed_files: List[str]):
    """Update manifest with newly processed files"""
    manifest_data = [{"processed_files": list(processed_files)}]
    manifest_df = spark.createDataFrame(manifest_data)
    
    # Coalesce to 1 partition and specify the filename
    manifest_df.coalesce(1) \
        .write \
        .mode("overwrite") \
        .option("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false") \  # Prevents _SUCCESS file
        .json(f"s3a://{bucket}/medicare/manifest/")
    
    # Optionally, rename the file to a specific name
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    src_path = spark._jvm.org.apache.hadoop.fs.Path(f"s3a://{bucket}/medicare/manifest/part-*.json")
    dst_path = spark._jvm.org.apache.hadoop.fs.Path(f"s3a://{bucket}/medicare/manifest/processed_files.json")
    fs.rename(src_path, dst_path)

def unpivot_medicare_spending_data():
    aws_conn = BaseHook.get_connection('s3_default')
    
    # Configure Spark with required packages and configs
    spark = SparkSession.builder \
        .appName("Medicare Data Processing") \
        .getOrCreate()

    bucket = "pharmaceutical-data-dashboard"
    
    # Get list of all input files
    input_files = spark.read.format("json") \
        .option("basePath", f"s3a://{bucket}/medicare/spending_data/") \
        .load(f"s3a://{bucket}/medicare/spending_data/*.json") \
        .inputFiles()

    # Get already processed files
    processed_files = get_processed_files(spark, bucket)
    
    # Filter for new files
    new_files = [f for f in input_files if f not in processed_files]
    
    if not new_files:
        logging.info("No new files to process")
        return
        
    # Read only new files
    df = spark.read.json(new_files)

    # Process the data
    years = ["2018", "2019", "2020", "2021", "2022"]
    base_cols = ["Brnd_Name", "Gnrc_Name", "Tot_Mftr", "Mftr_Name", 
                 "CAGR_Avg_Spnd_Per_Dsg_Unt_18_22", "Chg_Avg_Spnd_Per_Dsg_Unt_21_22"]
    
    dfs = []
    for year in years:
        metric_cols = [
            "Tot_Spndng",
            "Tot_Dsg_Unts", 
            "Tot_Clms",
            "Tot_Benes",
            "Avg_Spnd_Per_Dsg_Unt_Wghtd",
            "Avg_Spnd_Per_Clm",
            "Avg_Spnd_Per_Bene",
            "Outlier_Flag"
        ]
        
        year_cols = [f"{col}_{year}" for col in metric_cols]
        
        year_df = df.select(
            *base_cols,
            *[col(c).alias(c.replace(f"_{year}", "")) for c in year_cols]
        ).withColumn("year", lit(year))
        
        dfs.append(year_df)

    # Import needed for reduce operation
    from functools import reduce
    from pyspark.sql import DataFrame
    
    final_df = reduce(DataFrame.unionAll, dfs)

    final_df = final_df \
        .withColumn("Tot_Spndng", col("Tot_Spndng").cast("float")) \
        .withColumn("Tot_Dsg_Unts", col("Tot_Dsg_Unts").cast("integer")) \
        .withColumn("Tot_Clms", col("Tot_Clms").cast("integer")) \
        .withColumn("Tot_Benes", col("Tot_Benes").cast("integer")) \
        .withColumn("Avg_Spnd_Per_Dsg_Unt_Wghtd", col("Avg_Spnd_Per_Dsg_Unt_Wghtd").cast("float")) \
        .withColumn("Avg_Spnd_Per_Clm", col("Avg_Spnd_Per_Clm").cast("float")) \
        .withColumn("Avg_Spnd_Per_Bene", col("Avg_Spnd_Per_Bene").cast("float")) \
        .withColumn("Outlier_Flag", col("Outlier_Flag").cast("boolean")) \
        .withColumn("CAGR_Avg_Spnd_Per_Dsg_Unt_18_22", col("CAGR_Avg_Spnd_Per_Dsg_Unt_18_22").cast("float")) \
        .withColumn("Chg_Avg_Spnd_Per_Dsg_Unt_21_22", col("Chg_Avg_Spnd_Per_Dsg_Unt_21_22").cast("float"))

    # Write as Parquet with proper column names (optional: either rename columns here or map them in the snowflake external table definition)
    final_df.select(
        col("Brnd_Name").alias("brand_name"),
        col("Gnrc_Name").alias("generic_name"),
        col("Tot_Mftr").alias("total_manufacturer"),
        col("Mftr_Name").alias("manufacturer_name"),
        col("CAGR_Avg_Spnd_Per_Dsg_Unt_18_22").alias("cagr_avg_spend_per_dosage_unit_18_22"),
        col("Chg_Avg_Spnd_Per_Dsg_Unt_21_22").alias("change_avg_spend_per_dosage_unit_21_22"),
        col("Tot_Spndng").alias("total_spending"),
        col("Tot_Dsg_Unts").alias("total_dosage_units"),
        col("Tot_Clms").alias("total_claims"),
        col("Tot_Benes").alias("total_beneficiaries"),
        col("Avg_Spnd_Per_Dsg_Unt_Wghtd").alias("avg_spend_per_dosage_unit"),
        col("Avg_Spnd_Per_Clm").alias("avg_spend_per_claim"),
        col("Avg_Spnd_Per_Bene").alias("avg_spend_per_beneficiary"),
        col("Outlier_Flag").alias("outlier_flag"),
        col("year")
    ).write \
        .mode("append") \
        .parquet(f"s3a://{bucket}/medicare/processed/")

    # Update manifest with newly processed files
    processed_files.update(new_files)
    update_manifest(spark, bucket, processed_files)

if __name__ == "__main__":
    unpivot_medicare_spending_data() 