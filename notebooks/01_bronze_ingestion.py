# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Ingestion — Credit Risk Dataset
# MAGIC
# MAGIC Reads raw CSV from the Unity Catalog volume and writes a Delta table with audit columns.
# MAGIC
# MAGIC **Source:** `/Volumes/portfolio_finance/landing_zone/raw_files/credit_risk_dataset.csv`
# MAGIC **Target:** `portfolio_finance.bronze.loans_raw`

# COMMAND ----------

spark.sql("USE CATALOG portfolio_finance")
spark.sql("USE SCHEMA bronze")

SOURCE_PATH = "/Volumes/portfolio_finance/landing_zone/raw_files/credit_risk_dataset.csv"
TARGET_TABLE = "portfolio_finance.bronze.loans_raw"

# COMMAND ----------

from pyspark.sql import functions as F

df_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(SOURCE_PATH)
)

print(f"Rows: {df_raw.count():,}")
print(f"Columns: {len(df_raw.columns)}")
df_raw.printSchema()

# COMMAND ----------

df_bronze = df_raw.withColumn("_ingested_at", F.current_timestamp()) \
                  .withColumn("_source_file", F.lit(SOURCE_PATH)) \
                  .withColumn("_pipeline_version", F.lit("1.0"))

# COMMAND ----------

(
    df_bronze.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

count = spark.table(TARGET_TABLE).count()
print(f"✓ {TARGET_TABLE} written — {count:,} rows")

# COMMAND ----------

spark.table(TARGET_TABLE).limit(5).display()

# COMMAND ----------

# Null check
from pyspark.sql.functions import col, sum as spark_sum, when

key_cols = ["person_age","person_income","person_home_ownership","person_emp_length",
            "loan_intent","loan_grade","loan_amnt","loan_int_rate","loan_status",
            "loan_percent_income","cb_person_default_on_file","cb_person_cred_hist_length"]

df_bronze.select(
    [spark_sum(when(col(c).isNull(), 1).otherwise(0)).alias(c) for c in key_cols]
).display()
print("✓ Bronze ingestion complete")
