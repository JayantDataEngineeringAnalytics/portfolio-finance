# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Transformation — Credit Risk Dataset
# MAGIC
# MAGIC Cleans Bronze data, caps outliers, and derives 8 business columns.
# MAGIC
# MAGIC **Source:** `portfolio_finance.bronze.loans_raw`
# MAGIC **Target:** `portfolio_finance.silver.loans_clean`
# MAGIC
# MAGIC **Derived columns:**
# MAGIC - `default_flag` — Boolean (loan_status = 1)
# MAGIC - `prior_default_flag` — Boolean (cb_person_default_on_file = 'Y')
# MAGIC - `emp_length_years` — Employment length, capped at 50 (removes outliers like 123)
# MAGIC - `age_band` — 18-25 / 26-35 / 36-45 / 46-55 / 56+
# MAGIC - `emp_band` — <1yr / 1-3yr / 3-5yr / 5-10yr / 10+yr
# MAGIC - `rate_band` — Low (<8%) / Mid (8-12%) / High (12-16%) / Very High (16%+)
# MAGIC - `income_band` — Low / Mid / High / Top
# MAGIC - `income_percentile` — PERCENT_RANK() across all borrowers
# MAGIC - `loan_amount_band` — <$5K / $5-10K / $10-20K / $20K+
# MAGIC - `grade_risk_order` — Numeric 1-7 (A=1 lowest risk, G=7 highest)

# COMMAND ----------

spark.sql("USE CATALOG portfolio_finance")
from pyspark.sql import functions as F
from pyspark.sql.window import Window

SOURCE_TABLE = "portfolio_finance.bronze.loans_raw"
TARGET_TABLE = "portfolio_finance.silver.loans_clean"

df = spark.table(SOURCE_TABLE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Cast types, rename, cap outliers

# COMMAND ----------

df = df.select(
    F.col("person_age").cast("int").alias("person_age"),
    F.col("person_income").cast("double").alias("person_income"),
    F.col("person_home_ownership"),
    # cap emp_length at 50 years to remove data entry errors (e.g. 123)
    F.least(F.col("person_emp_length").cast("double"), F.lit(50)).alias("emp_length_years"),
    F.initcap(F.col("loan_intent")).alias("loan_intent"),
    F.col("loan_grade"),
    F.col("loan_amnt").cast("double").alias("loan_amount"),
    F.col("loan_int_rate").cast("double").alias("interest_rate"),
    (F.col("loan_status").cast("int") == 1).alias("default_flag"),
    F.col("loan_percent_income").cast("double").alias("loan_pct_income"),
    (F.col("cb_person_default_on_file") == "Y").alias("prior_default_flag"),
    F.col("cb_person_cred_hist_length").cast("int").alias("credit_hist_years"),
).filter(F.col("person_age").isNotNull() & F.col("loan_amount").isNotNull())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Derive band and risk columns

# COMMAND ----------

w_income = Window.orderBy("person_income")

df = df \
    .withColumn("age_band",
        F.when(F.col("person_age") < 26, "18-25")
         .when(F.col("person_age") < 36, "26-35")
         .when(F.col("person_age") < 46, "36-45")
         .when(F.col("person_age") < 56, "46-55")
         .otherwise("56+")) \
    .withColumn("emp_band",
        F.when(F.col("emp_length_years") < 1,  "<1yr")
         .when(F.col("emp_length_years") < 3,  "1-3yr")
         .when(F.col("emp_length_years") < 5,  "3-5yr")
         .when(F.col("emp_length_years") < 10, "5-10yr")
         .otherwise("10+yr")) \
    .withColumn("rate_band",
        F.when(F.col("interest_rate") < 8,  "Low (<8%)")
         .when(F.col("interest_rate") < 12, "Mid (8-12%)")
         .when(F.col("interest_rate") < 16, "High (12-16%)")
         .otherwise("Very High (16%+)")) \
    .withColumn("income_band",
        F.when(F.col("person_income") < 30000,  "Low")
         .when(F.col("person_income") < 60000,  "Mid")
         .when(F.col("person_income") < 100000, "High")
         .otherwise("Top")) \
    .withColumn("income_percentile",
        F.percent_rank().over(w_income) * 100) \
    .withColumn("loan_amount_band",
        F.when(F.col("loan_amount") < 5000,  "<$5K")
         .when(F.col("loan_amount") < 10000, "$5-10K")
         .when(F.col("loan_amount") < 20000, "$10-20K")
         .otherwise("$20K+")) \
    .withColumn("grade_risk_order",
        F.when(F.col("loan_grade") == "A", 1)
         .when(F.col("loan_grade") == "B", 2)
         .when(F.col("loan_grade") == "C", 3)
         .when(F.col("loan_grade") == "D", 4)
         .when(F.col("loan_grade") == "E", 5)
         .when(F.col("loan_grade") == "F", 6)
         .otherwise(7))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Write Silver table

# COMMAND ----------

(
    df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

count = spark.table(TARGET_TABLE).count()
print(f"✓ {TARGET_TABLE} written — {count:,} rows")

# COMMAND ----------

# Validate default rate
spark.table(TARGET_TABLE).selectExpr(
    "COUNT(*) AS rows",
    "ROUND(AVG(CAST(default_flag AS INT))*100,1) AS default_rate_pct",
    "ROUND(AVG(interest_rate),2) AS avg_rate",
    "ROUND(AVG(loan_amount),0) AS avg_loan"
).display()
print("✓ Silver transformation complete")
