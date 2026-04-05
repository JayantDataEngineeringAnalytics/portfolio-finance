# Databricks notebook source
# MAGIC %md
# MAGIC # Credit Default Risk Model — Financial Risk Analytics
# MAGIC
# MAGIC Scores each loan 0–100 using 7 weighted credit risk factors.
# MAGIC Validated against actual loan defaults.
# MAGIC
# MAGIC **Source:** `portfolio_finance.silver.loans_clean`
# MAGIC **Targets:**
# MAGIC - `portfolio_finance.gold.credit_default_risk_scores` — per-loan scores
# MAGIC - `portfolio_finance.gold.agg_risk_by_grade` — risk distribution per loan grade
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Scoring Factors (max raw = 120 pts)
# MAGIC
# MAGIC | Factor | Points | Condition |
# MAGIC |--------|--------|-----------|
# MAGIC | Loan Grade | 0–35 pts | A=0 → G=35 (stepped) |
# MAGIC | Prior Default on File | 25 pts | cb_person_default_on_file = 'Y' |
# MAGIC | High Debt-to-Income | 20 pts | DTI > 40%; 10 pts if 25–40% |
# MAGIC | Very High Interest Rate | 15 pts | rate ≥ 16%; 7 pts if 12–16% |
# MAGIC | Low Income Band | 10 pts | Low income; 5 pts if Mid |
# MAGIC | Short Credit History | 10 pts | < 2 yrs; 5 pts if 2–4 yrs |
# MAGIC | High-Risk Intent + Large Loan | 5 pts | Debt Consolidation/Medical > $15K |
# MAGIC
# MAGIC Normalized: `credit_risk_score = (raw_score / 120) * 100`
# MAGIC
# MAGIC Risk bands:
# MAGIC - Low: 0–24
# MAGIC - Medium: 25–49
# MAGIC - High: 50–74
# MAGIC - Critical: 75–100

# COMMAND ----------

spark.sql("USE CATALOG portfolio_finance")
from pyspark.sql import functions as F

df = spark.table("portfolio_finance.silver.loans_clean")
MAX_RAW = 120

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Compute factor scores

# COMMAND ----------

df_scored = df \
    .withColumn("score_grade",
        F.when(F.col("loan_grade") == "A", 0)
         .when(F.col("loan_grade") == "B", 5)
         .when(F.col("loan_grade") == "C", 10)
         .when(F.col("loan_grade") == "D", 20)
         .when(F.col("loan_grade") == "E", 25)
         .when(F.col("loan_grade") == "F", 30)
         .otherwise(35)) \
    .withColumn("score_prior_default",
        F.when(F.col("prior_default_flag"), 25).otherwise(0)) \
    .withColumn("score_dti",
        F.when(F.col("loan_pct_income") > 0.40, 20)
         .when(F.col("loan_pct_income") > 0.25, 10)
         .otherwise(0)) \
    .withColumn("score_rate",
        F.when(F.col("interest_rate") >= 16, 15)
         .when(F.col("interest_rate") >= 12, 7)
         .otherwise(0)) \
    .withColumn("score_income",
        F.when(F.col("income_band") == "Low", 10)
         .when(F.col("income_band") == "Mid", 5)
         .otherwise(0)) \
    .withColumn("score_cred_hist",
        F.when(F.col("credit_hist_years") < 2, 10)
         .when(F.col("credit_hist_years") < 4, 5)
         .otherwise(0)) \
    .withColumn("score_intent_risk",
        F.when(
            F.col("loan_intent").isin("Debtconsolidation","Medical") &
            (F.col("loan_amount") > 15000), 5
        ).otherwise(0))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Sum, normalize, band

# COMMAND ----------

df_scored = df_scored \
    .withColumn("raw_score",
        F.col("score_grade") + F.col("score_prior_default") + F.col("score_dti") +
        F.col("score_rate") + F.col("score_income") + F.col("score_cred_hist") +
        F.col("score_intent_risk")) \
    .withColumn("credit_risk_score",
        F.round((F.col("raw_score") / MAX_RAW) * 100, 1)) \
    .withColumn("risk_band",
        F.when(F.col("credit_risk_score") < 25, "Low")
         .when(F.col("credit_risk_score") < 50, "Medium")
         .when(F.col("credit_risk_score") < 75, "High")
         .otherwise("Critical"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Write credit_default_risk_scores

# COMMAND ----------

cols = [
    "person_age","person_income","income_band","person_home_ownership",
    "emp_length_years","loan_intent","loan_grade","grade_risk_order",
    "loan_amount","interest_rate","loan_pct_income","prior_default_flag",
    "credit_hist_years","default_flag","credit_risk_score","raw_score","risk_band",
    "score_grade","score_prior_default","score_dti","score_rate",
    "score_income","score_cred_hist","score_intent_risk"
]
(df_scored.select(cols).write.format("delta").mode("overwrite")
    .option("overwriteSchema","true")
    .saveAsTable("portfolio_finance.gold.credit_default_risk_scores"))
n = spark.table("portfolio_finance.gold.credit_default_risk_scores").count()
print(f"✓ gold.credit_default_risk_scores — {n:,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Validate: default rate by risk band

# COMMAND ----------

spark.table("portfolio_finance.gold.credit_default_risk_scores") \
    .groupBy("risk_band") \
    .agg(
        F.count("*").alias("loans"),
        F.round(F.count("*")*100.0/n, 1).alias("pct"),
        F.round(F.avg("credit_risk_score"),1).alias("avg_score"),
        F.round(F.avg(F.cast("default_flag","int"))*100,1).alias("actual_default_rate"),
    ).orderBy("avg_score").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. agg_risk_by_grade

# COMMAND ----------

agg_risk = spark.table("portfolio_finance.gold.credit_default_risk_scores") \
    .groupBy("loan_grade","grade_risk_order","risk_band") \
    .agg(
        F.count("*").alias("loans"),
        F.round(F.avg("credit_risk_score"),1).alias("avg_score"),
        F.round(F.avg(F.cast("default_flag","int"))*100,1).alias("actual_default_rate"),
    ).orderBy("grade_risk_order","avg_score")

(agg_risk.write.format("delta").mode("overwrite").option("overwriteSchema","true")
         .saveAsTable("portfolio_finance.gold.agg_risk_by_grade"))
n2 = spark.table("portfolio_finance.gold.agg_risk_by_grade").count()
print(f"✓ gold.agg_risk_by_grade — {n2:,} rows")
agg_risk.display()

# COMMAND ----------

print("=" * 60)
print("CREDIT DEFAULT RISK MODEL COMPLETE")
print("=" * 60)
print()
print("Factor weights (max 120 pts):")
print("  Loan Grade (A=0 → G=35 pts)      : up to 35 pts")
print("  Prior Default on File            : 25 pts")
print("  High Debt-to-Income (>40%)       : 20 pts")
print("  Very High Interest Rate (>=16%)  : 15 pts")
print("  Low Income Band                  : 10 pts")
print("  Short Credit History (<2yr)      : 10 pts")
print("  High-Risk Intent + Large Loan    :  5 pts")
print()
print("Risk bands: Low 0-24 | Medium 25-49 | High 50-74 | Critical 75-100")
