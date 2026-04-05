# Databricks notebook source
# MAGIC %md
# MAGIC # Cross-Tab Aggregates — Financial Risk Analytics
# MAGIC
# MAGIC Pre-computes cross-dimensional aggregates for HTML report.
# MAGIC
# MAGIC **Targets:**
# MAGIC - `portfolio_finance.gold.xtab_grade_risk` — grade × risk band matrix
# MAGIC - `portfolio_finance.gold.xtab_intent_grade` — loan intent × grade matrix
# MAGIC - `portfolio_finance.gold.xtab_income_grade` — income band × grade matrix

# COMMAND ----------

spark.sql("USE CATALOG portfolio_finance")
from pyspark.sql import functions as F

df_silver = spark.table("portfolio_finance.silver.loans_clean")
df_risk   = spark.table("portfolio_finance.gold.credit_default_risk_scores")

# COMMAND ----------

# 1. xtab_grade_risk
xtab_gr = df_risk.groupBy("loan_grade","grade_risk_order","risk_band").agg(
    F.count("*").alias("loans"),
    F.round(F.avg("credit_risk_score"),1).alias("avg_score"),
    F.round(F.avg(F.cast("default_flag","int"))*100,1).alias("default_rate"),
)
(xtab_gr.write.format("delta").mode("overwrite").option("overwriteSchema","true")
        .saveAsTable("portfolio_finance.gold.xtab_grade_risk"))
print(f"✓ xtab_grade_risk — {xtab_gr.count()} rows")
xtab_gr.orderBy("grade_risk_order","risk_band").display()

# COMMAND ----------

# 2. xtab_intent_grade
xtab_ig = df_silver.groupBy("loan_intent","loan_grade","grade_risk_order").agg(
    F.count("*").alias("loans"),
    F.round(F.avg(F.cast("default_flag","int"))*100,1).alias("default_rate"),
    F.round(F.avg("loan_amount"),0).alias("avg_loan"),
)
(xtab_ig.write.format("delta").mode("overwrite").option("overwriteSchema","true")
        .saveAsTable("portfolio_finance.gold.xtab_intent_grade"))
print(f"✓ xtab_intent_grade — {xtab_ig.count()} rows")

# COMMAND ----------

# 3. xtab_income_grade
xtab_inc = df_silver.groupBy("income_band","loan_grade","grade_risk_order").agg(
    F.count("*").alias("loans"),
    F.round(F.avg(F.cast("default_flag","int"))*100,1).alias("default_rate"),
    F.round(F.avg("interest_rate"),2).alias("avg_rate"),
)
(xtab_inc.write.format("delta").mode("overwrite").option("overwriteSchema","true")
         .saveAsTable("portfolio_finance.gold.xtab_income_grade"))
print(f"✓ xtab_income_grade — {xtab_inc.count()} rows")

# COMMAND ----------

print("=" * 50)
print("CROSS-TAB AGGREGATES COMPLETE")
print("=" * 50)
for t in ["xtab_grade_risk","xtab_intent_grade","xtab_income_grade"]:
    n = spark.table(f"portfolio_finance.gold.{t}").count()
    print(f"  portfolio_finance.gold.{t}: {n:,} rows")
