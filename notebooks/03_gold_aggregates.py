# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Aggregates — Financial Risk & Credit Analytics
# MAGIC
# MAGIC Builds 7 reporting-ready aggregate tables from `silver.loans_clean`.
# MAGIC
# MAGIC **Tables created:**
# MAGIC 1. `fact_loans` — pass-through from Silver
# MAGIC 2. `agg_kpi_summary` — single-row headline KPIs
# MAGIC 3. `agg_default_by_grade` — default rate by loan grade A–G
# MAGIC 4. `agg_default_by_intent` — default rate by loan purpose
# MAGIC 5. `agg_default_by_income` — default rate by income band
# MAGIC 6. `agg_default_by_rate_band` — default rate by interest rate band
# MAGIC 7. `agg_default_by_age` — default rate by borrower age band
# MAGIC 8. `agg_loan_benchmarks` — loan amount & rate stats per grade

# COMMAND ----------

spark.sql("USE CATALOG portfolio_finance")
from pyspark.sql import functions as F
from pyspark.sql.functions import percentile_approx

SOURCE = "portfolio_finance.silver.loans_clean"
df = spark.table(SOURCE)
print(f"Loaded {df.count():,} rows from silver")

# COMMAND ----------

# 1. fact_loans
(df.write.format("delta").mode("overwrite").option("overwriteSchema","true")
   .saveAsTable("portfolio_finance.gold.fact_loans"))
print("✓ gold.fact_loans")

# COMMAND ----------

# 2. agg_kpi_summary
kpi = df.agg(
    F.count("*").alias("total_loans"),
    F.round(F.sum("loan_amount"), 0).alias("total_loan_value"),
    F.round(F.avg("loan_amount"), 0).alias("avg_loan_amount"),
    F.round(F.avg(F.cast("default_flag", "int")) * 100, 1).alias("default_rate_pct"),
    F.count(F.when(F.col("default_flag"), 1)).alias("total_defaults"),
    F.round(F.avg("interest_rate"), 2).alias("avg_interest_rate"),
    F.round(F.avg("loan_pct_income") * 100, 1).alias("avg_dti_pct"),
    F.count(F.when(F.col("prior_default_flag"), 1)).alias("borrowers_prior_default"),
    F.countDistinct("loan_grade").alias("loan_grades"),
    F.countDistinct("loan_intent").alias("loan_intents"),
)
(kpi.write.format("delta").mode("overwrite").option("overwriteSchema","true")
    .saveAsTable("portfolio_finance.gold.agg_kpi_summary"))
spark.table("portfolio_finance.gold.agg_kpi_summary").display()
print("✓ gold.agg_kpi_summary")

# COMMAND ----------

# 3. agg_default_by_grade
agg_grade = df.groupBy("loan_grade","grade_risk_order").agg(
    F.count("*").alias("total_loans"),
    F.count(F.when(F.col("default_flag"),1)).alias("defaults"),
    F.round(F.avg(F.cast("default_flag","int"))*100,1).alias("default_rate_pct"),
    F.round(F.avg("interest_rate"),2).alias("avg_interest_rate"),
    F.round(F.avg("loan_amount"),0).alias("avg_loan_amount"),
    F.round(F.avg("loan_pct_income")*100,1).alias("avg_dti_pct"),
).orderBy("grade_risk_order")
(agg_grade.write.format("delta").mode("overwrite").option("overwriteSchema","true")
          .saveAsTable("portfolio_finance.gold.agg_default_by_grade"))
agg_grade.display()
print("✓ gold.agg_default_by_grade")

# COMMAND ----------

# 4. agg_default_by_intent
agg_intent = df.groupBy("loan_intent").agg(
    F.count("*").alias("total_loans"),
    F.count(F.when(F.col("default_flag"),1)).alias("defaults"),
    F.round(F.avg(F.cast("default_flag","int"))*100,1).alias("default_rate_pct"),
    F.round(F.avg("loan_amount"),0).alias("avg_loan_amount"),
    F.round(F.avg("interest_rate"),2).alias("avg_interest_rate"),
    F.round(F.avg("loan_pct_income")*100,1).alias("avg_dti_pct"),
).orderBy("default_rate_pct", ascending=False)
(agg_intent.write.format("delta").mode("overwrite").option("overwriteSchema","true")
           .saveAsTable("portfolio_finance.gold.agg_default_by_intent"))
agg_intent.display()
print("✓ gold.agg_default_by_intent")

# COMMAND ----------

# 5. agg_default_by_income
from pyspark.sql.functions import when as sql_when
agg_income = df.groupBy("income_band").agg(
    F.expr("CASE income_band WHEN 'Low' THEN 1 WHEN 'Mid' THEN 2 WHEN 'High' THEN 3 ELSE 4 END").alias("sort_order"),
    F.count("*").alias("total_loans"),
    F.count(F.when(F.col("default_flag"),1)).alias("defaults"),
    F.round(F.avg(F.cast("default_flag","int"))*100,1).alias("default_rate_pct"),
    F.round(F.avg("person_income"),0).alias("avg_income"),
    F.round(F.avg("loan_amount"),0).alias("avg_loan_amount"),
    F.round(F.avg("interest_rate"),2).alias("avg_rate"),
).orderBy("sort_order")
(agg_income.write.format("delta").mode("overwrite").option("overwriteSchema","true")
           .saveAsTable("portfolio_finance.gold.agg_default_by_income"))
agg_income.display()
print("✓ gold.agg_default_by_income")

# COMMAND ----------

# 6. agg_default_by_rate_band
agg_rate = df.groupBy("rate_band").agg(
    F.expr("CASE rate_band WHEN 'Low (<8%)' THEN 1 WHEN 'Mid (8-12%)' THEN 2 WHEN 'High (12-16%)' THEN 3 ELSE 4 END").alias("sort_order"),
    F.count("*").alias("total_loans"),
    F.count(F.when(F.col("default_flag"),1)).alias("defaults"),
    F.round(F.avg(F.cast("default_flag","int"))*100,1).alias("default_rate_pct"),
    F.round(F.avg("interest_rate"),2).alias("avg_rate"),
    F.round(F.avg("loan_amount"),0).alias("avg_loan_amount"),
).orderBy("sort_order")
(agg_rate.write.format("delta").mode("overwrite").option("overwriteSchema","true")
         .saveAsTable("portfolio_finance.gold.agg_default_by_rate_band"))
agg_rate.display()
print("✓ gold.agg_default_by_rate_band")

# COMMAND ----------

# 7. agg_default_by_age
agg_age = df.groupBy("age_band").agg(
    F.expr("CASE age_band WHEN '18-25' THEN 1 WHEN '26-35' THEN 2 WHEN '36-45' THEN 3 WHEN '46-55' THEN 4 ELSE 5 END").alias("sort_order"),
    F.count("*").alias("total_loans"),
    F.count(F.when(F.col("default_flag"),1)).alias("defaults"),
    F.round(F.avg(F.cast("default_flag","int"))*100,1).alias("default_rate_pct"),
    F.round(F.avg("person_income"),0).alias("avg_income"),
    F.round(F.avg("loan_amount"),0).alias("avg_loan_amount"),
).orderBy("sort_order")
(agg_age.write.format("delta").mode("overwrite").option("overwriteSchema","true")
        .saveAsTable("portfolio_finance.gold.agg_default_by_age"))
agg_age.display()
print("✓ gold.agg_default_by_age")

# COMMAND ----------

# 8. agg_loan_benchmarks
agg_bench = df.groupBy("loan_grade","grade_risk_order").agg(
    F.count("*").alias("total_loans"),
    F.round(F.min("loan_amount"),0).alias("min_loan"),
    F.round(F.avg("loan_amount"),0).alias("avg_loan"),
    F.round(percentile_approx("loan_amount",0.5),0).alias("median_loan"),
    F.round(F.max("loan_amount"),0).alias("max_loan"),
    F.round(F.min("interest_rate"),2).alias("min_rate"),
    F.round(F.avg("interest_rate"),2).alias("avg_rate"),
    F.round(F.max("interest_rate"),2).alias("max_rate"),
).orderBy("grade_risk_order")
(agg_bench.write.format("delta").mode("overwrite").option("overwriteSchema","true")
          .saveAsTable("portfolio_finance.gold.agg_loan_benchmarks"))
agg_bench.display()
print("✓ gold.agg_loan_benchmarks")

# COMMAND ----------

print("=" * 50)
print("GOLD AGGREGATES COMPLETE")
print("=" * 50)
for t in ["fact_loans","agg_kpi_summary","agg_default_by_grade","agg_default_by_intent",
          "agg_default_by_income","agg_default_by_rate_band","agg_default_by_age","agg_loan_benchmarks"]:
    n = spark.table(f"portfolio_finance.gold.{t}").count()
    print(f"  portfolio_finance.gold.{t}: {n:,} rows")
