# Databricks notebook source
# MAGIC %md
# MAGIC # Workspace Setup — portfolio_finance Catalog
# MAGIC
# MAGIC Run this notebook **once** to provision all schemas and volumes required for the
# MAGIC Financial Risk & Credit Analytics pipeline.
# MAGIC Requires: Unity Catalog enabled, `portfolio_finance` catalog already exists.

# COMMAND ----------

spark.sql("SHOW CATALOGS").filter("catalog = 'portfolio_finance'").display()

# COMMAND ----------

spark.sql("""
    CREATE SCHEMA IF NOT EXISTS portfolio_finance.landing_zone
    COMMENT 'Landing zone for raw uploaded files — Credit Risk CSV and future datasets'
""")
print("✓ Schema portfolio_finance.landing_zone ready")

# COMMAND ----------

spark.sql("""
    CREATE VOLUME IF NOT EXISTS portfolio_finance.landing_zone.raw_files
    COMMENT 'External volume for raw CSV uploads — upload credit_risk_dataset.csv here before Bronze ingestion'
""")
print("✓ Volume portfolio_finance.landing_zone.raw_files ready")
print("  Upload path: /Volumes/portfolio_finance/landing_zone/raw_files/")

# COMMAND ----------

spark.sql("""
    CREATE SCHEMA IF NOT EXISTS portfolio_finance.bronze
    COMMENT 'Bronze layer — raw ingested Delta tables with audit columns, no transformations'
""")
print("✓ Schema portfolio_finance.bronze ready")

# COMMAND ----------

spark.sql("""
    CREATE SCHEMA IF NOT EXISTS portfolio_finance.silver
    COMMENT 'Silver layer — cleaned and enriched loan data with derived columns (bands, flags, percentiles)'
""")
print("✓ Schema portfolio_finance.silver ready")

# COMMAND ----------

spark.sql("""
    CREATE SCHEMA IF NOT EXISTS portfolio_finance.gold
    COMMENT 'Gold layer — reporting-ready aggregates, fact tables, and Credit Default Risk Model scores'
""")
print("✓ Schema portfolio_finance.gold ready")

# COMMAND ----------

schemas = spark.sql("SHOW SCHEMAS IN portfolio_finance").collect()
print("Schemas in portfolio_finance:")
for s in schemas:
    print(f"  • {s['databaseName']}")

# COMMAND ----------

print("=" * 60)
print("SETUP COMPLETE")
print("=" * 60)
print()
print("Upload the following file to:")
print("  /Volumes/portfolio_finance/landing_zone/raw_files/")
print()
print("  ✓ credit_risk_dataset.csv")
print()
print("Source: Kaggle — Credit Risk Dataset (laotse)")
print("Rows: 32,581 loans | Columns: 12")
print()
print("Then run notebooks in order:")
print("  01_bronze_ingestion.py")
print("  02_silver_transform.py")
print("  03_gold_aggregates.py")
print("  04_credit_default_risk_model.py")
print("  05_cross_tab_aggregates.py")
print("=" * 60)
