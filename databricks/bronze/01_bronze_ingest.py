# Databricks notebook source
spark.sql("CREATE DATABASE IF NOT EXISTS customer360_bronze")
spark.sql("USE customer360_bronze")

base_path = "/Volumes/workspace/default/customer-360-project"

customers_path = f"{base_path}/customers.csv"
invoices_path = f"{base_path}/invoices.csv"
events_path = f"{base_path}/events.csv"
marketing_path = f"{base_path}/marketing.csv"

# 2. Read the raw CSVs
customers_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(customers_path)
)

invoices_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(invoices_path)
)

events_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(events_path)
)

marketing_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(marketing_path)
)

# 3. Write them as Delta tables in the bronze layer
(
    customers_df.write
    .mode("overwrite")
    .format("delta")
    .saveAsTable("bronze_customers")
)

(
    invoices_df.write
    .mode("overwrite")
    .format("delta")
    .saveAsTable("bronze_invoices")
)

(
    events_df.write
    .mode("overwrite")
    .format("delta")
    .saveAsTable("bronze_events")
)

(
    marketing_df.write
    .mode("overwrite")
    .format("delta")
    .saveAsTable("bronze_marketing")
)


# COMMAND ran in sql (to show it works) - done on databricks 

# MAGIC %sql
# MAGIC SELECT * FROM workspace.customer360_bronze.bronze_customers LIMIT 10;
# MAGIC