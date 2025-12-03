# Databricks notebook source
from pyspark.sql import functions as F

# 1. Create / use silver schema
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.customer360_silver")
spark.sql("USE workspace.customer360_silver")

bronze_customers = spark.table("workspace.customer360_bronze.bronze_customers")

dim_customer = (
    bronze_customers
    .select(
        F.col("customer_id").cast("long"),
        F.to_date("signup_date").alias("signup_date"),
        F.upper("region").alias("region"),
        F.lower("segment").alias("segment"),
        F.lower("email").alias("email"),
        F.col("company_name")
    )
    .dropDuplicates(["customer_id"])
)

dim_customer.write.mode("overwrite").format("delta").saveAsTable("dim_customer")

# invoices reformat
bronze_invoices = spark.table("workspace.customer360_bronze.bronze_invoices")

fact_invoices = (
    bronze_invoices
    .withColumn("customer_id", F.col("customer_id").cast("long"))
    .withColumn("invoice_date", F.to_date("invoice_date"))
    .withColumn("amount", F.col("amount").cast("double"))
    .withColumn("status", F.lower("status"))
    .withColumn("invoice_month", F.date_trunc("month", F.col("invoice_date")))
    .dropDuplicates(["invoice_id"])
)

fact_invoices.write.mode("overwrite").format("delta").saveAsTable("fact_invoices")

# events reformat
bronze_events = spark.table("workspace.customer360_bronze.bronze_events")

fact_events = (
    bronze_events
    .withColumn("customer_id", F.col("customer_id").cast("long"))
    .withColumn("event_timestamp", F.to_timestamp("event_timestamp"))
    .withColumn("event_date", F.to_date("event_timestamp"))
    .withColumn("event_type", F.lower("event_type"))
)

fact_events.write.mode("overwrite").format("delta").saveAsTable("fact_events")

# marketing reformat
bronze_marketing = spark.table("workspace.customer360_bronze.bronze_marketing")

fact_marketing = (
    bronze_marketing
    .withColumn("customer_id", F.col("customer_id").cast("long"))
    .withColumn("event_timestamp", F.to_timestamp("event_timestamp"))
    .withColumn("event_date", F.to_date("event_timestamp"))
    .withColumn("marketing_event", F.lower("marketing_event"))
)

fact_marketing.write.mode("overwrite").format("delta").saveAsTable("fact_marketing")

# COMMAND run in sql

# MAGIC %sql
# MAGIC SELECT * FROM workspace.customer360_silver.dim_customer LIMIT 10;