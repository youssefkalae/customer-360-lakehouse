# Databricks notebook source
from pyspark.sql import functions as F

# 1. Create / use gold schema
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.customer360_gold")
spark.sql("USE workspace.customer360_gold")

dim_customer   = spark.table("workspace.customer360_silver.dim_customer")
fact_invoices  = spark.table("workspace.customer360_silver.fact_invoices")
fact_events    = spark.table("workspace.customer360_silver.fact_events")
fact_marketing = spark.table("workspace.customer360_silver.fact_marketing")

# Filter paid invoices only for revenue metrics
paid_invoices = fact_invoices.filter(F.col("status") == "paid")

# mrr by month
mrr_by_month = (
    paid_invoices
    .groupBy("invoice_month")
    .agg(F.round(F.sum("amount"), 2).alias("mrr"))
    .orderBy("invoice_month")
)

mrr_by_month.write.mode("overwrite").format("delta").saveAsTable("mrr_by_month")

# Define "churn month" as the last month a customer ever paid an invoice
last_invoice = (
    paid_invoices
    .groupBy("customer_id")
    .agg(F.max("invoice_month").alias("last_invoice_month"))
)

churn_by_month = (
    last_invoice
    .groupBy("last_invoice_month")
    .agg(F.countDistinct("customer_id").alias("churned_customers"))
    .withColumnRenamed("last_invoice_month", "churn_month")
    .orderBy("churn_month")
)

churn_by_month.write.mode("overwrite").format("delta").saveAsTable("churn_by_month")

#aggregates for customer360

# Revenue per customer
customer_revenue = (
    paid_invoices
    .groupBy("customer_id")
    .agg(
        F.round(F.sum("amount"), 2).alias("total_revenue"),
        F.count("*").alias("num_invoices"),
        F.max("invoice_date").alias("last_invoice_date")
    )
)

# Usage per customer
customer_usage = (
    fact_events
    .groupBy("customer_id")
    .agg(
        F.count("*").alias("total_events"),
        F.max("event_timestamp").alias("last_event_timestamp")
    )
)

# Marketing engagement score
marketing_agg = (
    fact_marketing
    .groupBy("customer_id")
    .agg(
        F.sum(F.when(F.col("marketing_event") == "email_open", 1).otherwise(0)).alias("email_opens"),
        F.sum(F.when(F.col("marketing_event") == "email_click", 2).otherwise(0)).alias("email_click_points")
    )
    .withColumn("marketing_score", F.col("email_opens") + F.col("email_click_points"))
)

# Join everything into a Customer 360 table
customer_360 = (
    dim_customer
    .join(customer_revenue, "customer_id", "left")
    .join(customer_usage, "customer_id", "left")
    .join(marketing_agg, "customer_id", "left")
)

# Simple churn risk flag: if no invoice in last 90 days => high
customer_360 = customer_360.withColumn(
    "churn_risk",
    F.when(
        F.col("last_invoice_date") < F.date_sub(F.current_date(), 90),
        F.lit("high")
    ).otherwise("low")
)

customer_360.write.mode("overwrite").format("delta").saveAsTable("customer_360")


# COMMAND run in sql cell 

# MAGIC %sql
# MAGIC SELECT * FROM workspace.customer360_gold.customer_360 LIMIT 10;
# MAGIC