# 🍴 Databricks LakeFlow Declarative Pipelines — Restaurant Analytics Project

This repository showcases a complete **end-to-end Restaurant Analytics pipeline** built using **Databricks LakeFlow Declarative Pipelines (DLT)** and **Workflows**, following the modern Lakehouse Medallion Architecture.

The project demonstrates how to design a **reliable, declarative, and production-ready ETL pipeline** that ingests restaurant orders, menu metadata, and customer feedback, processes it using DLT, and powers AI-driven BI dashboards through Gold-layer materialized views.

---

## 🏗️ LakeFlow Architecture

This architecture outlines the full data lifecycle:

* **Auto Loader Ingestion**: Scalable landing zone for incremental data arrival.
* **DLT Pipeline**: Automated handling of Bronze → Silver → Gold transformations.
* **Data Quality Enforcement**: Built-in governance through **DLT Expectations**.
* **Business Aggregations**: Real-time KPI generation for restaurant performance.
* **Customer 360 Genie Workspace**: Interactive, AI-powered natural language insights.
* **Unified Governance**: Managed via **Unity Catalog**.

![DLT Pipeline](./assets/restaurant_dlt_architecture.png)

---

### 🔄 LakeFlow Declarative Architecture (Streaming + Materialized Views)

This pipeline combines **continuous streaming ingestion** and **incremental transformations** with optimized **materialized views**:

* **Streaming Tables (Landing → Bronze → Silver):** All early-stage transformations use `spark.readStream` and DLT streaming tables, enabling the pipeline to automatically process new orders and reservation files as they arrive in the cloud landing zone.
* **Materialized Views (Gold Layer):** The Gold layer is built using DLT Materialized Views, which maintain pre-aggregated, analytics-ready datasets (e.g., daily revenue, top-selling items) that refresh efficiently as upstream streaming tables update.

This design ensures high freshness, low latency, and strong reliability from raw transaction data to business insights.

---

## 🚀 Project Overview

This project simulates a high-velocity restaurant data engineering workflow using:

* **Streaming Ingestion**: Order transactions and menu master data.
* **Cleansing & Validation**: Expectations (e.g., non-null order IDs, valid price ranges).
* **Feature Engineering**: Derived attributes (order value, peak hour flags, prep time metrics).
* **Intelligent Reporting**: Business layer via **Materialized Views**.
* **Automated Orchestration**: Production scheduling through **Workflows**.
* **Interactive Analytics**: Databricks AI/BI Dashboard & **Genie Workspace** for prompt-based insights.

---

## 🧰 Tech Stack

| Layer | Technology |
| --- | --- |
| **Ingestion** | Databricks Auto Loader |
| **Processing** | LakeFlow — Delta Live Tables (DLT) |
| **Storage** | Databricks Volumes / Delta Lake |
| **Governance** | Unity Catalog |
| **Orchestration** | Databricks Workflows |
| **BI Reporting** | Databricks AI/BI Dashboard |
| **AI Insights** | Databricks Genie (Natural Language) |

---

## 🗂️ Dataset Description

The project uses a synthetic restaurant dataset structured as follows:

* **Customers**: Demographic information, loyalty status, and historical engagement.
* **Menu Items**: Offerings, categories, and pricing structures for historical tracking.
* **Orders**: Transaction-level data including IDs, timestamps, quantities, and totals.
* **Restaurants**: Location metadata, including addresses and operational capacities.
* **Reviews**: Feedback and star ratings for sentiment analysis and quality metrics.

Files are incrementally ingested using **Auto Loader** and processed through the DLT pipeline.

![Restaurant Dataset](./datasets/)

---

## 🪜 Lakehouse Medallion Architecture (Streaming + MVs)

### 🥉 Bronze — *Streaming Table*

* Ingested via Auto Loader (`cloudFiles`).
* Continuous streaming updates from raw CSV landing zones.
* Expectations applied on-the-fly to ensure raw data integrity.

### 🥈 Silver — *Streaming Table*

* Live transformations (tax calculations, category joins, order value derivation).
* Built on top of continuous Bronze output.

### 🥇 Gold — *Materialized Views (MV)*

* Pre-aggregated restaurant KPIs (Revenue, AOV, Peak Hours).
* Optimized for BI dashboards and Genie AI consumption.
* Automatically refreshed as upstream Streaming Tables update.

---

> ⭐ **Pipeline Design Summary**
> * **Landing = Streaming Table (Auto Loader)**
> * **Bronze = Streaming Table**
> * **Silver = Streaming Table**
> * **Gold = Materialized Views (MV)**
> 
> 
> This ensures your pipeline is **fully incremental** from raw ingestion to business reporting.

---

## 🔄 DLT Pipeline Lineage

Here is the exact pipeline executed on Databricks:

The lineage shows:

* **Incremental streaming ingestion** - **Bronze → Silver → Gold** - **Materialized views powering dashboards**

![DLT](./assets/restaurant_dlt_pipeline_project.png)

---

## ⚙️ Workflow Automation

End-to-end orchestration is implemented using **Databricks Workflows**:

The workflow ensures:

1. **DLT pipeline refresh** 2. **Dashboard auto-refresh** This creates a production-style continuous data pipeline for restaurant operations.

![Workflow](./assets/restaurant_project_workflow.png)

---

## 📊 Restaurant Analytics Dashboard

The final business intelligence layer is powered by a multi-dimensional **Databricks AI/BI Dashboard**. It transforms the Silver and Gold layer tables into a high-performance, interactive experience designed for both operations managers and marketing executives.

### 🏢 Executive Performance Summary

This view focuses on the "Big Picture" of the restaurant's financial and operational health:

* **Real-time Financial KPIs**: Instant visibility into **Total Revenue**, **Total Orders**, and **Average Order Value (AOV)** across all locations.
* **Temporal Sales Trends**: A time-series analysis comparing daily order volumes against revenue, helping identify cyclical patterns and seasonal growth.
* **Product Mix Analysis**:
* **Top-Performing Items**: A bar chart identifying the most popular menu items by quantity sold.
* **Category Contribution**: A breakdown of revenue by category (Main Course, Starters, Desserts, Beverages) to optimize menu engineering and inventory.
* **Heatmap Analysis**: An "Order Density" matrix that maps transaction volume by hour of day and day of the week, crucial for optimizing staff shifts and kitchen prep.

![Dashboard1](./assets/overall_performance_insights_dashboard.png)

### 💬 Customer Sentiment & Review Intelligence

Going beyond transaction numbers, this page utilizes the **Reviews** and **Customers** datasets to monitor brand health:

* **Sentiment Segmentation**: An automated classification of reviews into **Positive**, **Neutral**, and **Negative** buckets.
* **Geographic Rating Analysis**: A regional breakdown (e.g., City-level performance) that calculates average star ratings to identify high and low-performing locations.
* **Review Distribution**: A granular look at the 1–5 star spread, providing a statistical view of customer satisfaction.
* **Qualitative Issue Tracking**:
* **Keyword Analysis**: Identification of common pain points such as "Portion Size," "Delivery Time," and "Food Quality."
* **Live Feedback Feed**: A filtered table showing the most recent reviews, allowing for immediate follow-up on negative experiences.

![Dashboard2](./assets/customer_review_analysis_dashboard.png)

---

## 🧞 Databricks Genie: Customer 360

Databricks Genie provides a **Conversational AI Workspace**, allowing users to generate interactive, prompt-based insights without writing SQL. It bridges the gap between raw data and business users through natural language processing.

![Genie](./assets/customer_360_genie_space.png)

---

## 📁 Repository Structure

```text
restaurant-databricks-declarative-pipeline-project/
│
├── assets/                                   # Architecture, pipeline & dashboard screenshots
│   ├── architechure.png
│   ├── restaurant_dlt_project_pipeline.png
│   ├── restaurant_dlt_project_workflow.png
│   └── Restaurant Analytics Dashboard.png
│
├── 00_landing_layer.py                        # Auto Loader + Landing DLT tables
├── 01_bronze_layer.py                         # Bronze cleansing logic + expectations
├── 02_silver_layer.py                         # Silver transformations (Orders/Menu)
├── 02_silver_scd_layer.py                     # SCD Type-1 & Type-2 logic (Staff/Menu changes)
├── 03_gold_layer.py                           # Gold materialized views & KPIs
│
└── README.md

```
