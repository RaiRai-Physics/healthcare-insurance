# Takeo Capstone Project  
## Health Care Insurance Big Data Pipeline and Analytics Solution

A complete end-to-end big data project for a health care insurance company focused on improving revenue, understanding customer behavior, and generating actionable business insights from raw multi-source data.

This project covers the full pipeline from raw file ingestion and data cleansing to warehouse modeling and analytical output generation for business use cases.

---

## Project Overview

A health care insurance company wants to improve revenue and better understand customer behavior by analyzing insurance-related data collected from multiple sources, including scraped and third-party datasets.

The goal of this project is to build a scalable data pipeline that:

- ingests raw healthcare insurance datasets
- cleans and standardizes the data
- loads curated data into a warehouse layer
- generates analytical outputs for business decision-making
- supports customer targeting, offer customization, and royalty-related insights

This implementation delivers those objectives using:

- **AWS S3** for raw and cleaned data storage
- **AWS Glue + PySpark** for ETL and data cleansing
- **Snowflake** for warehouse modeling and analytical result generation
- **GitHub** for documentation and source code management

---

## Business Goal

The company wants to:

- identify diseases with the highest claim frequency
- analyze subscriber behavior
- understand policy subscription trends
- determine profitable groups
- evaluate hospitals, cities, and subgroups associated with claims
- support better business strategies, customer offers, and insurance planning

---

## Project Architecture

```mermaid
flowchart LR
    A[Raw Input Files] --> B[AWS S3 - input-data]
    B --> C[AWS Glue ETL Job]
    C --> D[Cleaned Data in S3 - clean-data]
    D --> E[Snowflake External Stage]
    E --> F[Snowflake Core Tables]
    F --> G[Analytical Output Tables]
    G --> H[Validation Queries]
    G --> I[Presentation / Screenshots / Reports]
