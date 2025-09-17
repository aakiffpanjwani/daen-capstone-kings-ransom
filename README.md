# DAEN Capstone – King's Ransom Data Warehouse

This repository contains all the infrastructure, ETL scripts, and database schema for building a hybrid AWS data warehouse. 

## 📂 Repo Structure
- **infra/** → Infrastructure as Code (Terraform/CloudFormation for S3, Lambda, EventBridge, IAM, EC2)
- **lambda/** → AWS Lambda ingestion functions (OrderPort API → S3)
- **airflow/** → Apache Airflow DAGs for ETL (transform JSON → CSV, load into Postgres)
- **sql/** → PostgreSQL schema & migration scripts
- **config/** → Sample `.env` file & metadata (API keys stored in AWS SSM, not here)
- **docs/** → Architecture diagrams & project documentation

## 🚀 Workflow
1. **Ingestion:** EventBridge triggers Lambda → pulls OrderPort data → stores in S3.
2. **Processing:** Airflow on EC2 transforms JSON → CSV and loads into Postgres.
3. **Warehouse:** PostgreSQL with fact & dimension tables for analytics.
4. **Security:** IAM roles, SSM Parameter Store, SSH tunneling into EC2.

## ⚠️ Security Notice
- Do NOT commit real API keys or AWS credentials.
- Use `config/sample.env` to show required variables with placeholders.
- Store secrets in **AWS Systems Manager Parameter Store**.
