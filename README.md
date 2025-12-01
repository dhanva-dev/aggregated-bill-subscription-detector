
# 📊 Aggregated Bill & Subscription Leakage Detection

![Python](https://img.shields.io/badge/Python-3.9+-3776AB?style=for-the-badge&logo=python&logoColor=white)
![BigQuery](https://img.shields.io/badge/Google_BigQuery-669DF6?style=for-the-badge&logo=googlecloud&logoColor=white)
![Power BI](https://img.shields.io/badge/Power_BI-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)
![Groq](https://img.shields.io/badge/Groq_AI-F55036?style=for-the-badge&logo=fastapi&logoColor=white)
![Airflow](https://img.shields.io/badge/Apache_Airflow-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)

An end-to-end data engineering pipeline that transforms raw, unstructured SMS text into actionable financial insights. This project automates the detection of "phantom" subscriptions and spending anomalies using LLMs and statistical analysis.

## 📸 Dashboard Preview

### Financial Overview
![Dashboard Overview](dashboard/dashboard_overview.png)
*Tracks monthly spending trends and category breakdowns.*

### Subscription Deep-Dive
![Subscription Analysis](dashboard/dashboard_subscriptions.png)
*Identifies recurring payments and flags statistical anomalies (Z-Score).*

---

## 🚀 Project Overview

In a world of digital payments, financial data is often trapped in unstructured SMS notifications. Manual tracking is tedious, leading to "subscription leakage"—paying for services you no longer use.

**This project solves that problem by:**
1.  **Ingesting** raw SMS backups (XML) into a staging environment.
2.  **Cleaning** noise using a hybrid Regex + LLM approach to minimize API costs.
3.  **Enriching** transaction data using the Groq API (Llama-3) to extract Vendors, Amounts, and Categories.
4.  **Warehousing** data in Google BigQuery with a Star Schema model.
5.  **Analyzing** spending patterns using SciPy (Z-score anomaly detection).
6.  **Visualizing** the results in Power BI.

---

## 🏗️ Architecture

```mermaid
graph LR
    A[Raw SMS XML] -->|Ingest| B(PostgreSQL Staging)
    B -->|Pre-Filter (Regex)| C{Is Transaction?}
    C -->|No| D[Junk/OTP]
    C -->|Yes| E[Groq API / Llama-3]
    E -->|Structured JSON| F[(Google BigQuery)]
    F -->|SQL Modeling| G[Analytical Tables]
    G -->|SciPy Analysis| H[Anomaly Detection]
    G -->|Connect| I[Power BI Dashboard]
````

### Key Engineering Decisions

  * **Hybrid Filtering:** Instead of sending 100k messages to the AI (costly/slow), I implemented a strict **Regex Pre-filter** layer in Python. This reduced the API load by \~90%, sending only high-confidence transaction messages to the LLM.
  * **Idempotency:** The pipeline handles deduplication using message hashing, ensuring the process can be re-run without creating duplicate records.
  * **Resilience:** The ingestion script handles diverse edge cases in XML parsing, and the API connector manages rate limits (`429`) automatically.

-----

## 🛠️ Tech Stack

  * **Languages:** Python, SQL
  * **Database:** PostgreSQL (Staging), Google BigQuery (Data Warehouse)
  * **AI/LLM:** Groq API (Llama-3-8b-instant) for Named Entity Recognition (NER)
  * **Orchestration:** Apache Airflow (DAGs included)
  * **Visualization:** Microsoft Power BI
  * **Libraries:** `pandas`, `sqlalchemy`, `scipy`, `xml.etree`

-----

## 📂 Repository Structure

```
├── dags/                       # Airflow DAGs for orchestration
│   └── sms_pipeline_dag.py
├── dashboard/                  # Screenshots and Power BI files
│   ├── dashboard_overview.png
│   └── subscription_dashboard.pbix
├── sql/                        # Data Modeling Logic
│   ├── model_subscriptions.sql
│   └── model_monthly_spend.sql
├── src/                        # Source Code
│   ├── ingest_xml.py           # Parses Android XML backup -> Postgres
│   ├── pre_filter_data.py      # Regex filtering (Junk vs Transaction)
│   ├── process_nlp_groq.py     # LLM Enrichment via Groq API
│   └── run_analysis.py         # Z-Score Anomaly Detection
├── requirements.txt            # Python dependencies
└── README.md                   # Project Documentation
```

-----

## ⚡ How to Run

### Prerequisites

1.  **PostgreSQL** installed locally.
2.  **Google Cloud Platform** project with BigQuery API enabled.
3.  **Groq API Key** (Free tier used).
4.  **Python 3.9+**.

### 1\. Setup Environment

```bash
# Clone the repo
git clone [https://github.com/YourUsername/aggregated-bill-subscription-detector.git](https://github.com/YourUsername/aggregated-bill-subscription-detector.git)
cd aggregated-bill-subscription-detector

# Install dependencies
pip install -r requirements.txt
```

### 2\. Configure Credentials

Set the following environment variables (or use a `.env` file):

```bash
export DB_PASS="your_postgres_password"
export GROQ_API_KEY="your_groq_api_key"
export GOOGLE_APPLICATION_CREDENTIALS="path/to/your/gcp-creds.json"
```

### 3\. Execute the Pipeline (Manual Mode)

You can run the scripts in order to replicate the pipeline:

```bash
# 1. Ingest XML Data (Load raw data to Postgres)
python src/ingest_xml.py

# 2. Pre-Filter Data (Separate transactions from OTPs/Spam)
python src/pre_filter_data.py

# 3. AI Enrichment (Extract entities using Llama-3)
python src/process_nlp_groq.py

# 4. Run Analytics (Detect Anomalies)
python src/run_analysis.py
```

### 4\. Visualization

Open `dashboard/subscription_dashboard.pbix` in Power BI Desktop and click **Refresh** to load the data from your BigQuery project.

-----

## 📈 Future Improvements

  * **Real-time Ingestion:** Build an Android app or background service to push SMS to the database in real-time instead of manual XML export.
  * **Categorization Model:** Fine-tune a small BERT model to replace the Regex filter for higher accuracy in detecting ambiguous transactions.
  * **Alerting:** Integrate Twilio or Slack to send a push notification when `run_analysis.py` detects a spending spike \> 2.5 Z-score.


